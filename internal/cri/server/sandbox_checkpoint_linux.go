//go:build linux

/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/log"
	"github.com/google/uuid"
	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// CheckpointPod creates a Pod-level checkpoint by checkpointing all containers
// in the pod sandbox and bundling them into a single checkpoint archive.
func (c *criService) CheckpointPod(ctx context.Context, r *runtime.CheckpointPodRequest) (*runtime.CheckpointPodResponse, error) {
	log.G(ctx).WithField("podSandboxId", r.PodSandboxId).Debug("CheckpointPod")

	sandbox, err := c.sandboxStore.Get(r.PodSandboxId)
	if err != nil {
		return nil, fmt.Errorf("failed to find sandbox %q: %w", r.PodSandboxId, err)
	}

	// Create the checkpoint output directory.
	checkpointDir := r.Path
	if checkpointDir == "" {
		return nil, fmt.Errorf("checkpoint path is required")
	}
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory %q: %w", checkpointDir, err)
	}

	// Save sandbox metadata (PodSandboxConfig) for restore.
	sandboxConfig := sandbox.Config
	configData, err := json.Marshal(sandboxConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal sandbox config: %w", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "pod-config.json"), configData, 0o600); err != nil {
		return nil, fmt.Errorf("failed to write sandbox config: %w", err)
	}

	// Checkpoint each container in the sandbox.
	containers := c.containerStore.List()
	var checkpointedContainers []string
	for _, cntr := range containers {
		if cntr.SandboxID != r.PodSandboxId {
			continue
		}
		// Only checkpoint running containers.
		if cntr.Status.Get().State() != runtime.ContainerState_CONTAINER_RUNNING {
			continue
		}

		containerCheckpointPath := filepath.Join(checkpointDir, fmt.Sprintf("container-%s.tar", cntr.Name))
		containerReq := &runtime.CheckpointContainerRequest{
			ContainerId: cntr.ID,
			Location:    containerCheckpointPath,
			Timeout:     r.Timeout,
		}
		if _, err := c.CheckpointContainer(ctx, containerReq); err != nil {
			return nil, fmt.Errorf("failed to checkpoint container %q: %w", cntr.Name, err)
		}
		checkpointedContainers = append(checkpointedContainers, cntr.Name)
		log.G(ctx).WithField("container", cntr.Name).Debug("Container checkpointed")
	}

	// Save manifest listing all checkpointed containers.
	manifest := map[string]interface{}{
		"sandboxId":  r.PodSandboxId,
		"containers": checkpointedContainers,
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal checkpoint manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"), manifestData, 0o600); err != nil {
		return nil, fmt.Errorf("failed to write checkpoint manifest: %w", err)
	}

	log.G(ctx).WithField("containers", checkpointedContainers).Info("Pod checkpoint completed")
	return &runtime.CheckpointPodResponse{}, nil
}

// findSandboxByPodUID searches for an existing sandbox that matches the given
// pod metadata (name, namespace, UID). Returns the sandbox ID or empty string.
func (c *criService) findSandboxByPodUID(meta *runtime.PodSandboxMetadata) string {
	if meta == nil {
		return ""
	}
	sandboxes := c.sandboxStore.List()
	for _, sb := range sandboxes {
		sbMeta := sb.Config.GetMetadata()
		if sbMeta == nil {
			continue
		}
		if sbMeta.Name == meta.Name &&
			sbMeta.Namespace == meta.Namespace &&
			sbMeta.Uid == meta.Uid {
			return sb.ID
		}
	}
	return ""
}

// extractSpecMounts reads spec.dump from a checkpoint tar archive and returns
// the bind mounts defined in the OCI spec. These mounts are needed so that the
// restored container's OCI spec includes the same bind mounts (e.g. service
// account tokens, /etc/hosts) that were present at checkpoint time.
func extractSpecMounts(checkpointPath string) ([]*runtime.Mount, error) {
	f, err := os.Open(checkpointPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tr := tar.NewReader(f)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Name == "spec.dump" {
			data, err := io.ReadAll(tr)
			if err != nil {
				return nil, err
			}
			var spec runtimespec.Spec
			if err := json.Unmarshal(data, &spec); err != nil {
				return nil, err
			}
			var mounts []*runtime.Mount
			for _, m := range spec.Mounts {
				if m.Type != "bind" {
					continue
				}
				// Skip sandbox-internal mounts that linuxContainerMounts
				// already generates (/etc/hostname, /etc/hosts,
				// /etc/resolv.conf, /dev/shm).
				switch m.Destination {
				case "/etc/hostname", "/etc/hosts", "/etc/resolv.conf", "/dev/shm":
					continue
				}
				criMount := &runtime.Mount{
					ContainerPath: m.Destination,
					HostPath:      m.Source,
				}
				for _, opt := range m.Options {
					if opt == "ro" {
						criMount.Readonly = true
					}
				}
				mounts = append(mounts, criMount)
			}
			return mounts, nil
		}
	}
	return nil, nil // spec.dump not found
}

// RestorePod restores a Pod sandbox and its containers from a checkpoint.
func (c *criService) RestorePod(ctx context.Context, r *runtime.RestorePodRequest) (*runtime.RestorePodResponse, error) {
	log.G(ctx).WithField("path", r.Path).Debug("RestorePod")

	checkpointDir := r.Path
	if checkpointDir == "" {
		return nil, fmt.Errorf("checkpoint path is required")
	}

	// Load sandbox config from checkpoint.
	sandboxConfig := r.Config
	configFromCaller := sandboxConfig != nil
	if !configFromCaller {
		configData, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
		if err != nil {
			return nil, fmt.Errorf("failed to read sandbox config from checkpoint: %w", err)
		}
		sandboxConfig = &runtime.PodSandboxConfig{}
		if err := json.Unmarshal(configData, sandboxConfig); err != nil {
			return nil, fmt.Errorf("failed to unmarshal sandbox config: %w", err)
		}
	}

	// Generate a new UID for the restored sandbox to avoid name conflicts
	// with any previously existing sandbox that used the same pod config.
	// Skip this when the caller provided a config (e.g. the kubelet already
	// created a Pod with the correct UID).
	if !configFromCaller && sandboxConfig.GetMetadata() != nil {
		newUID := uuid.New().String()
		oldUID := sandboxConfig.Metadata.Uid
		sandboxConfig.Metadata.Uid = newUID

		// Update LogDirectory to reflect the new UID, since the kubelet
		// constructs it as /var/log/pods/{namespace}_{podName}_{uid}.
		if oldUID != "" && sandboxConfig.LogDirectory != "" {
			sandboxConfig.LogDirectory = strings.Replace(
				sandboxConfig.LogDirectory, oldUID, newUID, 1)
		}
	}

	// Try to find an existing sandbox for this Pod. When the kubelet
	// creates a Pod before calling RestorePod, it may have already set
	// up a sandbox (with networking). In that case we reuse it and stop
	// any running containers so we can replace them with CRIU-restored ones.
	var newSandboxID string
	existingSandbox := c.findSandboxByPodUID(sandboxConfig.GetMetadata())
	if existingSandbox != "" {
		log.G(ctx).WithField("sandbox", existingSandbox).Info("Reusing existing sandbox for restore")
		newSandboxID = existingSandbox

		// Stop any containers running in the existing sandbox so that
		// we can replace them with checkpoint-restored containers.
		containers := c.containerStore.List()
		for _, cntr := range containers {
			if cntr.SandboxID != existingSandbox {
				continue
			}
			if cntr.Status.Get().State() == runtime.ContainerState_CONTAINER_RUNNING {
				log.G(ctx).WithField("container", cntr.Name).Info("Stopping existing container before restore")
				_, _ = c.StopContainer(ctx, &runtime.StopContainerRequest{
					ContainerId: cntr.ID,
					Timeout:     5,
				})
			}
			log.G(ctx).WithField("container", cntr.Name).Info("Removing existing container before restore")
			_, _ = c.RemoveContainer(ctx, &runtime.RemoveContainerRequest{
				ContainerId: cntr.ID,
			})
		}
		// Allow time for cgroup cleanup after removing containers.
		time.Sleep(2 * time.Second)
	} else {
		// No existing sandbox — create a new one.
		runResp, err := c.RunPodSandbox(ctx, &runtime.RunPodSandboxRequest{
			Config: sandboxConfig,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create sandbox for restore: %w", err)
		}
		newSandboxID = runResp.GetPodSandboxId()
	}

	// Load the checkpoint manifest.
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to read checkpoint manifest: %w", err)
	}
	var manifest struct {
		SandboxID  string   `json:"sandboxId"`
		Containers []string `json:"containers"`
	}
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal checkpoint manifest: %w", err)
	}

	// Restore each container from its checkpoint archive.
	// The manifest stores the full CRI container name
	// (format: {k8sName}_{podName}_{namespace}_{uid}_{attempt}).
	// CreateContainer needs just the Kubernetes container name as the
	// metadata name — it will regenerate the full CRI name via
	// makeContainerName using the new sandbox metadata.
	for _, criContainerName := range manifest.Containers {
		checkpointPath := filepath.Join(checkpointDir, fmt.Sprintf("container-%s.tar", criContainerName))
		if _, err := os.Stat(checkpointPath); err != nil {
			log.G(ctx).WithError(err).WithField("container", criContainerName).Warn("Checkpoint archive not found, skipping")
			continue
		}

		// Extract the Kubernetes container name from the CRI name.
		// CRI names use "_" as delimiter and the first component is
		// the Kubernetes container name (which cannot contain "_").
		k8sContainerName := criContainerName
		if idx := strings.Index(criContainerName, nameDelimiter); idx > 0 {
			k8sContainerName = criContainerName[:idx]
		}

		// Find matching container config from the request if provided.
		var containerConfig *runtime.ContainerConfig
		for _, cc := range r.ContainerConfigs {
			if cc.GetMetadata().GetName() == k8sContainerName {
				containerConfig = cc
				break
			}
		}

		// Extract bind mounts from the checkpoint's OCI spec so that
		// the restored container has the same mounts (service account
		// tokens, termination log, projected volumes, etc.).
		specMounts, err := extractSpecMounts(checkpointPath)
		if err != nil {
			log.G(ctx).WithError(err).WithField("container", criContainerName).Warn("Failed to extract mounts from checkpoint spec")
		}

		// Ensure all mount source paths exist on the host. When reusing
		// a sandbox created by the kubelet, the pod directory structure
		// may not include paths for the checkpointed containers (e.g.
		// termination log, projected volumes with different names).
		// Create directories for most mounts; files only for known
		// single-file mounts like /dev/termination-log.
		for _, m := range specMounts {
			if _, err := os.Stat(m.HostPath); err != nil {
				switch m.ContainerPath {
				case "/dev/termination-log":
					// Termination log is a single file.
					dir := filepath.Dir(m.HostPath)
					if err := os.MkdirAll(dir, 0o755); err != nil {
						log.G(ctx).WithError(err).Warn("Failed to create mount parent dir")
					}
					if f, err := os.Create(m.HostPath); err == nil {
						f.Close()
					}
				default:
					// Most mounts (projected volumes, configmaps, etc.)
					// are directories.
					if err := os.MkdirAll(m.HostPath, 0o755); err != nil {
						log.G(ctx).WithError(err).WithField("path", m.HostPath).Warn("Failed to create mount source dir")
					}
				}
				log.G(ctx).WithField("path", m.HostPath).WithField("dest", m.ContainerPath).Debug("Created missing mount source for restore")
			}
		}

		// If no explicit config, create a minimal one. If a config was
		// provided (e.g. from the kubelet), use it but override the Image
		// to point to the checkpoint archive. CreateContainer detects
		// file paths via os.Stat and routes them through
		// CRImportCheckpoint for CRIU-based restore.
		if containerConfig == nil {
			containerConfig = &runtime.ContainerConfig{
				Metadata: &runtime.ContainerMetadata{
					Name: k8sContainerName,
				},
				Image: &runtime.ImageSpec{
					Image: checkpointPath,
				},
				Mounts:  specMounts,
				LogPath: fmt.Sprintf("%s/0.log", k8sContainerName),
			}
		} else {
			// Override image to point to the checkpoint archive so
			// CRImportCheckpoint is triggered. The original image name
			// is preserved in the container's metadata for hash matching.
			containerConfig.Image = &runtime.ImageSpec{
				Image: checkpointPath,
			}
			if len(containerConfig.Mounts) == 0 && len(specMounts) > 0 {
				containerConfig.Mounts = specMounts
			}
			if containerConfig.LogPath == "" {
				containerConfig.LogPath = fmt.Sprintf("%s/0.log", k8sContainerName)
			}
		}

		// Ensure the container's Linux security context has userns
		// options matching the sandbox. CreateContainer validates
		// that the sandbox and container userns configs are the same.
		sandboxUserns := sandboxConfig.GetLinux().GetSecurityContext().GetNamespaceOptions().GetUsernsOptions()
		if sandboxUserns != nil {
			if containerConfig.Linux == nil {
				containerConfig.Linux = &runtime.LinuxContainerConfig{}
			}
			if containerConfig.Linux.SecurityContext == nil {
				containerConfig.Linux.SecurityContext = &runtime.LinuxContainerSecurityContext{}
			}
			if containerConfig.Linux.SecurityContext.NamespaceOptions == nil {
				containerConfig.Linux.SecurityContext.NamespaceOptions = &runtime.NamespaceOption{}
			}
			containerConfig.Linux.SecurityContext.NamespaceOptions.UsernsOptions = sandboxUserns
		}

		createResp, err := c.CreateContainer(ctx, &runtime.CreateContainerRequest{
			PodSandboxId:  newSandboxID,
			Config:        containerConfig,
			SandboxConfig: sandboxConfig,
		})
		if err != nil {
			log.G(ctx).WithError(err).WithField("container", criContainerName).Error("Failed to create container from checkpoint")
			continue
		}

		// Start the restored container — this triggers CRIU restore
		// because the container was created with restore=true.
		if _, err := c.StartContainer(ctx, &runtime.StartContainerRequest{
			ContainerId: createResp.GetContainerId(),
		}); err != nil {
			return nil, fmt.Errorf("failed to start restored container %q: %w", criContainerName, err)
		}

		log.G(ctx).WithField("container", criContainerName).Info("Container restored from checkpoint")
	}

	log.G(ctx).WithField("sandboxId", newSandboxID).Info("Pod restore completed")
	return &runtime.RestorePodResponse{
		PodSandboxId: newSandboxID,
	}, nil
}
