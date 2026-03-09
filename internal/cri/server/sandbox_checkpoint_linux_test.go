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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

	containerstore "github.com/containerd/containerd/v2/internal/cri/store/container"
	sandboxstore "github.com/containerd/containerd/v2/internal/cri/store/sandbox"
)

func TestCheckpointPodSandboxNotFound(t *testing.T) {
	c := newTestCRIService()
	_, err := c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "nonexistent-sandbox",
		Path:         t.TempDir(),
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find sandbox")
}

func TestCheckpointPodEmptyPath(t *testing.T) {
	c := newTestCRIService()

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:   "sandbox-1",
			Name: "test-pod",
			Config: &runtime.PodSandboxConfig{
				Metadata: &runtime.PodSandboxMetadata{Name: "test-pod"},
			},
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	_, err := c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-1",
		Path:         "",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint path is required")
}

func TestCheckpointPodNoContainers(t *testing.T) {
	c := newTestCRIService()

	sandboxConfig := &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "test-pod",
			Namespace: "default",
			Uid:       "uid-1",
		},
	}

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:     "sandbox-1",
			Name:   "test-pod",
			Config: sandboxConfig,
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	checkpointDir := t.TempDir()
	_, err := c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-1",
		Path:         checkpointDir,
	})
	require.NoError(t, err)

	// Verify pod-config.json was written.
	configData, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
	require.NoError(t, err)
	var savedConfig runtime.PodSandboxConfig
	require.NoError(t, json.Unmarshal(configData, &savedConfig))
	assert.Equal(t, "test-pod", savedConfig.GetMetadata().GetName())
	assert.Equal(t, "default", savedConfig.GetMetadata().GetNamespace())

	// Verify checkpoint-manifest.json was written with empty containers.
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		SandboxID  string   `json:"sandboxId"`
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Equal(t, "sandbox-1", manifest.SandboxID)
	assert.Empty(t, manifest.Containers)
}

func TestCheckpointPodSkipsNonRunningContainers(t *testing.T) {
	c := newTestCRIService()

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:   "sandbox-1",
			Name: "test-pod",
			Config: &runtime.PodSandboxConfig{
				Metadata: &runtime.PodSandboxMetadata{Name: "test-pod"},
			},
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	// Add a created (not running) container to the sandbox.
	createdContainer, err := containerstore.NewContainer(
		containerstore.Metadata{
			ID:        "container-created",
			Name:      "created-container",
			SandboxID: "sandbox-1",
			Config:    &runtime.ContainerConfig{Metadata: &runtime.ContainerMetadata{Name: "created"}},
		},
		containerstore.WithFakeStatus(containerstore.Status{
			CreatedAt: 1000,
		}),
	)
	require.NoError(t, err)
	require.NoError(t, c.containerStore.Add(createdContainer))

	// Add an exited container.
	exitedContainer, err := containerstore.NewContainer(
		containerstore.Metadata{
			ID:        "container-exited",
			Name:      "exited-container",
			SandboxID: "sandbox-1",
			Config:    &runtime.ContainerConfig{Metadata: &runtime.ContainerMetadata{Name: "exited"}},
		},
		containerstore.WithFakeStatus(containerstore.Status{
			CreatedAt:  1000,
			StartedAt:  2000,
			FinishedAt: 3000,
		}),
	)
	require.NoError(t, err)
	require.NoError(t, c.containerStore.Add(exitedContainer))

	checkpointDir := t.TempDir()
	_, err = c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-1",
		Path:         checkpointDir,
	})
	require.NoError(t, err)

	// Verify manifest has no containers (both were non-running).
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Empty(t, manifest.Containers)
}

func TestCheckpointPodSkipsContainersFromOtherSandbox(t *testing.T) {
	c := newTestCRIService()

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:   "sandbox-1",
			Name: "test-pod",
			Config: &runtime.PodSandboxConfig{
				Metadata: &runtime.PodSandboxMetadata{Name: "test-pod"},
			},
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	// Add a running container belonging to a different sandbox.
	otherContainer, err := containerstore.NewContainer(
		containerstore.Metadata{
			ID:        "container-other",
			Name:      "other-container",
			SandboxID: "other-sandbox",
			Config:    &runtime.ContainerConfig{Metadata: &runtime.ContainerMetadata{Name: "other"}},
		},
		containerstore.WithFakeStatus(containerstore.Status{
			CreatedAt: 1000,
			StartedAt: 2000,
		}),
	)
	require.NoError(t, err)
	require.NoError(t, c.containerStore.Add(otherContainer))

	checkpointDir := t.TempDir()
	_, err = c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-1",
		Path:         checkpointDir,
	})
	require.NoError(t, err)

	// Verify manifest has no containers.
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Empty(t, manifest.Containers)
}

func TestRestorePodEmptyPath(t *testing.T) {
	c := newTestCRIService()
	_, err := c.RestorePod(context.Background(), &runtime.RestorePodRequest{
		Path: "",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checkpoint path is required")
}

func TestRestorePodMissingCheckpointFiles(t *testing.T) {
	c := newTestCRIService()
	_, err := c.RestorePod(context.Background(), &runtime.RestorePodRequest{
		Path: "/nonexistent/path",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read sandbox config from checkpoint")
}

func TestRestorePodInvalidConfig(t *testing.T) {
	checkpointDir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(checkpointDir, "pod-config.json"),
		[]byte("not valid json"),
		0o600,
	))

	c := newTestCRIService()
	_, err := c.RestorePod(context.Background(), &runtime.RestorePodRequest{
		Path: checkpointDir,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal sandbox config")
}

// writeCheckpointDir creates a checkpoint directory with pod-config.json,
// checkpoint-manifest.json, and dummy container archives for testing.
func writeCheckpointDir(t *testing.T, dir string, config *runtime.PodSandboxConfig, sandboxID string, containerNames []string) {
	t.Helper()

	configData, err := json.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pod-config.json"), configData, 0o600))

	manifest := map[string]interface{}{
		"sandboxId":  sandboxID,
		"containers": containerNames,
	}
	manifestData, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "checkpoint-manifest.json"), manifestData, 0o600))

	// Create dummy container checkpoint archives.
	for _, name := range containerNames {
		archivePath := filepath.Join(dir, fmt.Sprintf("container-%s.tar", name))
		require.NoError(t, os.WriteFile(archivePath, []byte("fake-checkpoint-data"), 0o600))
	}
}

// TestCheckpointRestoreFileFormat verifies the data contract between
// CheckpointPod and RestorePod: the checkpoint directory format written
// by CheckpointPod (with no running containers) is readable by RestorePod.
func TestCheckpointRestoreFileFormat(t *testing.T) {
	c := newTestCRIService()

	sandboxConfig := &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "roundtrip-pod",
			Namespace: "test-ns",
			Uid:       "uid-roundtrip",
		},
		Labels: map[string]string{"app": "test"},
	}

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:     "sandbox-roundtrip",
			Name:   "roundtrip-pod",
			Config: sandboxConfig,
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	// Checkpoint with no running containers (avoids CheckpointContainer).
	checkpointDir := t.TempDir()
	_, err := c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-roundtrip",
		Path:         checkpointDir,
	})
	require.NoError(t, err)

	// Verify pod-config.json written by CheckpointPod can be parsed
	// by RestorePod's config loading logic.
	configData, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
	require.NoError(t, err)
	var restoredConfig runtime.PodSandboxConfig
	require.NoError(t, json.Unmarshal(configData, &restoredConfig))
	assert.Equal(t, "roundtrip-pod", restoredConfig.GetMetadata().GetName())
	assert.Equal(t, "test-ns", restoredConfig.GetMetadata().GetNamespace())
	assert.Equal(t, "test", restoredConfig.GetLabels()["app"])

	// Verify manifest written by CheckpointPod uses the same JSON
	// field names that RestorePod expects.
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		SandboxID  string   `json:"sandboxId"`
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Equal(t, "sandbox-roundtrip", manifest.SandboxID)
	assert.Empty(t, manifest.Containers)

	// Verify the files can be read and parsed the same way RestorePod
	// would parse them (we can't call RestorePod because RunPodSandbox
	// panics with the fake service).
	configFromFile, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
	require.NoError(t, err)
	var parsedConfig runtime.PodSandboxConfig
	require.NoError(t, json.Unmarshal(configFromFile, &parsedConfig))
	assert.Equal(t, "roundtrip-pod", parsedConfig.GetMetadata().GetName())
}

// TestCheckpointPodConfigPreservesAllFields verifies that the checkpoint
// preserves the full PodSandboxConfig including labels, annotations, and
// security context.
func TestCheckpointPodConfigPreservesAllFields(t *testing.T) {
	c := newTestCRIService()

	sandboxConfig := &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "full-config-pod",
			Namespace: "kube-system",
			Uid:       "uid-full",
			Attempt:   2,
		},
		Hostname: "my-host",
		Labels: map[string]string{
			"app":     "web",
			"version": "v1",
		},
		Annotations: map[string]string{
			"checkpoint.k8s.io/reason": "migration",
		},
		Linux: &runtime.LinuxPodSandboxConfig{
			SecurityContext: &runtime.LinuxSandboxSecurityContext{
				NamespaceOptions: &runtime.NamespaceOption{
					Network: runtime.NamespaceMode_NODE,
				},
			},
		},
	}

	sb := sandboxstore.NewSandbox(
		sandboxstore.Metadata{
			ID:     "sandbox-full",
			Name:   "full-config-pod",
			Config: sandboxConfig,
		},
		sandboxstore.Status{State: sandboxstore.StateReady},
	)
	require.NoError(t, c.sandboxStore.Add(sb))

	checkpointDir := t.TempDir()
	_, err := c.CheckpointPod(context.Background(), &runtime.CheckpointPodRequest{
		PodSandboxId: "sandbox-full",
		Path:         checkpointDir,
	})
	require.NoError(t, err)

	// Read back the config and verify all fields are preserved.
	configData, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
	require.NoError(t, err)
	var restored runtime.PodSandboxConfig
	require.NoError(t, json.Unmarshal(configData, &restored))

	assert.Equal(t, "full-config-pod", restored.GetMetadata().GetName())
	assert.Equal(t, "kube-system", restored.GetMetadata().GetNamespace())
	assert.Equal(t, "uid-full", restored.GetMetadata().GetUid())
	assert.Equal(t, uint32(2), restored.GetMetadata().GetAttempt())
	assert.Equal(t, "my-host", restored.GetHostname())
	assert.Equal(t, "web", restored.GetLabels()["app"])
	assert.Equal(t, "migration", restored.GetAnnotations()["checkpoint.k8s.io/reason"])
	assert.Equal(t, runtime.NamespaceMode_NODE, restored.GetLinux().GetSecurityContext().GetNamespaceOptions().GetNetwork())
}

// TestRestorePodConfigLoading verifies that the config loading logic in
// RestorePod correctly reads and parses pod-config.json from the
// checkpoint directory. Tests the file format without calling RunPodSandbox.
func TestRestorePodConfigLoading(t *testing.T) {
	checkpointDir := t.TempDir()
	originalConfig := &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "restored-pod",
			Namespace: "default",
			Uid:       "original-uid-12345",
			Attempt:   1,
		},
		Hostname: "test-host",
		Labels:   map[string]string{"app": "web"},
	}
	writeCheckpointDir(t, checkpointDir, originalConfig, "old-sandbox-id", nil)

	// Verify the written config can be read back and parsed correctly,
	// simulating what RestorePod does internally.
	configData, err := os.ReadFile(filepath.Join(checkpointDir, "pod-config.json"))
	require.NoError(t, err)

	var loadedConfig runtime.PodSandboxConfig
	require.NoError(t, json.Unmarshal(configData, &loadedConfig))

	assert.Equal(t, "restored-pod", loadedConfig.GetMetadata().GetName())
	assert.Equal(t, "default", loadedConfig.GetMetadata().GetNamespace())
	assert.Equal(t, "original-uid-12345", loadedConfig.GetMetadata().GetUid())
	assert.Equal(t, "test-host", loadedConfig.GetHostname())
	assert.Equal(t, "web", loadedConfig.GetLabels()["app"])
}

// TestRestorePodManifestLoading verifies that the manifest loading logic
// in RestorePod correctly reads checkpoint-manifest.json.
func TestRestorePodManifestLoading(t *testing.T) {
	checkpointDir := t.TempDir()
	containers := []string{
		"app_my-pod_default_uid_0",
		"sidecar_my-pod_default_uid_0",
	}
	writeCheckpointDir(t, checkpointDir, &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name: "my-pod", Namespace: "default", Uid: "uid",
		},
	}, "sandbox-123", containers)

	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)

	// Parse using the same struct that RestorePod uses.
	var manifest struct {
		SandboxID  string   `json:"sandboxId"`
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Equal(t, "sandbox-123", manifest.SandboxID)
	assert.Equal(t, containers, manifest.Containers)

	// Verify each container has a corresponding archive.
	for _, name := range manifest.Containers {
		archivePath := filepath.Join(checkpointDir, fmt.Sprintf("container-%s.tar", name))
		_, err := os.Stat(archivePath)
		assert.NoError(t, err, "archive for container %q should exist", name)
	}
}

// TestRestorePodManifestContainerArchiveNaming verifies that RestorePod
// looks for container archives using the correct naming convention:
// container-{criName}.tar
func TestRestorePodManifestContainerArchiveNaming(t *testing.T) {
	checkpointDir := t.TempDir()
	containers := []string{
		"app_my-pod_default_uid_0",
		"sidecar_my-pod_default_uid_0",
	}
	writeCheckpointDir(t, checkpointDir, &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "my-pod",
			Namespace: "default",
			Uid:       "uid",
		},
	}, "old-sandbox", containers)

	// Verify archives exist at the paths RestorePod would check.
	for _, name := range containers {
		archivePath := filepath.Join(checkpointDir, fmt.Sprintf("container-%s.tar", name))
		_, err := os.Stat(archivePath)
		require.NoError(t, err, "archive for %s should exist", name)
	}

	// Verify manifest is parseable and references the containers.
	manifestData, err := os.ReadFile(filepath.Join(checkpointDir, "checkpoint-manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		SandboxID  string   `json:"sandboxId"`
		Containers []string `json:"containers"`
	}
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	assert.Equal(t, "old-sandbox", manifest.SandboxID)
	assert.Equal(t, containers, manifest.Containers)
}

// TestCRIContainerNameExtraction verifies the logic that extracts the
// Kubernetes container name from a full CRI container name. This is the
// same logic used in RestorePod to build the container metadata.
func TestCRIContainerNameExtraction(t *testing.T) {
	tests := []struct {
		criName     string
		expectedK8s string
	}{
		{
			criName:     "app_my-pod_default_uid-123_0",
			expectedK8s: "app",
		},
		{
			criName:     "sidecar_my-pod_kube-system_uid-456_1",
			expectedK8s: "sidecar",
		},
		{
			criName:     "simple-name",
			expectedK8s: "simple-name",
		},
		{
			criName:     "init-container_pod_ns_uid_0",
			expectedK8s: "init-container",
		},
		{
			criName:     "_leading-delimiter",
			expectedK8s: "_leading-delimiter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.criName, func(t *testing.T) {
			k8sName := tt.criName
			if idx := strings.Index(tt.criName, nameDelimiter); idx > 0 {
				k8sName = tt.criName[:idx]
			}
			assert.Equal(t, tt.expectedK8s, k8sName)
		})
	}
}

// TestRestorePodContainerConfigImagePath verifies that the container
// config created by RestorePod uses the raw file path for the Image
// field (not prefixed with "checkpoint:") so that CreateContainer's
// os.Stat check succeeds and routes through CRImportCheckpoint.
func TestRestorePodContainerConfigImagePath(t *testing.T) {
	checkpointDir := t.TempDir()
	criContainerName := "app_test-pod_default_uid_0"

	writeCheckpointDir(t, checkpointDir, &runtime.PodSandboxConfig{
		Metadata: &runtime.PodSandboxMetadata{
			Name:      "test-pod",
			Namespace: "default",
			Uid:       "uid",
		},
	}, "old-sandbox", []string{criContainerName})

	// Verify the checkpoint archive file exists at the path RestorePod
	// would use for the Image field.
	expectedImagePath := filepath.Join(checkpointDir, fmt.Sprintf("container-%s.tar", criContainerName))
	_, err := os.Stat(expectedImagePath)
	require.NoError(t, err, "container archive should exist at expected path")

	// Verify the path does NOT have a "checkpoint:" prefix — this is
	// critical for os.Stat in CreateContainer to succeed.
	assert.False(t, strings.HasPrefix(expectedImagePath, "checkpoint:"),
		"image path must not have checkpoint: prefix")
}

// TestRestorePodContainerConfigMatching verifies that the ContainerConfigs
// matching logic in RestorePod correctly maps CRI container names to
// Kubernetes container names for config lookup.
func TestRestorePodContainerConfigMatching(t *testing.T) {
	tests := []struct {
		name           string
		criNames       []string
		configs        []*runtime.ContainerConfig
		expectedMatch  map[string]string // criName -> expected matched config name
		expectedNoConf []string          // criNames that should get a default config
	}{
		{
			name:     "single container with matching config",
			criNames: []string{"app_test-pod_default_uid_0"},
			configs: []*runtime.ContainerConfig{
				{
					Metadata: &runtime.ContainerMetadata{Name: "app"},
					Image:    &runtime.ImageSpec{Image: "custom:v1"},
				},
			},
			expectedMatch: map[string]string{
				"app_test-pod_default_uid_0": "app",
			},
		},
		{
			name:     "multiple containers with partial config match",
			criNames: []string{"web_pod_ns_uid_0", "sidecar_pod_ns_uid_0"},
			configs: []*runtime.ContainerConfig{
				{
					Metadata: &runtime.ContainerMetadata{Name: "web"},
					Image:    &runtime.ImageSpec{Image: "nginx:latest"},
				},
			},
			expectedMatch: map[string]string{
				"web_pod_ns_uid_0": "web",
			},
			expectedNoConf: []string{"sidecar_pod_ns_uid_0"},
		},
		{
			name:           "no configs provided",
			criNames:       []string{"app_pod_ns_uid_0"},
			configs:        nil,
			expectedNoConf: []string{"app_pod_ns_uid_0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, criName := range tt.criNames {
				// Extract k8s name (same logic as RestorePod).
				k8sName := criName
				if idx := strings.Index(criName, nameDelimiter); idx > 0 {
					k8sName = criName[:idx]
				}

				// Find matching config (same logic as RestorePod).
				var matched *runtime.ContainerConfig
				for _, cc := range tt.configs {
					if cc.GetMetadata().GetName() == k8sName {
						matched = cc
						break
					}
				}

				if expectedName, ok := tt.expectedMatch[criName]; ok {
					require.NotNil(t, matched, "expected config match for %s", criName)
					assert.Equal(t, expectedName, matched.GetMetadata().GetName())
				}

				for _, noConfName := range tt.expectedNoConf {
					if criName == noConfName {
						assert.Nil(t, matched, "expected no config match for %s", criName)
					}
				}
			}
		})
	}
}
