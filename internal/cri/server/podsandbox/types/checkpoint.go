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

package types

import (
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// CheckpointPodRequest contains the parameters for checkpointing a pod sandbox.
type CheckpointPodRequest struct {
	// PodSandboxId is the ID of the pod sandbox to checkpoint.
	PodSandboxId string
	// Path is the directory where the checkpoint data will be stored.
	Path string
	// Timeout is the timeout in seconds for the checkpoint operation.
	Timeout int64
}

// CheckpointPodResponse is the response from a CheckpointPod call.
type CheckpointPodResponse struct{}

// RestorePodRequest contains the parameters for restoring a pod sandbox.
type RestorePodRequest struct {
	// Path is the directory containing the checkpoint data.
	Path string
	// Config is an optional PodSandboxConfig to use for the restored pod.
	// If nil, the config is read from the checkpoint.
	Config *runtime.PodSandboxConfig
	// ContainerConfigs is an optional list of container configs to use
	// when restoring containers. If empty, minimal configs are created
	// from the checkpoint manifest.
	ContainerConfigs []*runtime.ContainerConfig
}

// RestorePodResponse is the response from a RestorePod call.
type RestorePodResponse struct {
	// PodSandboxId is the ID of the newly created pod sandbox.
	PodSandboxId string
}
