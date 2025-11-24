/*
Copyright 2025 The Kubernetes Authors.

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

package scorer

import (
	"context"
	"encoding/json"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/framework"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
)

const (
	GPUUtilizationScorerType = "gpu-utilization-scorer"
)

// compile-time type assertion
var _ framework.Scorer = &GPUUtilizationScorer{}

// GPUUtilizationScorerFactory defines the factory function for GPUUtilizationScorer.
func GPUUtilizationScorerFactory(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	return NewGPUUtilizationScorer().WithName(name), nil
}

// NewGPUUtilizationScorer initializes a new GPUUtilizationScorer and returns its pointer.
func NewGPUUtilizationScorer() *GPUUtilizationScorer {
	return &GPUUtilizationScorer{
		typedName: plugins.TypedName{Type: GPUUtilizationScorerType, Name: GPUUtilizationScorerType},
	}
}

// GPUUtilizationScorer scores list of candidate pods based on the pod's GPU utilization.
// the lower GPU utilization the pod has, the higher score it will get.
type GPUUtilizationScorer struct {
	typedName plugins.TypedName
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *GPUUtilizationScorer) TypedName() plugins.TypedName {
	return s.typedName
}

// Consumes returns the list of data that is consumed by the plugin.
func (s *GPUUtilizationScorer) Consumes() map[string]any {
	return map[string]any{
		metrics.GPUUtilizationKey: float64(0),
	}
}

// WithName sets the name of the scorer.
func (s *GPUUtilizationScorer) WithName(name string) *GPUUtilizationScorer {
	s.typedName.Name = name
	return s
}

// Score returns the scoring result for the given list of pods based on context.
func (s *GPUUtilizationScorer) Score(_ context.Context, _ *types.CycleState, _ *types.LLMRequest, pods []types.Pod) map[types.Pod]float64 {
	scores := make(map[types.Pod]float64, len(pods))

	// Create a map to hold the scores for each pod
	for _, pod := range pods {
		scores[pod] = 1.0 - min(pod.GetMetrics().GPUUtilization, 1.0)
	}

	return scores
}
