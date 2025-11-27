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
	"math"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/plugins"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/framework"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/scheduling/types"
)

const (
	RunningRequestsScorerType = "running-requests-scorer"
)

// compile-time type assertion
var _ framework.Scorer = &RunningRequestsScorer{}

// RunningRequestsScorerFactory defines the factory function for RunningRequestsScorer.
func RunningRequestsScorerFactory(name string, _ json.RawMessage, _ plugins.Handle) (plugins.Plugin, error) {
	return NewRunningRequestsScorer().WithName(name), nil
}

// NewRunningRequestsScorer initializes a new RunningRequestsScorer and returns its pointer.
func NewRunningRequestsScorer() *RunningRequestsScorer {
	return &RunningRequestsScorer{
		typedName: plugins.TypedName{Type: RunningRequestsScorerType, Name: RunningRequestsScorerType},
	}
}

// RunningRequestsScorer scores list of candidate pods based on the pod's running requests count.
// the less running requests the pod has, the higher score it will get (since it's more available to serve new request).
type RunningRequestsScorer struct {
	typedName plugins.TypedName
}

// TypedName returns the type and name tuple of this plugin instance.
func (s *RunningRequestsScorer) TypedName() plugins.TypedName {
	return s.typedName
}

// Consumes returns the list of data that is consumed by the plugin.
func (s *RunningRequestsScorer) Consumes() map[string]any {
	return map[string]any{
		metrics.NumRequestsRunningKey: int(0),
	}
}

// WithName sets the name of the scorer.
func (s *RunningRequestsScorer) WithName(name string) *RunningRequestsScorer {
	s.typedName.Name = name
	return s
}

// Score returns the scoring result for the given list of pods based on context.
func (s *RunningRequestsScorer) Score(_ context.Context, _ *types.CycleState, _ *types.LLMRequest, pods []types.Pod) map[types.Pod]float64 {
	minRunningRequests := math.MaxInt
	maxRunningRequests := math.MinInt

	// Iterate through the remaining pods to find min and max
	for _, pod := range pods {
		runningRequests := pod.GetMetrics().NumRequestsRunning
		if runningRequests < minRunningRequests {
			minRunningRequests = runningRequests
		}
		if runningRequests > maxRunningRequests {
			maxRunningRequests = runningRequests
		}
	}

	// podScoreFunc calculates the score based on the queue size of each pod. Longer queue gets a lower score.
	podScoreFunc := func(pod types.Pod) float64 {
		if maxRunningRequests == minRunningRequests {
			// If all pods have the same running requests, return a neutral score
			return 1.0
		}
		return float64(maxRunningRequests-pod.GetMetrics().NumRequestsRunning) / float64(maxRunningRequests-minRunningRequests)
	}

	// Create a map to hold the scores for each pod
	scores := make(map[types.Pod]float64, len(pods))
	for _, pod := range pods {
		scores[pod] = podScoreFunc(pod)
	}
	return scores
}
