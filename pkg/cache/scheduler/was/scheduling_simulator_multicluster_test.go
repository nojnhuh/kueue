//go:build !exclude_scheduler_library

/*
Copyright The Kubernetes Authors.

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

package was

import (
	"slices"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/scheduler/simulator"
	"sigs.k8s.io/kueue/pkg/constants"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
)

func TestMultiClusterSimulatorIsolation(t *testing.T) {
	cases := map[string]struct {
		simulateEmpty bool
		unmanaged     bool
		preempt       bool
		wantClusters  []string
	}{
		"local host port does not block remote nodes": {wantClusters: []string{"worker2"}},
		"simulate empty removes only managed usage": {
			simulateEmpty: true, wantClusters: []string{"", "worker2"},
		},
		"simulate empty retains unmanaged local usage": {
			simulateEmpty: true, unmanaged: true, wantClusters: []string{"worker2"},
		},
		"transaction preempts and restores local usage": {
			preempt: true, wantClusters: []string{"", "worker2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := klog.NewContext(t.Context(), logr.Discard())
			sim, err := NewWASSimulator(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			var nodes []*corev1.Node
			var candidates []simulator.Candidate
			for _, cluster := range []string{"", "worker1", "worker2"} {
				node := testingnode.MakeNode("same-node").
					Label(corev1.LabelHostname, "same-host").
					StatusAllocatable(corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("4"), corev1.ResourcePods: resource.MustParse("10"),
					}).Ready().Obj()
				if cluster != "" {
					node.Labels[constants.MultiKueueClusterLabel] = cluster
				}
				if cluster == "worker1" {
					node.Spec.Taints = []corev1.Taint{{Key: "dedicated", Value: "other", Effect: corev1.TaintEffectNoSchedule}}
				}
				nodes = append(nodes, node)
				candidates = append(candidates, &testCandidate{node: node, id: utiltas.HostnameDomainID(cluster, "same-host")})
			}
			pod := testingpod.MakePod("existing", "ns").UID("existing").
				NodeName("same-node").StatusPhase(corev1.PodRunning).
				Port(8080, 8080, corev1.ProtocolTCP).Obj()
			if !tc.unmanaged {
				pod.Annotations = map[string]string{kueue.WorkloadAnnotation: "wl"}
			}
			sim.TrackPod(ctx, pod)
			snapshot, err := sim.Snapshot(ctx, nodes)
			if err != nil {
				t.Fatal(err)
			}
			template := &corev1.PodTemplateSpec{Spec: *pod.Spec.DeepCopy()}
			template.Spec.NodeName = ""
			assertFeasible := func(want []string, simulateEmpty bool) {
				t.Helper()
				var stats simulator.NodeExclusionStats
				matches, err := snapshot.FindFeasibleNodes(ctx, slices.Values(candidates),
					&simulator.PodRequirements{PodTemplate: template, SimulateEmpty: simulateEmpty}, &stats)
				if err != nil {
					t.Fatal(err)
				}
				got := make(map[utiltas.NodeKey]bool)
				for _, match := range matches {
					got[utiltas.NodeKeyFor(match.GetNode())] = true
				}
				wantNodes := make(map[utiltas.NodeKey]bool)
				for _, cluster := range want {
					wantNodes[utiltas.NodeKey{Cluster: cluster, Name: "same-node"}] = true
				}
				if diff := cmp.Diff(wantNodes, got); diff != "" {
					t.Errorf("feasible nodes mismatch (-want,+got):\n%s", diff)
				}
				if stats.TotalNodes != 3 || stats.SchedulerLibraryNoFit != 3-len(want) {
					t.Errorf("unexpected combined exclusion statistics: %+v", stats)
				}
			}
			if tc.preempt {
				if err := snapshot.Simulate(ctx, func() {
					if _, err := snapshot.PreemptWorkload(ctx, client.ObjectKey{Namespace: "ns", Name: "wl"}); err != nil {
						t.Fatal(err)
					}
					assertFeasible(tc.wantClusters, false)
				}); err != nil {
					t.Fatal(err)
				}
				assertFeasible([]string{"worker2"}, false)
			} else {
				assertFeasible(tc.wantClusters, tc.simulateEmpty)
			}
		})
	}
}
