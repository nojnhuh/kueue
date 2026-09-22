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

package scheduler

import (
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

func TestClusterQualifiedTopologyCapacity(t *testing.T) {
	cases := map[string]struct {
		virtual bool
		cpus    []int64
		wantFit bool
	}{
		"hostname leaves cannot pool two workers": {cpus: []int64{5000}},
		"hostname leaves charge earlier PodSets":  {cpus: []int64{3000, 3000}},
		"hostname leaves fit exactly":             {cpus: []int64{2000, 2000}, wantFit: true},
		"virtual leaves cannot pool two workers":  {virtual: true, cpus: []int64{5000}},
		"virtual leaves charge earlier PodSets":   {virtual: true, cpus: []int64{3000, 3000}},
		"virtual leaves fit exactly":              {virtual: true, cpus: []int64{2000, 2000}, wantFit: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.MultiKueueCentralizedTAS, true)
			features.SetFeatureGateDuringTest(t, features.TASNodeFeasibilityForAllLevels, tc.virtual)
			ctx, log := utiltesting.ContextWithLog(t)
			levels := []string{constants.MultiKueueClusterLabel, "rack"}
			if !tc.virtual {
				levels = append(levels, corev1.LabelHostname)
			}
			var nodes []*corev1.Node
			for _, cluster := range []string{"worker1", "worker2"} {
				nodes = append(nodes, testingnode.MakeNode("node-0").
					Label(constants.MultiKueueClusterLabel, cluster).
					Label("rack", "rack-0").
					Label(corev1.LabelHostname, "host-0").
					StatusAllocatable(corev1.ResourceList{
						corev1.ResourceCPU:  resource.MustParse("4"),
						corev1.ResourcePods: resource.MustParse("10"),
					}).Ready().Obj())
			}
			tree := newTopologyTree(levels, nodes, 0)
			if len(tree.leaves) != 2 {
				t.Fatalf("got %d leaves, want one per worker", len(tree.leaves))
			}
			for _, leaf := range tree.leaves {
				if got := leaf.capacity.ResourceValue(corev1.ResourceCPU); got != 4000 {
					t.Errorf("leaf %q has %d milliCPU, want 4000", leaf.id, got)
				}
			}
			snapshot := newTASFlavorSnapshot(log, flavorInformation{TopologyName: "topology"}, tree, newDefaultSimulatorSnapshot())
			var requests FlavorTASRequests
			for i, cpu := range tc.cpus {
				ps := utiltestingapi.MakePodSet(kueue.PodSetReference(fmt.Sprintf("ps-%d", i)), 1).
					RequiredTopologyRequest(levels[len(levels)-1]).Obj()
				ps.Template.Spec.NodeSelector = map[string]string{constants.MultiKueueClusterLabel: "worker1"}
				requests = append(requests, TASPodSetRequests{
					PodSet: ps, Count: 1,
					SinglePodRequests: resources.NewRequestsFromMap(resources.MapRequests{corev1.ResourceCPU: cpu}),
				})
			}
			result := snapshot.FindTopologyAssignmentsForFlavor(ctx, requests)
			if got := result.Failure() == nil; got != tc.wantFit {
				t.Errorf("fit = %t, want %t; result: %v", got, tc.wantFit, result)
			}
		})
	}
}

func TestNodesCacheClusterIsolation(t *testing.T) {
	cases := map[string]struct {
		read func(*nodesCache) []*corev1.Node
		want []string
	}{
		"enumerate every cluster": {
			read: (*nodesCache).getAllNodes, want: []string{"", "worker1", "worker2"},
		},
		"local topology excludes worker inventory": {
			read: func(cache *nodesCache) []*corev1.Node {
				nodes, _ := cache.find(nil, []string{corev1.LabelHostname})
				return nodes
			},
			want: []string{""},
		},
		"manager topology selects worker inventory": {
			read: func(cache *nodesCache) []*corev1.Node {
				nodes, _ := cache.find(nil, []string{constants.MultiKueueClusterLabel, corev1.LabelHostname})
				return nodes
			},
			want: []string{"worker1", "worker2"},
		},
		"delete only one worker": {
			read: func(cache *nodesCache) []*corev1.Node {
				cache.deleteCluster("worker1")
				return cache.getAllNodes()
			},
			want: []string{"", "worker2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cache := newNodesCache()
			for _, cluster := range []string{"", "worker1", "worker2"} {
				node := testingnode.MakeNode("node-0").Label(corev1.LabelHostname, "host-0").Ready()
				if cluster != "" {
					node.Label(constants.MultiKueueClusterLabel, cluster)
				}
				cache.sync(node.Obj())
			}
			got := make(map[utiltas.NodeKey]bool)
			for _, node := range tc.read(cache) {
				got[utiltas.NodeKeyFor(node)] = true
			}
			want := make(map[utiltas.NodeKey]bool)
			for _, cluster := range tc.want {
				want[utiltas.NodeKey{Cluster: cluster, Name: "node-0"}] = true
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("nodes mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestNonTASUsageClusterIsolation(t *testing.T) {
	cases := map[string]struct {
		action func(*nonTasUsageCache, logr.Logger)
		wantA  map[string]int64
	}{
		"same Pod identity on three clusters": {wantA: map[string]int64{"node-0": 2000}},
		"delete one Pod": {
			action: func(cache *nonTasUsageCache, log logr.Logger) {
				cache.deleteWithCluster("worker1", client.ObjectKey{Namespace: "ns", Name: "pod"}, log)
			},
		},
		"resize one Pod": {
			action: func(cache *nonTasUsageCache, log logr.Logger) {
				cache.updateWithCluster("worker1", makePod("pod", "ns", "node-0", "4"), log)
			},
			wantA: map[string]int64{"node-0": 4000},
		},
		"move one Pod": {
			action: func(cache *nonTasUsageCache, log logr.Logger) {
				cache.updateWithCluster("worker1", makePod("pod", "ns", "node-1", "2"), log)
			},
			wantA: map[string]int64{"node-1": 2000},
		},
		"terminate one Pod": {
			action: func(cache *nonTasUsageCache, log logr.Logger) {
				pod := makePod("pod", "ns", "node-0", "2")
				pod.Status.Phase = corev1.PodSucceeded
				cache.updateWithCluster("worker1", pod, log)
			},
		},
		"remove one worker": {
			action: func(cache *nonTasUsageCache, _ logr.Logger) { cache.deleteCluster("worker1") },
		},
		"reconnect and replay": {
			action: func(cache *nonTasUsageCache, log logr.Logger) {
				cache.deleteCluster("worker1")
				for range 2 {
					cache.updateWithCluster("worker1", makePod("pod", "ns", "node-0", "1"), log)
				}
			},
			wantA: map[string]int64{"node-0": 1000},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, log := utiltesting.ContextWithLog(t)
			cache := &nonTasUsageCache{
				podUsage: make(map[client.ObjectKey]podUsageValue), nodeUsage: make(map[string]resources.Requests),
			}
			cache.update(makePod("pod", "ns", "node-0", "1"), log)
			cache.updateWithCluster("worker1", makePod("pod", "ns", "node-0", "2"), log)
			cache.updateWithCluster("worker2", makePod("pod", "ns", "node-0", "3"), log)
			if tc.action != nil {
				tc.action(cache, log)
			}
			got := make(map[utiltas.NodeKey]int64)
			cache.forEachClusterNodeUsage(func(node utiltas.NodeKey, usage resources.Requests) {
				got[node] = usage.ResourceValue(corev1.ResourceCPU)
				if pods := usage.ResourceValue(corev1.ResourcePods); pods != 1 {
					t.Errorf("node %v has %d Pods, want 1", node, pods)
				}
			})
			want := map[utiltas.NodeKey]int64{
				{Name: "node-0"}: 1000, {Cluster: "worker2", Name: "node-0"}: 3000,
			}
			for node, cpu := range tc.wantA {
				want[utiltas.NodeKey{Cluster: "worker1", Name: node}] = cpu
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("node usage mismatch (-want,+got):\n%s", diff)
			}
		})
	}
}
