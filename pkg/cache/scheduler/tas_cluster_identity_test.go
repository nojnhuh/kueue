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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/kueue/pkg/constants"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

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
