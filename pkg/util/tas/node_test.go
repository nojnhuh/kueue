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

package tas

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/kueue/pkg/constants"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
)

func TestNodeKeyFor(t *testing.T) {
	cases := map[string]struct {
		cluster string
		want    NodeKey
	}{
		"local":    {want: NodeKey{Name: "node"}},
		"worker-a": {cluster: "worker-a", want: NodeKey{Cluster: "worker-a", Name: "node"}},
		"worker-b": {cluster: "worker-b", want: NodeKey{Cluster: "worker-b", Name: "node"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			node := testingnode.MakeNode("node").
				Label(corev1.LabelHostname, "hostname").
				Label(constants.MultiKueueClusterLabel, tc.cluster).Obj()
			if got := NodeKeyFor(node); got != tc.want {
				t.Errorf("NodeKeyFor() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHostnameDomainID(t *testing.T) {
	cases := map[string]struct {
		cluster string
		want    TopologyDomainID
	}{
		"local":    {want: "hostname"},
		"worker-a": {cluster: "worker-a", want: "worker-a,hostname"},
		"worker-b": {cluster: "worker-b", want: "worker-b,hostname"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := HostnameDomainID(tc.cluster, "hostname"); got != tc.want {
				t.Errorf("HostnameDomainID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClusterFromTopology(t *testing.T) {
	cases := map[string]struct {
		levels []string
		values []string
		want   string
	}{
		"local hostname": {
			levels: []string{corev1.LabelHostname}, values: []string{"host"},
		},
		"centralized hostname": {
			levels: []string{constants.MultiKueueClusterLabel, corev1.LabelHostname},
			values: []string{"worker", "host"}, want: "worker",
		},
		"centralized hierarchy": {
			levels: []string{constants.MultiKueueClusterLabel, "rack", corev1.LabelHostname},
			values: []string{"worker", "rack", "host"}, want: "worker",
		},
		"partial hostname does not invent a worker": {
			levels: []string{constants.MultiKueueClusterLabel, corev1.LabelHostname},
			values: []string{"host"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ClusterFromTopology(tc.levels, tc.values); got != tc.want {
				t.Errorf("ClusterFromTopology() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNodeHostname(t *testing.T) {
	cases := map[string]struct {
		node *corev1.Node
		want string
	}{
		"hostname label differs from the Node name": {
			node: testingnode.MakeNode("node-x1").Label(corev1.LabelHostname, "x1").Obj(),
			want: "x1",
		},
		"hostname label equals the Node name": {
			node: testingnode.MakeNode("x1").Label(corev1.LabelHostname, "x1").Obj(),
			want: "x1",
		},
		"hostname label missing falls back to the Node name": {
			node: testingnode.MakeNode("x1").Obj(),
			want: "x1",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := NodeHostname(tc.node); got != tc.want {
				t.Errorf("NodeHostname() = %q, want %q", got, tc.want)
			}
		})
	}
}
