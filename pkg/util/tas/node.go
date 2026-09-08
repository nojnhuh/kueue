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
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/kueue/pkg/constants"
)

type NodeKey struct {
	Cluster string
	Name    string
}

func NodeKeyFor(node *corev1.Node) NodeKey {
	return NodeKey{Cluster: node.Labels[constants.MultiKueueClusterLabel], Name: node.Name}
}

// HostnameDomainID identifies the same capacity across flavors, independent of
// their rack/zone hierarchy. Empty cluster preserves single-cluster identities.
func HostnameDomainID(cluster, hostname string) TopologyDomainID {
	if cluster == "" {
		return TopologyDomainID(hostname)
	}
	return DomainID([]string{cluster, hostname})
}

// ClusterFromTopology extracts the worker from a complete manager assignment.
// Local and partial paths do not identify a worker cluster.
func ClusterFromTopology(levels, values []string) string {
	if len(levels) > 0 && len(levels) == len(values) && levels[0] == constants.MultiKueueClusterLabel {
		return values[0]
	}
	return ""
}

// NodeHostname returns the value that identifies the node in a hostname-level
// topology domain: its kubernetes.io/hostname label, which can differ from the
// Node name, or the Node name when the label is missing.
func NodeHostname(node *corev1.Node) string {
	if hostname := node.Labels[corev1.LabelHostname]; hostname != "" {
		return hostname
	}
	return node.Name
}

// NodeMatchesFlavor checks if a node's labels match the required labels
// and contains all required topology levels. Returns true if matches.
func NodeMatchesFlavor(nodeLabels map[string]string, requiredLabels map[string]string, requiredLevels []string) bool {
	for k, v := range requiredLabels {
		if nodeLabels[k] != v {
			return false
		}
	}
	for _, level := range requiredLevels {
		if _, ok := nodeLabels[level]; !ok {
			return false
		}
	}
	return true
}
