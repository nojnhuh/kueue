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

package multikueue

// This file implements centralized TAS inventory for MultiKueue. The manager
// feeds remote Nodes into the same scheduler TAS cache the single-cluster TAS
// controllers use. The feature is controlled by the
// MultiKueueCentralizedTAS feature gate.

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
)

// startTASInventoryWatchers feeds remote worker Nodes into the manager's TAS
// cache using the same mutators as the single-cluster TAS controllers.
func (rc *remoteClient) startTASInventoryWatchers(ctx context.Context) error {
	tasCache := rc.schedulerCache.TASCache()
	log := ctrl.LoggerFrom(ctx).WithValues("clusterName", rc.clusterName)

	syncNode := func(obj any) {
		node, ok := obj.(*corev1.Node)
		if !ok {
			return
		}
		// Stamp the cluster label so the manager sees this node under the
		// worker's literal topology level, then sync it verbatim.
		nodeCopy := node.DeepCopy()
		if nodeCopy.Labels == nil {
			nodeCopy.Labels = map[string]string{}
		}
		nodeCopy.Labels[constants.MultiKueueClusterLabel] = rc.clusterName
		tasCache.SyncNode(nodeCopy)
	}

	if _, err := rc.client.AddCacheEventHandler(ctx, &corev1.Node{}, toolscache.ResourceEventHandlerFuncs{
		AddFunc:    syncNode,
		UpdateFunc: func(_, newObj any) { syncNode(newObj) },
		DeleteFunc: func(obj any) {
			if node, err := deletedObjectState[*corev1.Node](obj); err == nil {
				tasCache.DeleteNodeByNameWithCluster(rc.clusterName, node.Name)
			}
		},
	}); err != nil {
		return err
	}

	log.V(2).Info("Started centralized-TAS remote node inventory watchers")
	return nil
}

// projectAdmissionForWorker translates the manager's node-level admission into
// the admission the worker must execute verbatim. The manager assignment uses
// the worker cluster as its top (literal) topology level and the node hostname
// as its lowest level; the projection strips the cluster level and collapses
// the assignment to a host-exact placement (Levels=[hostname]) that the worker
// ungater applies as plain node selectors on real worker nodes.
//
// It returns the chosen worker cluster (the shared top-level value), the
// projected Admission, and ok=false when the manager has not yet computed a
// node-level assignment (so the caller falls back to normal MultiKueue).
func projectAdmissionForWorker(local *kueue.Workload) (string, *kueue.Admission, bool) {
	if local.Status.Admission == nil {
		return "", nil, false
	}
	out := local.Status.Admission.DeepCopy()
	clusterName := ""
	sawTopology := false
	for i := range out.PodSetAssignments {
		psa := &out.PodSetAssignments[i]
		if psa.TopologyAssignment == nil {
			continue
		}
		internal := utiltas.InternalFrom(psa.TopologyAssignment)
		if len(internal.Levels) < 2 {
			// Expect at least [cluster-label, ..., hostname]; without a cluster
			// level we cannot pick a worker, so treat as not-yet-ready.
			return "", nil, false
		}
		sawTopology = true

		countByHost := make(map[string]int32)
		var hostOrder []string
		for _, d := range internal.Domains {
			cluster := d.Values[0]
			if clusterName == "" {
				clusterName = cluster
			} else if clusterName != cluster {
				// A workload must be pinned to a single worker cluster.
				return "", nil, false
			}
			host := d.Values[len(d.Values)-1]
			if _, seen := countByHost[host]; !seen {
				hostOrder = append(hostOrder, host)
			}
			countByHost[host] += d.Count
		}

		worker := &utiltas.TopologyAssignment{Levels: []string{corev1.LabelHostname}}
		for _, host := range hostOrder {
			worker.Domains = append(worker.Domains, utiltas.TopologyDomainAssignment{
				Values: []string{host},
				Count:  countByHost[host],
			})
		}
		psa.TopologyAssignment = utiltas.V1Beta2From(worker)
		psa.DelayedTopologyRequest = nil
	}
	if !sawTopology || clusterName == "" {
		return "", nil, false
	}
	return clusterName, out, true
}
