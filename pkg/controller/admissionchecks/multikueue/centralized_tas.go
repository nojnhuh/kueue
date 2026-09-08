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

	"sigs.k8s.io/kueue/pkg/constants"
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
				tasCache.DeleteNodeByName(node.Name)
			}
		},
	}); err != nil {
		return err
	}

	log.V(2).Info("Started centralized-TAS remote node inventory watchers")
	return nil
}
