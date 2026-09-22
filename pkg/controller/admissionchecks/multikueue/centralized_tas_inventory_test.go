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

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	toolscache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	testingpod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
	"sigs.k8s.io/kueue/pkg/workload"
)

type inventoryEventClient struct {
	client.WithWatch
	nodes toolscache.ResourceEventHandler
	pods  toolscache.ResourceEventHandler
}

func (c *inventoryEventClient) AddCacheEventHandler(_ context.Context, obj client.Object, handler toolscache.ResourceEventHandler) (toolscache.ResourceEventHandlerRegistration, error) {
	switch obj.(type) {
	case *corev1.Node:
		c.nodes = handler
	case *corev1.Pod:
		c.pods = handler
	default:
		return nil, fmt.Errorf("unexpected inventory type %T", obj)
	}
	return nil, nil
}

func TestTASInventoryClusterIsolation(t *testing.T) {
	cases := map[string]struct {
		action func(*testing.T, context.Context, *remoteClient, *inventoryEventClient, *corev1.Node, *corev1.Pod)
		wantA  int64
	}{
		"identical names stay independent": {wantA: 4000},
		"delete only one worker Pod": {
			action: func(_ *testing.T, _ context.Context, _ *remoteClient, events *inventoryEventClient, _ *corev1.Node, pod *corev1.Pod) {
				events.pods.OnDelete(toolscache.DeletedFinalStateUnknown{Obj: pod})
			},
			wantA: 5000,
		},
		"terminal update releases only one worker Pod": {
			action: func(_ *testing.T, _ context.Context, _ *remoteClient, events *inventoryEventClient, _ *corev1.Node, pod *corev1.Pod) {
				terminal := pod.DeepCopy()
				terminal.Status.Phase = corev1.PodSucceeded
				events.pods.OnUpdate(pod, terminal)
			},
			wantA: 5000,
		},
		"stop rejects queued stale callbacks": {
			action: func(_ *testing.T, ctx context.Context, rc *remoteClient, events *inventoryEventClient, node *corev1.Node, pod *corev1.Pod) {
				rc.tasInventoryMu.Lock()
				stopped := make(chan struct{})
				go func() {
					rc.StopWatchers()
					close(stopped)
				}()
				<-ctx.Done()
				delivered := make(chan struct{})
				go func() {
					events.nodes.OnAdd(node, false)
					events.pods.OnAdd(pod, false)
					close(delivered)
				}()
				rc.tasInventoryMu.Unlock()
				<-stopped
				<-delivered
			},
		},
		"reconnect drops old Pod usage and ignores old events": {
			action: func(t *testing.T, _ context.Context, rc *remoteClient, events *inventoryEventClient, node *corev1.Node, pod *corev1.Pod) {
				oldNodes, oldPods := events.nodes, events.pods
				rc.StopWatchers()
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				rc.setWatchCancel(cancel)
				if err := rc.startTASInventoryWatchers(ctx); err != nil {
					t.Fatal(err)
				}
				events.nodes.OnAdd(node, true)
				oldNodes.OnDelete(node)
				oldPods.OnAdd(pod, false)
			},
			wantA: 5000,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.TopologyAwareScheduling, true)
			ctx, log := utiltesting.ContextWithLog(t)
			cache := schdcache.New(utiltesting.NewFakeClient())
			cache.AddOrUpdateTopology(log, utiltestingapi.MakeTopology("topology").
				Levels(constants.MultiKueueClusterLabel, corev1.LabelHostname).Obj())
			cache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("flavor").TopologyName("topology").Obj())
			if err := cache.AddClusterQueue(ctx, utiltestingapi.MakeClusterQueue("cq").
				ResourceGroup(*utiltestingapi.MakeFlavorQuotas("flavor").Resource(corev1.ResourceCPU, "10").Obj()).Obj()); err != nil {
				t.Fatal(err)
			}
			node := testingnode.MakeNode("same-node").Label(corev1.LabelHostname, "same-host").
				StatusAllocatable(corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("5"), corev1.ResourcePods: resource.MustParse("10"),
				}).Ready().Obj()
			pod := testingpod.MakePod("same-pod", "same-ns").NodeName(node.Name).
				Request(corev1.ResourceCPU, "1").StatusPhase(corev1.PodRunning).Obj()
			// A Pod's own cluster label is not its source identity.
			pod.Labels = map[string]string{constants.MultiKueueClusterLabel: "spoofed"}
			cache.TASCache().UpdateNonTASUsage(pod, log)
			events := make(map[string]*inventoryEventClient)
			clients := make(map[string]*remoteClient)
			contexts := make(map[string]context.Context)
			for _, cluster := range []string{"worker1", "worker2"} {
				watchCtx, cancel := context.WithCancel(ctx)
				t.Cleanup(cancel)
				events[cluster] = &inventoryEventClient{}
				rc := &remoteClient{clusterName: cluster, client: events[cluster], schedulerCache: cache}
				rc.setWatchCancel(cancel)
				t.Cleanup(rc.StopWatchers)
				if err := rc.startTASInventoryWatchers(watchCtx); err != nil {
					t.Fatal(err)
				}
				events[cluster].nodes.OnAdd(node, true)
				workerPod := pod.DeepCopy()
				if cluster == "worker2" {
					workerPod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("2")
				}
				events[cluster].pods.OnAdd(workerPod, true)
				clients[cluster], contexts[cluster] = rc, watchCtx
			}
			if tc.action != nil {
				tc.action(t, contexts["worker1"], clients["worker1"], events["worker1"], node, pod)
			}
			snapshot, err := cache.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			flavor := snapshot.ClusterQueue("cq").TASFlavors["flavor"]
			for cluster, capacity := range map[string]int64{"worker1": tc.wantA, "worker2": 3000} {
				usage := workload.TASFlavorUsage{{
					Cluster: cluster, Values: []string{cluster, "same-host"}, Count: 1,
					SinglePodRequests: resources.NewRequestsFromMap(resources.MapRequests{corev1.ResourceCPU: capacity}),
				}}
				if capacity > 0 && !flavor.Fits(usage) {
					t.Errorf("worker %s should fit %d milliCPU", cluster, capacity)
				}
				usage[0].SinglePodRequests = resources.NewRequestsFromMap(resources.MapRequests{corev1.ResourceCPU: capacity + 1})
				if flavor.Fits(usage) {
					t.Errorf("worker %s unexpectedly fits more than %d milliCPU", cluster, capacity)
				}
			}
			if _, found := node.Labels[constants.MultiKueueClusterLabel]; found {
				t.Error("remote Node was mutated while stamping cache provenance")
			}
		})
	}
}
