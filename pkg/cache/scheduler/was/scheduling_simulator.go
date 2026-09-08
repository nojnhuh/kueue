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
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/backend/cache"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeunschedulable"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/scheduler-library/pkg/framework"
	schedLibSimulator "sigs.k8s.io/scheduler-library/pkg/simulator"
	schedLibSnapshot "sigs.k8s.io/scheduler-library/pkg/upstreamsync/snapshot"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/cache/scheduler/simulator"
	"sigs.k8s.io/kueue/pkg/features"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
)

type snapshotFactory func(ctx context.Context, pods []*corev1.Pod, nodes []*corev1.Node) (*schedLibSnapshot.ClusterSnapshot, error)

type wasSimulator struct {
	newSnapshot snapshotFactory
	pods        podTracker
}

type wasSimulatorSnapshot struct {
	// wasSnapshot is the cluster as it stands, with every tracked Pod on its node.
	wasSnapshot *schedLibSnapshot.ClusterSnapshot
	// podsByWorkload indexes the tracked Pods by the Workload that owns them, which
	// is the only set PreemptWorkload can release.
	podsByWorkload podsByWorkload
	// emptyCluster holds the same nodes with no Pods, for callers asking what would
	// fit if nothing were running.
	emptyCluster lazyCluster
}

// lazyCluster builds its cluster on first use, so cycles that never ask do not
// pay for it.
type lazyCluster struct {
	// build produces the cluster. It runs at most once.
	build func(context.Context) (*schedLibSnapshot.ClusterSnapshot, error)
	once  sync.Once
	// value and err hold what build returned, and are only read after once has run.
	value *schedLibSnapshot.ClusterSnapshot
	err   error
}

func (l *lazyCluster) get(ctx context.Context) (*schedLibSnapshot.ClusterSnapshot, error) {
	l.once.Do(func() {
		l.value, l.err = l.build(ctx)
	})
	return l.value, l.err
}

var _ simulator.SimulatorSnapshot = (*wasSimulatorSnapshot)(nil)

func newWASSchedulerConfig() *schedulerconfig.KubeSchedulerConfiguration {
	return &schedulerconfig.KubeSchedulerConfiguration{
		Profiles: []schedulerconfig.KubeSchedulerProfile{
			{
				SchedulerName: corev1.DefaultSchedulerName,
				// https://kubernetes.io/docs/reference/scheduling/config/#scheduling-plugins
				Plugins: &schedulerconfig.Plugins{
					QueueSort: schedulerconfig.PluginSet{
						Enabled: []schedulerconfig.Plugin{{Name: queuesort.Name}},
					},
					Bind: schedulerconfig.PluginSet{
						Enabled: []schedulerconfig.Plugin{{Name: defaultbinder.Name}},
					},
					Filter: schedulerconfig.PluginSet{
						Enabled: []schedulerconfig.Plugin{
							{Name: nodeunschedulable.Name},
							{Name: tainttoleration.Name},
							{Name: nodeaffinity.Name},
							{Name: nodeports.Name},
						},
					},
					PreFilter: schedulerconfig.PluginSet{
						Enabled: []schedulerconfig.Plugin{
							{Name: nodeaffinity.Name},
							{Name: nodeports.Name},
						},
					},
				},
				PluginConfig: []schedulerconfig.PluginConfig{
					{
						Name: nodeaffinity.Name,
						Args: &schedulerconfig.NodeAffinityArgs{},
					},
				},
			},
		},
	}
}

func newWASSimulator(ctx context.Context, client kubernetes.Interface) (*wasSimulator, error) {
	cfg := newWASSchedulerConfig()

	snapshotFn := func(ctx context.Context, pods []*corev1.Pod, nodes []*corev1.Node) (*schedLibSnapshot.ClusterSnapshot, error) {
		// Building the framework registers a DRA index on the factory it is given, so it
		// cannot be shared across snapshots, and the enabled plugins read the snapshot
		// rather than the informers, so it is not needed once the framework is built.
		buildCtx, cancelBuild := context.WithCancel(ctx)
		defer cancelBuild()
		informerFactory := informers.NewSharedInformerFactory(client, 0)

		// Register node and pod informers with the factory; sync errors are caught by AsError() below.
		_ = informerFactory.Core().V1().Nodes().Informer()
		_ = informerFactory.Core().V1().Pods().Informer()
		informerFactory.StartWithContext(buildCtx)
		if err := informerFactory.WaitForCacheSyncWithContext(buildCtx).AsError(); err != nil {
			return nil, err
		}
		snap := cache.NewSnapshot(pods, nodes)
		profiles, err := framework.NewProfileMap(buildCtx, client, informerFactory, snap, cfg)
		if err != nil {
			return nil, err
		}
		return schedLibSnapshot.New(snap, profiles), nil
	}

	return &wasSimulator{
		newSnapshot: snapshotFn,
		pods: podTracker{
			pods:         make(podsByKey),
			workloadPods: make(podsByWorkload),
		},
	}, nil
}

func NewWASSimulator(ctx context.Context, restConfig *rest.Config) (*wasSimulator, error) {
	if restConfig != nil {
		// TODO(#13534): when DRA plugins are added, use a real client here
		// instead of the fake so the informer factory is populated.
		if _, err := schedLibSimulator.NewReadonlyClient(restConfig); err != nil {
			return nil, err
		}
	}
	return newWASSimulator(ctx, fake.NewSimpleClientset())
}

func (s *wasSimulator) Snapshot(ctx context.Context, nodes []*corev1.Node) (simulator.SimulatorSnapshot, error) {
	allPods, ownersByWorkload := s.pods.snapshot()
	nodesByCluster := map[string][]*corev1.Node{"": nil}
	for _, node := range nodes {
		cluster := utiltas.NodeKeyFor(node).Cluster
		nodesByCluster[cluster] = append(nodesByCluster[cluster], node)
	}
	if len(nodesByCluster) == 1 {
		return s.clusterSnapshot(ctx, allPods, ownersByWorkload, nodes)
	}
	result := &multiClusterSimulatorSnapshot{clusters: make(map[string]*wasSimulatorSnapshot, len(nodesByCluster))}
	for _, cluster := range slices.Sorted(maps.Keys(nodesByCluster)) {
		var pods []*corev1.Pod
		var owners podsByWorkload
		if cluster == "" {
			pods, owners = allPods, ownersByWorkload
		}
		snapshot, err := s.clusterSnapshot(ctx, pods, owners, nodesByCluster[cluster])
		if err != nil {
			return nil, fmt.Errorf("building simulator snapshot for cluster %q: %w", cluster, err)
		}
		result.clusters[cluster] = snapshot
	}
	return result, nil
}

func (s *wasSimulator) clusterSnapshot(ctx context.Context, allPods []*corev1.Pod, owners podsByWorkload, nodes []*corev1.Node) (*wasSimulatorSnapshot, error) {
	clusterSnap, err := s.newSnapshot(ctx, allPods, nodes)
	if err != nil {
		return nil, err
	}
	snapshot := &wasSimulatorSnapshot{
		wasSnapshot:    clusterSnap,
		podsByWorkload: owners,
	}
	snapshot.emptyCluster.build = func(ctx context.Context) (*schedLibSnapshot.ClusterSnapshot, error) {
		return s.newSnapshot(ctx, podsNotManagedByKueue(allPods, owners), nodes)
	}
	return snapshot, nil
}

type multiClusterSimulatorSnapshot struct {
	clusters map[string]*wasSimulatorSnapshot
}

func (s *multiClusterSimulatorSnapshot) FindFeasibleNodes(
	ctx context.Context,
	candidates iter.Seq[simulator.Candidate],
	requirements *simulator.PodRequirements,
	stats *simulator.NodeExclusionStats,
) ([]simulator.MatchedCandidate, error) {
	byCluster := make(map[string][]simulator.Candidate)
	for candidate := range candidates {
		node := candidate.GetNode()
		if node == nil {
			return nil, fmt.Errorf("simulator candidate %q has no node", candidate.GetID())
		}
		cluster := utiltas.NodeKeyFor(node).Cluster
		byCluster[cluster] = append(byCluster[cluster], candidate)
	}
	var result []simulator.MatchedCandidate
	for _, cluster := range slices.Sorted(maps.Keys(byCluster)) {
		snapshot, found := s.clusters[cluster]
		if !found {
			return nil, fmt.Errorf("simulator snapshot for cluster %q not found", cluster)
		}
		var clusterStats simulator.NodeExclusionStats
		matches, err := snapshot.FindFeasibleNodes(ctx, slices.Values(byCluster[cluster]), requirements, &clusterStats)
		if err != nil {
			return nil, fmt.Errorf("simulating placement in cluster %q: %w", cluster, err)
		}
		stats.TotalNodes += clusterStats.TotalNodes
		stats.SchedulerLibraryNoFit += clusterStats.SchedulerLibraryNoFit
		result = append(result, matches...)
	}
	return result, nil
}

func (s *multiClusterSimulatorSnapshot) PreemptWorkload(ctx context.Context, key client.ObjectKey) (func() error, error) {
	// The tracker contains only local Pods; worker Pods are not ingested into WAS.
	return s.clusters[""].PreemptWorkload(ctx, key)
}

func (s *multiClusterSimulatorSnapshot) Simulate(ctx context.Context, fn func()) error {
	clusters := slices.Sorted(maps.Keys(s.clusters))
	var simulate func(int) error
	simulate = func(index int) error {
		if index == len(clusters) {
			fn()
			return nil
		}
		var innerErr error
		err := s.clusters[clusters[index]].Simulate(ctx, func() {
			innerErr = simulate(index + 1)
		})
		return errors.Join(err, innerErr)
	}
	return simulate(0)
}

// podsNotManagedByKueue returns the Pods that belong to no Workload. Preemption
// cannot remove them, so they keep occupying their node even when the caller assumes
// every Workload is gone.
func podsNotManagedByKueue(allPods []*corev1.Pod, byWorkload podsByWorkload) []*corev1.Pod {
	managed := sets.New[client.ObjectKey]()
	for _, pods := range byWorkload {
		managed.Insert(slices.Collect(maps.Keys(pods))...)
	}
	var kept []*corev1.Pod
	for _, pod := range allPods {
		if !managed.Has(client.ObjectKeyFromObject(pod)) {
			kept = append(kept, pod)
		}
	}
	return kept
}

func (s *wasSimulator) TrackPod(ctx context.Context, pod *corev1.Pod) {
	if _, ok := pod.Annotations[kueue.WorkloadAnnotation]; !ok {
		ctrl.LoggerFrom(ctx).V(1).Info(
			"Missing annotation on Pod object; Quality of WAS simulation may be degraded.",
			"pod", client.ObjectKeyFromObject(pod).String(),
			"missing annotation", kueue.WorkloadAnnotation,
		)
	}
	s.pods.track(pod)
}

func (s *wasSimulator) UntrackPod(_ context.Context, key client.ObjectKey) {
	s.pods.untrack(key)
}

func (s *wasSimulatorSnapshot) FindFeasibleNodes(
	ctx context.Context,
	candidates iter.Seq[simulator.Candidate],
	requirements *simulator.PodRequirements,
	stats *simulator.NodeExclusionStats,
) ([]simulator.MatchedCandidate, error) {
	var candidateLeaves = make(map[string]simulator.MatchedCandidate)
	var candidateNodeNames []string
	var feasibleCandidates []simulator.MatchedCandidate

	for candidate := range candidates {
		matchedCandidate, ok := candidate.(simulator.MatchedCandidate)
		if !ok {
			return nil, fmt.Errorf("failed to cast candidate %T to simulator.MatchedCandidate", candidate)
		}

		stats.TotalNodes++
		nodeObj := candidate.GetNode()
		candidateNodeNames = append(candidateNodeNames, nodeObj.Name)
		candidateLeaves[nodeObj.Name] = matchedCandidate
	}

	dummyPod := &corev1.Pod{
		ObjectMeta: requirements.PodTemplate.ObjectMeta,
		Spec:       requirements.PodTemplate.Spec,
	}
	// The simulator builds one profile, so judge the Pod by it rather than by the scheduler it names.
	dummyPod.Spec.SchedulerName = corev1.DefaultSchedulerName
	cluster := s.wasSnapshot
	if requirements.SimulateEmpty {
		var err error
		if cluster, err = s.emptyCluster.get(ctx); err != nil {
			return nil, err
		}
	}
	placement, err := cluster.MakePlacement(candidateNodeNames)
	if err != nil {
		return nil, err
	}
	feasibleNodeNames, _, err := cluster.CanSchedulePod(ctx, dummyPod, placement)
	if err != nil {
		return nil, err
	}

	for _, nodeName := range feasibleNodeNames {
		leaf := candidateLeaves[nodeName]
		feasibleCandidates = append(feasibleCandidates, leaf)
		if features.Enabled(features.TASRespectNodeAffinityPreferred) && requirements.PreferredSchedulingTerms != nil {
			newAffinityScore := leaf.GetAffinityScore() + requirements.PreferredSchedulingTerms.Score(leaf.GetNode())
			leaf.SetAffinityScore(newAffinityScore)
		}
	}
	stats.SchedulerLibraryNoFit = len(candidateNodeNames) - len(feasibleNodeNames)

	return feasibleCandidates, nil
}

func (s *wasSimulatorSnapshot) PreemptWorkload(ctx context.Context, wlKey client.ObjectKey) (func() error, error) {
	// Pods with indeterminate workloads are not stored in s.podsByWorkload and are omitted from preemptions.
	// This means the simulation may be more restrictive than the real scheduler would be,
	// if the preempted workload has pods that do not identify with it directly.
	unpreempt, err := s.wasSnapshot.PreemptPods(ctx, s.podsByWorkload.getPodsForWorkload(wlKey))
	if err != nil {
		return nil, fmt.Errorf("failed to preempt workload's pods from WAS snapshot: %w", err)
	}

	return func() error {
		_, err := s.wasSnapshot.Unpreempt(unpreempt)
		return err
	}, nil
}

func (s *wasSimulatorSnapshot) Simulate(ctx context.Context, fn func()) error {
	return s.wasSnapshot.Transaction(ctx, func() (schedLibSnapshot.TransactionResult, error) {
		fn()
		return schedLibSnapshot.Revert, nil
	})
}
