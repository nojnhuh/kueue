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
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	config "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	workloadjob "sigs.k8s.io/kueue/pkg/controller/jobs/job"
	"sigs.k8s.io/kueue/pkg/features"
	utiltas "sigs.k8s.io/kueue/pkg/util/tas"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	testingnode "sigs.k8s.io/kueue/pkg/util/testingjobs/node"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Centralized TAS", ginkgo.Label("area:multikueue", "feature:centralizedtas", "feature:tas"), ginkgo.Ordered, ginkgo.Serial, func() {
	const (
		clusterQueueName = "centralized-tas-cq"
		localQueueName   = "centralized-tas-lq"
		flavorName       = "centralized-tas-flavor"
		managerTopology  = "centralized-tas-manager"
		workerTopology   = "centralized-tas-worker"
		workerNodeName   = "centralized-tas-node"
		workerHostname   = "centralized-tas-host"
	)

	var (
		managerNs *corev1.Namespace
		workerNs  *corev1.Namespace

		managerSecret *corev1.Secret
		workerCluster *kueue.MultiKueueCluster
		multiKueueCfg *kueue.MultiKueueConfig
		multiKueueAC  *kueue.AdmissionCheck

		managerTopologyObj *kueue.Topology
		managerFlavor      *kueue.ResourceFlavor
		managerCQ          *kueue.ClusterQueue
		managerLQ          *kueue.LocalQueue

		workerTopologyObj *kueue.Topology
		workerFlavor      *kueue.ResourceFlavor
		workerCQ          *kueue.ClusterQueue
		workerLQ          *kueue.LocalQueue
		workerNode        *corev1.Node
	)

	ginkgo.BeforeAll(func() {
		features.SetFeatureGateDuringTest(ginkgo.GinkgoTB(), features.MultiKueueCentralizedTAS, true)
		managerTestCluster.fwk.StartManager(managerTestCluster.ctx, managerTestCluster.cfg, func(ctx context.Context, mgr manager.Manager) {
			managerAndMultiKueueSetup(
				ctx,
				mgr,
				2*time.Second,
				sets.New(workloadjob.FrameworkName),
				config.MultiKueueDispatcherModeAllAtOnce,
			)
		})
	})

	ginkgo.AfterAll(func() {
		managerTestCluster.fwk.StopManager(managerTestCluster.ctx)
	})

	ginkgo.BeforeEach(func() {
		managerNs = util.CreateNamespaceFromPrefixWithLog(managerTestCluster.ctx, managerTestCluster.client, "centralized-tas-")
		workerNs = util.CreateNamespaceWithLog(worker1TestCluster.ctx, worker1TestCluster.client, managerNs.Name)

		workerTopologyObj = utiltestingapi.MakeDefaultOneLevelTopology(workerTopology)
		util.MustCreate(worker1TestCluster.ctx, worker1TestCluster.client, workerTopologyObj)
		workerFlavor = utiltestingapi.MakeResourceFlavor(flavorName).
			NodeLabel("node-group", "tas").
			TopologyName(workerTopologyObj.Name).
			Obj()
		util.MustCreate(worker1TestCluster.ctx, worker1TestCluster.client, workerFlavor)
		workerNode = testingnode.MakeNode(workerNodeName).
			Label(corev1.LabelHostname, workerHostname).
			Label("node-group", "tas").
			StatusAllocatable(corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse("5"),
				corev1.ResourcePods: resource.MustParse("10"),
			}).
			Ready().
			Obj()
		util.CreateNodesWithStatus(worker1TestCluster.ctx, worker1TestCluster.client, []corev1.Node{*workerNode})
		workerCQ = utiltestingapi.MakeClusterQueue(clusterQueueName).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavorName).Resource(corev1.ResourceCPU, "100").Obj()).
			Obj()
		util.CreateClusterQueuesAndWaitForActive(worker1TestCluster.ctx, worker1TestCluster.client, workerCQ)
		workerLQ = utiltestingapi.MakeLocalQueue(localQueueName, workerNs.Name).ClusterQueue(workerCQ.Name).Obj()
		util.CreateLocalQueuesAndWaitForActive(worker1TestCluster.ctx, worker1TestCluster.client, workerLQ)

		workerKubeconfig, err := worker1TestCluster.kubeConfigBytes()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		managerSecret = utiltesting.MakeSecret("centralized-tas-worker", managersConfigNamespace.Name).
			Data(kueue.MultiKueueConfigSecretKey, workerKubeconfig).
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerSecret)
		workerCluster = utiltestingapi.MakeMultiKueueCluster("centralized-tas-worker").
			KubeConfig(kueue.SecretLocationType, managerSecret.Name).
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, workerCluster)
		multiKueueCfg = utiltestingapi.MakeMultiKueueConfig("centralized-tas-config").Clusters(workerCluster.Name).Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, multiKueueCfg)
		multiKueueAC = utiltestingapi.MakeAdmissionCheck("centralized-tas").
			ControllerName(kueue.MultiKueueControllerName).
			Parameters(kueue.SchemeGroupVersion.Group, "MultiKueueConfig", multiKueueCfg.Name).
			Obj()
		util.CreateAdmissionChecksAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, multiKueueAC)

		gomega.Eventually(func(g gomega.Gomega) {
			updated := &kueue.MultiKueueCluster{}
			g.Expect(managerTestCluster.client.Get(managerTestCluster.ctx, client.ObjectKeyFromObject(workerCluster), updated)).To(gomega.Succeed())
			g.Expect(updated.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.MultiKueueClusterActive))
		}, util.Timeout, util.Interval).Should(gomega.Succeed())

		managerTopologyObj = utiltestingapi.MakeTopology(managerTopology).
			Levels(constants.MultiKueueClusterLabel, corev1.LabelHostname).
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerTopologyObj)
		managerFlavor = utiltestingapi.MakeResourceFlavor(flavorName).
			NodeLabel("node-group", "tas").
			TopologyName(managerTopologyObj.Name).
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, managerFlavor)
		managerCQ = utiltestingapi.MakeClusterQueue(clusterQueueName).
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas(flavorName).Resource(corev1.ResourceCPU, "5").Obj()).
			AdmissionChecks(kueue.AdmissionCheckReference(multiKueueAC.Name)).
			Obj()
		util.CreateClusterQueuesAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, managerCQ)
		managerLQ = utiltestingapi.MakeLocalQueue(localQueueName, managerNs.Name).ClusterQueue(managerCQ.Name).Obj()
		util.CreateLocalQueuesAndWaitForActive(managerTestCluster.ctx, managerTestCluster.client, managerLQ)
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(util.DeleteNamespace(managerTestCluster.ctx, managerTestCluster.client, managerNs)).To(gomega.Succeed())
		gomega.Expect(util.DeleteNamespace(worker1TestCluster.ctx, worker1TestCluster.client, workerNs)).To(gomega.Succeed())
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerLQ, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, workerLQ, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerCQ, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, workerCQ, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerFlavor, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, workerFlavor, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerTopologyObj, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, workerTopologyObj, true)
		util.ExpectObjectToBeDeleted(worker1TestCluster.ctx, worker1TestCluster.client, workerNode, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, multiKueueAC, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, multiKueueCfg, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, workerCluster, true)
		util.ExpectObjectToBeDeleted(managerTestCluster.ctx, managerTestCluster.client, managerSecret, true)
	})

	ginkgo.It("places a workload on the worker selected by the manager", func() {
		job := testingjob.MakeJob("job", managerNs.Name).
			ManagedBy(kueue.MultiKueueControllerName).
			Queue(kueue.LocalQueueName(managerLQ.Name)).
			PodAnnotation(kueue.PodSetRequiredTopologyAnnotation, corev1.LabelHostname).
			Request(corev1.ResourceCPU, "1").
			Obj()
		util.MustCreate(managerTestCluster.ctx, managerTestCluster.client, job)

		wlKey := types.NamespacedName{
			Name:      workloadjob.GetWorkloadNameForJob(job.Name, job.UID),
			Namespace: job.Namespace,
		}
		gomega.Eventually(func(g gomega.Gomega) {
			managerWl := &kueue.Workload{}
			g.Expect(managerTestCluster.client.Get(managerTestCluster.ctx, wlKey, managerWl)).To(gomega.Succeed())
			g.Expect(managerWl.Status.ClusterName).NotTo(gomega.BeNil())
			g.Expect(*managerWl.Status.ClusterName).To(gomega.Equal(workerCluster.Name))
			g.Expect(managerWl.Status.Admission).NotTo(gomega.BeNil())
			g.Expect(managerWl.Status.Admission.PodSetAssignments).To(gomega.HaveLen(1))
			topologyAssignment := managerWl.Status.Admission.PodSetAssignments[0].TopologyAssignment
			g.Expect(topologyAssignment).NotTo(gomega.BeNil())
			assignment := utiltas.InternalFrom(topologyAssignment)
			g.Expect(assignment.Levels).To(gomega.Equal([]string{constants.MultiKueueClusterLabel, corev1.LabelHostname}))
			g.Expect(assignment.Domains).To(gomega.Equal([]utiltas.TopologyDomainAssignment{{
				Values: []string{workerCluster.Name, workerHostname},
				Count:  1,
			}}))
		}, util.Timeout, util.Interval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			workerWl := &kueue.Workload{}
			g.Expect(worker1TestCluster.client.Get(worker1TestCluster.ctx, wlKey, workerWl)).To(gomega.Succeed())
			g.Expect(workerWl.Status.Conditions).To(utiltesting.HaveConditionStatusTrue(kueue.WorkloadAdmitted))
			g.Expect(workerWl.Status.Admission).NotTo(gomega.BeNil())
			g.Expect(workerWl.Status.Admission.PodSetAssignments).To(gomega.HaveLen(1))
			topologyAssignment := workerWl.Status.Admission.PodSetAssignments[0].TopologyAssignment
			g.Expect(topologyAssignment).NotTo(gomega.BeNil())
			assignment := utiltas.InternalFrom(topologyAssignment)
			g.Expect(assignment.Levels).To(gomega.Equal([]string{corev1.LabelHostname}))
			g.Expect(assignment.Domains).To(gomega.Equal([]utiltas.TopologyDomainAssignment{{
				Values: []string{workerHostname},
				Count:  1,
			}}))
		}, util.Timeout, util.Interval).Should(gomega.Succeed())
	})
})
