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

package centralizedtas

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/test/util"
)

var _ = ginkgo.Describe("Centralized TAS", ginkgo.Label("area:multikueue", "feature:centralizedtas", "feature:tas"), func() {
	var (
		fixture   *centralizedTASFixture
		managerLq *kueue.LocalQueue
	)

	ginkgo.BeforeEach(func() {
		fixture = setupCentralizedTASFixture(managerCQSpec{generated: true, cpu: "8", memory: "8Gi"})
		managerLq = createManagerLQ(fixture, fixture.managerCQs[0].Name, "user-queue")
	})

	ginkgo.AfterEach(func() {
		cleanupCentralizedTASFixture(fixture)
	})

	ginkgo.It("should compute manager topology with cluster and hostname levels before dispatch", func() {
		job := createTASJob("topology-levels", fixture.managerNs.Name, managerLq.Name, 1, "500m")
		util.MustCreate(ctx, k8sManagerClient, job)
		waitForJobManagedByMultiKueue(job)

		wlKey := workloadKeyForJob(job)
		clusterName, levels := waitForManagerCentralizedAdmission(wlKey)
		gomega.Expect(levels).To(gomega.Equal([]string{"kueue.x-k8s.io/multikueue-cluster", corev1.LabelHostname}))
		gomega.Expect(clusterName).To(gomega.Or(gomega.Equal(fixture.workerCluster1.Name), gomega.Equal(fixture.workerCluster2.Name)))

		tasNode := getTASWorkerNode(fixture.workers[clusterName].client)
		expectWorkerPodsHostPinned(fixture.workers[clusterName].client, fixture.managerNs.Name, job.Name, tasNode.Name, 1)
	})

	ginkgo.It("should route around a worker whose TAS node is occupied by a non-TAS pod", func() {
		worker1Node := getTASWorkerNode(k8sWorker1Client)
		hogCPU := cpuRequestToSaturateNode(k8sWorker1Client, worker1Node, resource.MustParse("500m"))

		ginkgo.By("occupying worker1 TAS node with a non-TAS pod", func() {
			_ = createNonTASPodOnNode(k8sWorker1Client, fixture.worker1Ns.Name, "hog", worker1Node.Name, hogCPU)
		})

		ginkgo.By("waiting for the manager to ingest remote pod usage", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				pods := &corev1.PodList{}
				g.Expect(k8sWorker1Client.List(ctx, pods, client.InNamespace(fixture.worker1Ns.Name))).To(gomega.Succeed())
				g.Expect(pods.Items).NotTo(gomega.BeEmpty())
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})

		job := createTASJob("avoid-worker1", fixture.managerNs.Name, managerLq.Name, 1, "1")
		util.MustCreate(ctx, k8sManagerClient, job)
		waitForJobManagedByMultiKueue(job)

		wlKey := workloadKeyForJob(job)
		waitForWorkloadAdmittedOnCluster(wlKey, fixture.workerCluster2.Name)

		worker2Node := getTASWorkerNode(k8sWorker2Client)
		expectWorkerPodsHostPinned(k8sWorker2Client, fixture.managerNs.Name, job.Name, worker2Node.Name, 1)
	})

	ginkgo.It("should account for identically named external Pods independently on each worker", func() {
		firstNode := getTASWorkerNode(k8sWorker1Client)
		secondNode := getTASWorkerNode(k8sWorker2Client)
		firstHog := createNonTASPodOnNode(k8sWorker1Client, fixture.worker1Ns.Name, "same-hog", firstNode.Name,
			cpuRequestToSaturateNode(k8sWorker1Client, firstNode, resource.MustParse("500m")))
		secondHog := createNonTASPodOnNode(k8sWorker2Client, fixture.worker2Ns.Name, "same-hog", secondNode.Name,
			cpuRequestToSaturateNode(k8sWorker2Client, secondNode, resource.MustParse("500m")))

		expectPendingWithoutReservation := func(name string) {
			ginkgo.GinkgoHelper()
			probe := createTASJob(name, fixture.managerNs.Name, managerLq.Name, 1, "1")
			util.MustCreate(ctx, k8sManagerClient, probe)
			waitForJobManagedByMultiKueue(probe)
			key := workloadKeyForJob(probe)
			gomega.Eventually(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, key, wl)).To(gomega.Succeed())
				g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusFalse(kueue.WorkloadQuotaReserved))
			}, util.Timeout, util.Interval).Should(gomega.Succeed())
			gomega.Consistently(func(g gomega.Gomega) {
				wl := &kueue.Workload{}
				g.Expect(k8sManagerClient.Get(ctx, key, wl)).To(gomega.Succeed())
				g.Expect(workload.HasQuotaReservation(wl)).To(gomega.BeFalse())
			}, util.ShortConsistentDuration, util.ShortInterval).Should(gomega.Succeed())
			// Probe fresh admissions after releasing capacity; remote-Pod
			// notifications do not currently requeue inadmissible workloads.
			util.ExpectObjectToBeDeleted(ctx, k8sManagerClient, probe, true)
			util.ExpectObjectToBeDeleted(ctx, k8sManagerClient, &kueue.Workload{
				ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
			}, true)
		}

		expectPendingWithoutReservation("blocked-before-first-release")
		util.ExpectObjectToBeDeleted(ctx, k8sWorker1Client, firstHog, true)
		first := createTASJob("first-isolated", fixture.managerNs.Name, managerLq.Name, 1, "1")
		util.MustCreate(ctx, k8sManagerClient, first)
		waitForJobManagedByMultiKueue(first)
		waitForWorkloadAdmittedOnCluster(workloadKeyForJob(first), fixture.workerCluster1.Name)
		expectWorkerPodsHostPinned(k8sWorker1Client, fixture.managerNs.Name, first.Name, firstNode.Name, 1)

		_ = createNonTASPodOnNode(k8sWorker1Client, fixture.worker1Ns.Name, "same-hog", firstNode.Name,
			cpuRequestToSaturateNode(k8sWorker1Client, firstNode, resource.MustParse("500m")))
		expectPendingWithoutReservation("blocked-before-second-release")
		util.ExpectObjectToBeDeleted(ctx, k8sWorker2Client, secondHog, true)
		second := createTASJob("second-isolated", fixture.managerNs.Name, managerLq.Name, 1, "1")
		util.MustCreate(ctx, k8sManagerClient, second)
		waitForJobManagedByMultiKueue(second)
		waitForWorkloadAdmittedOnCluster(workloadKeyForJob(second), fixture.workerCluster2.Name)
		expectWorkerPodsHostPinned(k8sWorker2Client, fixture.managerNs.Name, second.Name, secondNode.Name, 1)
	})

	ginkgo.It("should not admit workloads when central quota exceeds physical fleet capacity", func() {
		ginkgo.By("recreating fixture with inflated manager quota", func() {
			cleanupCentralizedTASFixture(fixture)
			fixture = setupCentralizedTASFixture(managerCQSpec{generated: true, cpu: "1000", memory: "1000Gi"})
			managerLq = createManagerLQ(fixture, fixture.managerCQs[0].Name, "user-queue")
		})

		podCPU := smallestTASNodeCPU(k8sWorker1Client, k8sWorker2Client)
		podCPU.Sub(resource.MustParse("1"))
		job := createTASJob("oversubscribed", fixture.managerNs.Name, managerLq.Name, 3, podCPU.String())
		util.MustCreate(ctx, k8sManagerClient, job)
		waitForJobManagedByMultiKueue(job)

		wlKey := workloadKeyForJob(job)
		expectWorkloadNotAdmitted(wlKey)
	})
})

var _ = ginkgo.Describe("Centralized TAS fair sharing", ginkgo.Label("area:multikueue", "feature:centralizedtas", "feature:fairsharing", "feature:tas"), func() {
	var (
		fixture *centralizedTASFixture
		cohort  kueue.CohortReference
		teamACQ *kueue.ClusterQueue
		teamBCQ *kueue.ClusterQueue
		teamALq *kueue.LocalQueue
		teamBLq *kueue.LocalQueue
	)

	ginkgo.BeforeEach(func() {
		cohort = kueue.CohortReference("centralized-tas-cohort")
		fixture = setupCentralizedTASFixture(
			managerCQSpec{name: "team-a", cpu: "1", memory: "1Gi", cohort: cohort},
			managerCQSpec{name: "team-b", cpu: "3", memory: "3Gi", cohort: cohort},
		)
		for _, cq := range fixture.managerCQs {
			switch cq.Name {
			case "team-a":
				teamACQ = cq
			case "team-b":
				teamBCQ = cq
			}
		}
		teamALq = createManagerLQ(fixture, teamACQ.Name, "team-a")
		teamBLq = createManagerLQ(fixture, teamBCQ.Name, "team-b")
	})

	ginkgo.AfterEach(func() {
		cleanupCentralizedTASFixture(fixture)
	})

	ginkgo.It("should reduce high borrowing as workloads finish across worker clusters", func() {
		var teamAJobs []*batchv1.Job
		ginkgo.By("team-a submits workloads that borrow from the cohort", func() {
			for i := range 3 {
				job := createTASJob(fmt.Sprintf("team-a-%d", i), fixture.managerNs.Name, teamALq.Name, 2, "500m")
				util.MustCreate(ctx, k8sManagerClient, job)
				waitForJobManagedByMultiKueue(job)
				teamAJobs = append(teamAJobs, job)
			}
		})

		ginkgo.By("waiting for team-a to enter high borrowing", func() {
			waitForClusterQueueWeightedShare(teamACQ.Name, ">", 100)
		})
		peakBorrowing := waitForClusterQueueWeightedShare(teamACQ.Name, ">=", 100)

		ginkgo.By("team-b also gets admitted work on the global pool", func() {
			job := createTASJob("team-b-0", fixture.managerNs.Name, teamBLq.Name, 1, "500m")
			util.MustCreate(ctx, k8sManagerClient, job)
			waitForJobManagedByMultiKueue(job)
			util.ExpectWorkloadsToBeAdmittedByKeys(ctx, k8sManagerClient, workloadKeyForJob(job))
		})

		ginkgo.By("finishing team-a workloads on both worker clusters", func() {
			for _, job := range teamAJobs {
				wlKey := workloadKeyForJob(job)
				clusterName, _ := waitForManagerCentralizedAdmission(wlKey)
				terminateJobPods(fixture, clusterName, fixture.managerNs.Name, job.Name, 2)
				gomega.Eventually(func(g gomega.Gomega) {
					wl := &kueue.Workload{}
					g.Expect(k8sManagerClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
					g.Expect(wl.Status.Conditions).To(utiltesting.HaveConditionStatusTrueAndReason(kueue.WorkloadFinished, kueue.WorkloadFinishedReasonSucceeded))
				}, util.VeryLongTimeout, util.Interval).Should(gomega.Succeed())
			}
		})

		ginkgo.By("expecting team-a borrowing to drop as capacity returns to the cohort", func() {
			gomega.Eventually(func(g gomega.Gomega) {
				cq := &kueue.ClusterQueue{}
				g.Expect(k8sManagerClient.Get(ctx, client.ObjectKeyFromObject(teamACQ), cq)).To(gomega.Succeed())
				g.Expect(cq.Status.FairSharing).NotTo(gomega.BeNil())
				g.Expect(cq.Status.FairSharing.WeightedShare).To(gomega.BeNumerically("<", peakBorrowing))
				g.Expect(cq.Status.AdmittedWorkloads).To(gomega.Equal(int32(0)))
			}, util.LongTimeout, util.Interval).Should(gomega.Succeed())
		})
	})
})

var _ = ginkgo.Describe("Centralized TAS placement invariants", ginkgo.Label("area:multikueue", "feature:centralizedtas", "feature:tas"), func() {
	var (
		fixture   *centralizedTASFixture
		managerLq *kueue.LocalQueue
	)

	ginkgo.BeforeEach(func() {
		fixture = setupCentralizedTASFixture(managerCQSpec{generated: true, cpu: "8", memory: "8Gi"})
		managerLq = createManagerLQ(fixture, fixture.managerCQs[0].Name, "user-queue")
	})

	ginkgo.AfterEach(func() {
		cleanupCentralizedTASFixture(fixture)
	})

	ginkgo.It("should keep an entire gang on a single worker cluster", func() {
		job := createTASJob("gang", fixture.managerNs.Name, managerLq.Name, 3, "500m")
		util.MustCreate(ctx, k8sManagerClient, job)
		waitForJobManagedByMultiKueue(job)

		wlKey := workloadKeyForJob(job)
		clusterName, _ := waitForManagerCentralizedAdmission(wlKey)

		wl := &kueue.Workload{}
		gomega.Expect(k8sManagerClient.Get(ctx, wlKey, wl)).To(gomega.Succeed())
		ta := wl.Status.Admission.PodSetAssignments[0].TopologyAssignment
		gomega.Expect(ta.Slices).NotTo(gomega.BeEmpty())
		for _, slice := range ta.Slices {
			gomega.Expect(slice.ValuesPerLevel).NotTo(gomega.BeEmpty())
			clusterVal := slice.ValuesPerLevel[0].Universal
			gomega.Expect(clusterVal).NotTo(gomega.BeNil())
			gomega.Expect(*clusterVal).To(gomega.Equal(clusterName))
		}

		otherCluster := fixture.workerCluster2.Name
		if clusterName == fixture.workerCluster2.Name {
			otherCluster = fixture.workerCluster1.Name
		}
		pods := &corev1.PodList{}
		gomega.Consistently(func(g gomega.Gomega) {
			g.Expect(fixture.workers[otherCluster].client.List(ctx, pods, client.InNamespace(fixture.managerNs.Name))).To(gomega.Succeed())
			g.Expect(pods.Items).To(gomega.BeEmpty())
		}, util.ShortConsistentDuration, util.ShortInterval).Should(gomega.Succeed())
	})
})
