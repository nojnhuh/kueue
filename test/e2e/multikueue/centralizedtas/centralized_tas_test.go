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
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
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
