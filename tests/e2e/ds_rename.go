package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	admissionapi "k8s.io/pod-security-admission/api"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	fnodes "k8s.io/kubernetes/test/e2e/framework/node"
	fpv "k8s.io/kubernetes/test/e2e/framework/pv"

	snapV1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	snapclient "github.com/kubernetes-csi/external-snapshotter/client/v6/clientset/versioned"
)

var _ bool = ginkgo.Describe("hci", func() {
	f := framework.NewDefaultFramework("hci")
	f.NamespacePodSecurityEnforceLevel = admissionapi.LevelPrivileged
	var (
		client                     clientset.Interface
		namespace                  string
		storagePolicyName          string
		scParameters               map[string]string
		pandoraSyncWaitTime        int
		snapc                      *snapclient.Clientset
		isVsanHealthServiceStopped bool
		isSPSServiceStopped        bool
		vcAddress                  string
	)

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		namespace = getNamespaceToRunTests(f)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		nodeList, err := fnodes.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err, "Unable to find ready and schedulable Node")
		if !(len(nodeList.Items) > 0) {
			framework.Failf("Unable to find ready and schedulable Node")
		}
		bootstrap()
		scParameters = make(map[string]string)
		storagePolicyName = GetAndExpectStringEnvVar(envStoragePolicyNameForSharedDatastores)

		// reading fullsync wait time
		if os.Getenv(envPandoraSyncWaitTime) != "" {
			pandoraSyncWaitTime, err = strconv.Atoi(os.Getenv(envPandoraSyncWaitTime))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		} else {
			pandoraSyncWaitTime = defaultPandoraSyncWaitTime
		}
		/*cfg, err := getConfig()
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		dcList := strings.Split(cfg.Global.Datacenters, ",")
		for _, dc := range dcList {
			dcName := strings.TrimSpace(dc)
			if dcName != "" {
				datacenters = append(datacenters, dcName)
			}
		}*/
	})

	ginkgo.AfterEach(func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		vcAddress = e2eVSphere.Config.Global.VCenterHostname + ":" + sshdPort
		if isVsanHealthServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}

		if isSPSServiceStopped {
			startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)
		}
	})

	/*
	   Create PVC and then rename datastore
	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create PVC and then rename datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 10
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		//pvcs := []*v1.PersistentVolumeClaim{}
		snapshotIDs := []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))
		ginkgo.By("")

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, accessMode)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {
				if snapshotContentCreated {
					err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandle, snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*

	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create PVC and then rename datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 10
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		snapshotIDs := []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))
		ginkgo.By("")

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, accessMode)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {
				if snapshotContentCreated {
					err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandle, snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*

	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create workloads and rename datastore"+
		" after SPS is down", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 10
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		//pvcs := []*v1.PersistentVolumeClaim{}
		snapshotIDs := []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))
		ginkgo.By("")

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, accessMode)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {
				if snapshotContentCreated {
					err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		ginkgo.By(fmt.Sprintln("Stopping SPS on the vCenter host"))
		isSPSServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, spsServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr)

		ginkgo.By(fmt.Sprintln("Starting SPS on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, spsServiceName, &isSPSServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Bring up vsan health")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandle, snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*

	   Steps:
	   1	create a SC which points to remote vsan ds
	   2	create 5 pvcs each on remote vsan ds and wait for them to be bound
	   3	attach a pod to each of the PVCs created in step 2
	   4	wait for all pods to be running and verify that the respective pvcs are accessible
	   5	storage vmotion remote workers to local datastore
	   6	verify that volumes are accessible for all the pods
	   7	storage vmotion remote workers to back to remote datastore
	   8	verify that volumes are accessible for all the pods
	   9	cleanup all the pods, pvcs and SCs created for the test
	*/
	ginkgo.It("Create workloads and rename datastore"+
		" after vsan health is down", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		pvcCount := 10
		var stsReplicas int32 = 3
		var sc *storagev1.StorageClass
		var err error
		var volHandle string
		//pvcs := []*v1.PersistentVolumeClaim{}
		snapshotIDs := []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		randomStr := strconv.Itoa(r1.Intn(1000))
		ginkgo.By("")

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		statefulset, _, _ := createStsDeployment(ctx, client, namespace, sc, false,
			false, stsReplicas, "", 3, accessMode)

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		allPvcs, err := client.CoreV1().PersistentVolumeClaims(namespace).List(ctx, metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		for _, pvclaim := range allPvcs.Items {
			volumeSnapshot, snapshotContent, snapshotCreated,
				snapshotContentCreated, snapshotId, _, err := createDynamicVolumeSnapshot(ctx, namespace, snapc, volumeSnapshotClass,
				&pvclaim, volHandle, diskSize, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			volumeSnapshotList = append(volumeSnapshotList, volumeSnapshot)
			snapshotIDs = append(snapshotIDs, snapshotId)

			defer func() {
				if snapshotContentCreated {
					err = deleteVolumeSnapshotContent(ctx, snapshotContent, snapc, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}

				if snapshotCreated {
					framework.Logf("Deleting volume snapshot")
					deleteVolumeSnapshotWithPandoraWait(ctx, snapc, namespace, volumeSnapshot.Name, pandoraSyncWaitTime)

					framework.Logf("Wait till the volume snapshot is deleted")
					err = waitForVolumeSnapshotContentToBeDeletedWithPandoraWait(ctx, snapc,
						*volumeSnapshot.Status.BoundVolumeSnapshotContentName, pandoraSyncWaitTime)
					gomega.Expect(err).NotTo(gomega.HaveOccurred())
				}
			}()
		}

		ginkgo.By(fmt.Sprintln("Stopping vsan-health on the vCenter host"))
		isVsanHealthServiceStopped = true
		err = invokeVCenterServiceControl(ctx, stopOperation, vsanhealthServiceName, vcAddress)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow vsan-health to completely shutdown",
			vsanHealthServiceWaitTime))
		time.Sleep(time.Duration(vsanHealthServiceWaitTime) * time.Second)

		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.By("Rename datastore to a new name")
		e2eVSphere.renameDs(ctx, datastoreName+randomStr)

		ginkgo.By(fmt.Sprintln("Starting vsan-health on the vCenter host"))
		startVCServiceWait4VPs(ctx, vcAddress, vsanhealthServiceName, &isVsanHealthServiceStopped)

		ginkgo.By(fmt.Sprintf("Sleeping for %v seconds to allow full sync finish", pandoraSyncWaitTime))
		time.Sleep(time.Duration(pandoraSyncWaitTime) * time.Second)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Bring up vsan health")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		ginkgo.By("Verifying statefulset scale up went fine on statefulset")
		// Scale up replicas of statefulset1 and verify CNS entries for volumes
		scaleUpStsAndVerifyPodMetadata(ctx, client, namespace, statefulset,
			stsReplicas, false, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandle, snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

	})

	/*
		Create CSI snapshot while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Create some PVCs with storageclass created in step 1 and verify they are in bound state.
		3  Verify CNS metadata for all volumes and verify it is placed on correct datastore.
		4  Rename the datastore while creating CSI snapshot from volumes created in step 2.
		5  Verify CSI snapshots are in readyToUse state.
		6  Invoke query API to check snapshots metadata in CNS
		7  Create new volumes on the renamed datastore.
		8  Restore volume from snapshot created in step 5 and verify this operation is a success.
		9  Delete all the workloads created in the test.
	*/
	ginkgo.It("Create CSI snapshot while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		snapshotIDs := []string{}
		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		volumeSnapshotList := []*snapV1.VolumeSnapshot{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Create volume snapshot class")
		volumeSnapshotClass, err := snapc.SnapshotV1().VolumeSnapshotClasses().Create(ctx,
			getVolumeSnapshotClassSpec(snapV1.DeletionPolicy("Delete"), nil), metav1.CreateOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		framework.Logf("Volume snapshot class with name %q created", volumeSnapshotClass.Name)

		ginkgo.By("Rename datastore to a new name while creating volume snapshot in parallel")
		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		ch := make(chan *snapV1.VolumeSnapshot)

		lock := &sync.Mutex{}
		wg.Add(2)
		go createDynamicSnapshotInParallel(ctx, namespace, snapc, pvclaimsList, volumeSnapshotClass.Name, ch, lock, &wg)
		go e2eVSphere.renameDsInParallel(ctx, datastoreName, &wg)
		go func() {
			for v := range ch {
				volumeSnapshotList = append(volumeSnapshotList, v)
			}
		}()
		wg.Wait()
		close(ch)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

		ginkgo.By("Attach a pod to each of the PVCs created earlier")
		ginkgo.By("wait for all pods to be running and verify that the respective pvcs are accessible")
		pods := createMultiplePods(ctx, client, pvclaims2d, true)

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, pods, true)
		}()

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, pods, pvclaims2d)

		for i := 0; i < len(volumeSnapshotList); i++ {
			ginkgo.By("Verify volume restore fromsnapshots created earlier is successful")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshotList[i], diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshotList[i], pandoraSyncWaitTime, volHandle, snapshotIDs[i], true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

		}

		ginkgo.By("Verify volume snapshot is created")
		for i, volumeSnapshot := range volumeSnapshotList {
			volumeSnapshot, err = waitForVolumeSnapshotReadyToUse(*snapc, ctx, namespace, volumeSnapshot.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			if volumeSnapshot.Status.RestoreSize.Cmp(resource.MustParse(diskSize)) != 0 {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "unexpected restore size")
			}

			ginkgo.By("Verify volume snapshot content is created")
			snapshotContent, err := snapc.SnapshotV1().VolumeSnapshotContents().Get(ctx,
				*volumeSnapshot.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			snapshotContent, err = waitForVolumeSnapshotContentReadyToUse(*snapc, ctx, snapshotContent.Name)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			volHandle = pvs[i].Spec.CSI.VolumeHandle
			gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
			framework.Logf("Get volume snapshot ID from snapshot handle")
			_, snapshotId, err := getVolumeSnapshotIdFromSnapshotHandle(ctx, snapshotContent)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Query CNS and check the volume snapshot entry")
			err = waitForCNSSnapshotToBeCreated(volHandle, snapshotId)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())

			ginkgo.By("Verify restore volume from snapshot is successfull")
			verifyVolumeRestoreOperation(ctx, client, namespace, sc, volumeSnapshot, diskSize, true)

			ginkgo.By("Delete dynamic volume snapshot")
			_, _, err = deleteVolumeSnapshot(ctx, snapc, namespace,
				volumeSnapshot, pandoraSyncWaitTime, volHandle, snapshotId, true)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

	})

	/*
		Create workloads while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Create workloads while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while creating volume snapshot in parallel")
		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		pvcChan := make(chan *v1.PersistentVolumeClaim)
		podChan := make(chan *v1.Pod)

		lock := &sync.Mutex{}
		wg.Add(3)
		go createPvcInParallel(ctx, client, namespace, "", sc, pvcChan, lock, &wg, 10)
		go createPodsInParallel(client, namespace, pvclaimsList, ctx, lock, podChan, &wg, len(pvclaimsList))
		go e2eVSphere.renameDsInParallel(ctx, datastoreName, &wg)
		go func() {
			for v := range pvcChan {
				newPvcList = append(newPvcList, v)
			}
		}()
		go func() {
			for v := range podChan {
				newPodList = append(newPodList, v)
			}
		}()
		wg.Wait()
		close(pvcChan)
		close(podChan)

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

	/*
		Expand volumes while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Expand volumes while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while creating volume snapshot in parallel")
		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		podChan := make(chan *v1.Pod)

		lock := &sync.Mutex{}
		wg.Add(3)
		go expandVolumeInParallel(client, pvclaimsList, &wg, "10Gi")
		go createPodsInParallel(client, namespace, pvclaimsList, ctx, lock, podChan, &wg, len(pvclaimsList))
		go e2eVSphere.renameDsInParallel(ctx, datastoreName, &wg)
		go func() {
			for v := range podChan {
				newPodList = append(newPodList, v)
			}
		}()
		wg.Wait()
		close(podChan)

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

	/*
		Expand volumes while renaming datastore
		Steps:
		1  Create a storageclass with with storage policy set as vsan default storage policy.
		2  Rename the datastore while creating some PVCs and statefulsets on that datastore using the storageclass
			created in step 1.
		3  Verify all PVCs come to bound state.
		4  Verify all pods are up and in running state.
		5  Verify CNS metadata for all volumes and verify it is placed on newly renamed datastore.
		6  Create new volumes on the renamed datastore.
		7  Scale up statefulset replicas to 5.
		8  Delete all the workloads created in the test.
	*/
	ginkgo.It("Expand volumes while renaming datastore", ginkgo.Label(p0, block, vanilla), func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pvcCount := 10
		var sc *storagev1.StorageClass
		var err error
		var volHandle string

		pvclaims2d := [][]*v1.PersistentVolumeClaim{}
		newPvcList := []*v1.PersistentVolumeClaim{}
		newPodList := []*v1.Pod{}

		ginkgo.By("Create a storageclass")
		if vanillaCluster {
			scParameters = map[string]string{}
			scParameters[scParamStoragePolicyName] = storagePolicyName
			scSpec := getVSphereStorageClassSpec(defaultNginxStorageClassName, scParameters, nil, "", "", false)
			sc, err = client.StorageV1().StorageClasses().Create(ctx, scSpec, metav1.CreateOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer func() {
				err = client.StorageV1().StorageClasses().Delete(ctx, sc.Name, *metav1.NewDeleteOptions(0))
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}()
		} else {
			sc, err = client.StorageV1().StorageClasses().Get(ctx, storagePolicyName, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		ginkgo.By("Create multiple PVCs")
		pvclaimsList := createMultiplePVCsInParallel(ctx, client, namespace, sc, pvcCount, nil)
		pvclaims2d = append(pvclaims2d, pvclaimsList)

		ginkgo.By("wait for pvcs to be bound")
		pvs, err := fpv.WaitForPVClaimBoundPhase(ctx, client, pvclaimsList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		defer func() {
			for i, pvc := range pvclaimsList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
				ginkgo.By("Waiting for CNS volumes to be deleted")
				volHandle = pvs[i].Spec.CSI.VolumeHandle
				gomega.Expect(volHandle).NotTo(gomega.BeEmpty())
				err = e2eVSphere.waitForCNSVolumeToBeDeleted(volHandle)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		ginkgo.By("Rename datastore to a new name while creating volume snapshot in parallel")
		datastoreName, err := e2eVSphere.fetchDatastoreNameFromDatastoreUrl(ctx, volHandle)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		var wg sync.WaitGroup
		podChan := make(chan *v1.Pod)

		lock := &sync.Mutex{}
		wg.Add(3)
		go expandVolumeInParallel(client, pvclaimsList, &wg, "10Gi")
		go createPodsInParallel(client, namespace, pvclaimsList, ctx, lock, podChan, &wg, len(pvclaimsList))
		go e2eVSphere.renameDsInParallel(ctx, datastoreName, &wg)
		go func() {
			for v := range podChan {
				newPodList = append(newPodList, v)
			}
		}()
		wg.Wait()
		close(podChan)

		defer func() {
			for _, pvc := range newPvcList {
				ginkgo.By("Delete PVCs")
				err = fpv.DeletePersistentVolumeClaim(ctx, client, pvc.Name, namespace)
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
		}()

		defer func() {
			deletePodsAndWaitForVolsToDetach(ctx, client, newPodList, true)
		}()

		ginkgo.By("wait for pvcs to be bound")
		_, err = fpv.WaitForPVClaimBoundPhase(ctx, client, newPvcList, framework.ClaimProvisionTimeout)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("verify that volumes are accessible for all the pods")
		verifyVolMountsInPods(ctx, client, newPodList, pvclaims2d)

		ginkgo.By("Perform volume lifecycle actions after datastore name change")
		volumeLifecycleActions(ctx, client, namespace, sc, "")

	})

})
