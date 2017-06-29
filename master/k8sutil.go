package master

import (
	"fmt"
	"log"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/pkg/api/v1"
	v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

func (master *Master) newReplicaSet(name string, command []string, image string, replicas int32) v1beta1.ReplicaSet {
	var resourceRequirements v1.ResourceRequirements
	if master.JobDesc.CPULimit != "" {
		cpulimt, err := resource.ParseQuantity(master.JobDesc.CPULimit)
		if err != nil {
			log.Fatalf("Can't parse cpulimit \"%s\": %v", master.JobDesc.CPULimit, err)
		}
		resourceRequirements = v1.ResourceRequirements{
			Requests: v1.ResourceList{
				"cpu": cpulimt,
			},
			Limits: v1.ResourceList{
				"cpu": cpulimt,
			},
		}
	}
	podTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr-worker",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:    "kmr-worker",
					Command: command,
					Image:   image,
					Env: []v1.EnvVar{
						v1.EnvVar{
							Name:  "KMR_MASTER_ADDRESS",
							Value: fmt.Sprintf("%s%s", master.JobName, master.port),
						},
					},
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "cephfs",
							MountPath: "/cephfs",
						},
					},
					Resources: resourceRequirements,
				},
			},
			Volumes: []v1.Volume{
				v1.Volume{
					Name: "cephfs",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/mnt/cephfs",
						},
					},
				},
			},
		},
	}
	rsSpec := v1beta1.ReplicaSetSpec{
		Replicas: &replicas,
		Template: podTemplate,
	}
	replicaSet := v1beta1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				"kmr.jobname": name,
				"app":         "kmr",
			},
		},
		Spec: rsSpec,
	}
	return replicaSet
}

func (master *Master) createReplicaSet(replicaSet *v1beta1.ReplicaSet) (*v1beta1.ReplicaSet, error) {
	return master.k8sclient.ExtensionsV1beta1().
		ReplicaSets(master.namespace).Create(replicaSet)
}

func (master *Master) replicaSetName(phase string, jobName string) string {
	return fmt.Sprintf("%s-%s", jobName, phase)
}

func (master *Master) startWorker(phase string) error {
	var rs v1beta1.ReplicaSet
	switch phase {
	case mapPhase:
		rs = master.newReplicaSet(master.replicaSetName(phase, master.JobName),
			master.JobDesc.Map.Command, master.JobDesc.Map.Image, int32(master.JobDesc.Map.NWorker))
	case reducePhase:
		rs = master.newReplicaSet(master.replicaSetName(phase, master.JobName),
			master.JobDesc.Reduce.Command, master.JobDesc.Reduce.Image, int32(master.JobDesc.Reduce.NWorker))
	case mapreducePhase:
		rs = master.newReplicaSet(master.replicaSetName(phase, master.JobName),
			master.JobDesc.Command, master.JobDesc.Image, int32(master.JobDesc.NWorker))
	}
	_, err := master.createReplicaSet(&rs)
	return err
}
func (master *Master) killWorkers(phase string) error {
	falseVal := false

	return master.k8sclient.ExtensionsV1beta1().ReplicaSets(master.namespace).
		Delete(master.replicaSetName(phase, master.JobName), &metav1.DeleteOptions{
			OrphanDependents: &falseVal,
		})
}
