package master

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/pkg/api/v1"
	v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

func (master *Master) generateReplicaSet(name string, command []string, image string, replicas int32) v1beta1.ReplicaSet {
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
					VolumeMounts: []v1.VolumeMount{
						v1.VolumeMount{
							Name:      "cephfs",
							MountPath: "/cephfs",
						},
					},
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
		rs = master.generateReplicaSet(master.replicaSetName(phase, master.JobName),
			master.JobDesc.Map.commad, master.JobDesc.Map.Image, int32(master.JobDesc.Map.NWorker))
	case reducePhase:
		rs = master.generateReplicaSet(master.replicaSetName(phase, master.JobName),
			master.JobDesc.Reduce.commad, master.JobDesc.Map.Image, int32(master.JobDesc.Map.NWorker))
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
