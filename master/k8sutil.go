package master

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/client-go/pkg/api/v1"
	v1beta1 "k8s.io/client-go/pkg/apis/extensions/v1beta1"
)

func (master *Master) generateReplicaSet(command []string, image string, replicas int32) (v1beta1.ReplicaSet, error) {
	podTemplate := v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: master.JobName,
			Labels: map[string]string{
				"kmr.jobname": master.JobName,
				"app":         "kmr-worker",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Name:    "kmr-worker",
					Command: command,
					Image:   image,
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
			Name: master.JobName,
			Labels: map[string]string{
				"kmr.jobname": master.JobName,
				"app":         "kmr",
			},
		},
		Spec: rsSpec,
	}
	return replicaSet, nil
}
