package scheduler

import (
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

type Workload struct {
	pods []*corev1.Pod

	// namespace is the namespace of this workload
	namespace string

	// name represents pod group name if this is a PodGroup, or pod name if this is a pure Pod.
	name string

	// size indicates the number of pods in this workload
	size int32

	// priority indicates the priority of this workload
	priority int32
}

func (w *Workload) ActualPendingPodSize() int {
	return len(w.pods)
}

type SchedulingWorkload struct {
	Workload
	schedulingNodeNames []string
}

type ScheduledWorkload struct {
	Workload
	scheduledNodeNames []string
}

// PendingWorkload represents pending PodGroup or single Pod as a scheduling unit.
type PendingWorkload struct {
	Workload
}

func NewPendingWorkload(pods []*corev1.Pod) (*PendingWorkload, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("pods slice is empty")
	}

	// FIXME:
	//   validate all pods belogs to the same pgNamespace
	//   validate all pods belong to the same PodGroup if any
	//   validate all pods have the same priority
	//   validate all pods are not already scheduled
	pgNamespace := pods[0].Namespace
	pgName := pods[0].Name
	pgSize := int32(1)
	if podGroupName := pods[0].Annotations[podGroupNameAnnotationKey]; podGroupName != "" {
		pgName = podGroupName
	}
	if podGroupSize := pods[0].Annotations[podGroupSizeAnnotationKey]; podGroupSize != "" {
		var err error
		pgSizeIntUnsized, err := strconv.Atoi(podGroupSize)
		if err != nil {
			return nil, fmt.Errorf("invalid PodGroup size annotation: %v", err)
		}
		pgSize = int32(pgSizeIntUnsized)
	}

	// if pods are less than required size, return nil
	if int32(len(pods)) < pgSize {
		klog.InfoS("NewPendingWorkload: not enough pods for PodGroup ", "namespace", pgNamespace, "podGroup", pgName, "required", pgSize, "actual", len(pods))
		return nil, nil
	}

	return &PendingWorkload{
		Workload: Workload{
			pods:      pods,
			namespace: pgNamespace,
			name:      pgName,
			priority:  *pods[0].Spec.Priority,
			size:      pgSize,
		},
	}, nil
}

func (w *PendingWorkload) IsReadyToSchedule() bool {
	return w.size == int32(len(w.pods))
}
