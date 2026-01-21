package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	informers "k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"

	listerscorev1 "k8s.io/client-go/listers/core/v1"
)

type Scheduler struct {
	clientset    clientset.Interface
	resyncPeriod time.Duration

	// initilized in Start()
	factory     informers.SharedInformerFactory
	broadcaster events.EventBroadcaster
	podLister   listerscorev1.PodLister
	nodeLister  listerscorev1.NodeLister

	// priorityQueue of PendingWorkloads
	queue     PriorityQueue
	queueLock sync.Mutex

	// scheduling workloads in progress
	schedulingWorkloadsLock sync.Mutex
	schedulingWorkloads     map[string]*SchedulingWorkload

	// scheduled workloads
	scheduledWorkloadsLock sync.Mutex
	scheduledWorkloads     map[string]*ScheduledWorkload
}

func NewScheduler(
	clientset clientset.Interface,
	resyncPeriod time.Duration,
) *Scheduler {
	return &Scheduler{
		clientset:           clientset,
		resyncPeriod:        resyncPeriod,
		schedulingWorkloads: make(map[string]*SchedulingWorkload),
		scheduledWorkloads:  make(map[string]*ScheduledWorkload),
	}
}

func (s *Scheduler) scheduleOne(ctx context.Context) {
	pendingWorkload := s.NextWorkload()
	if pendingWorkload == nil {
		// no workload to schedule
		return
	}

	// check node availability for the workload
	availableNodes := s.availableNodes()
	if len(availableNodes) < int(pendingWorkload.size) {
		// not enough available nodes
		klog.InfoS("not enough available nodes for workload", "workload", pendingWorkload.name, "required", pendingWorkload.size, "available", len(availableNodes))
		klog.Info("trying to the preemption (deleting lower priority workloads)...")

		// FIXME: implement preemption logic here

		// after preemption, re-enqueue the workload
		s.EnqueueWorkload(pendingWorkload)
		return
	}

	// Binding workloads to the nodes
	bindingNodeNames := []string{}
	for _, n := range availableNodes[:pendingWorkload.ActualPendingPodSize()] {
		bindingNodeNames = append(bindingNodeNames, n.Name)
	}
	schedulingWorkload := &SchedulingWorkload{
		Workload:            pendingWorkload.Workload,
		schedulingNodeNames: bindingNodeNames,
	}
	s.addOrUpdateSchedulingWorkloadCache(schedulingWorkload)
	klog.InfoS("binding workload to nodes", "workload", pendingWorkload.name, "nodes", bindingNodeNames)
	go func() {
		err := s.BindToNodes(ctx, schedulingWorkload, bindingNodeNames)

		// FIXME: how to handle partially bound pod group???
		if err != nil {
			klog.ErrorS(err, "failed to bind workload to nodes", "workload", pendingWorkload.name)
			return
		}
		s.schedulingWorkloadsLock.Lock()
		s.scheduledWorkloadsLock.Lock()
		defer s.schedulingWorkloadsLock.Unlock()
		defer s.scheduledWorkloadsLock.Unlock()

		s.removeSchedulingWorkloadCache(schedulingWorkload)
		scheduledWorkload := &ScheduledWorkload{
			Workload:           pendingWorkload.Workload,
			scheduledNodeNames: bindingNodeNames,
		}
		s.removeSchedulingWorkloadCache(schedulingWorkload)
		s.addOrUpdateScheduledWorkloadCache(scheduledWorkload)
		klog.InfoS("successfully bound workload to nodes", "workload", pendingWorkload.name, "nodes", bindingNodeNames)
	}()
}

func (s *Scheduler) BindToNodes(ctx context.Context, w *SchedulingWorkload, nodeNames []string) error {
	if w.ActualPendingPodSize() > len(nodeNames) {
		return fmt.Errorf("not enough node names to bind: required %d, provided %d", len(w.pods), len(nodeNames))
	}

	for i, p := range w.pods {
		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID},
			Target:     corev1.ObjectReference{Kind: "Node", Name: nodeNames[i]},
		}
		if err := s.clientset.CoreV1().Pods(p.Namespace).Bind(ctx, binding, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to bind pod %s/%s to node %s: %v", p.Namespace, p.Name, nodeNames[i], err)
		}
	}
	return nil
}

func (s *Scheduler) addOrUpdateSchedulingWorkloadCache(w *SchedulingWorkload) {
	s.schedulingWorkloadsLock.Lock()
	defer s.schedulingWorkloadsLock.Unlock()
	s.schedulingWorkloads[w.namespace+"/"+w.name] = w
}

func (s *Scheduler) removeSchedulingWorkloadCache(w *SchedulingWorkload) {
	s.schedulingWorkloadsLock.Lock()
	defer s.schedulingWorkloadsLock.Unlock()
	delete(s.schedulingWorkloads, w.namespace+"/"+w.name)
}

func (s *Scheduler) addOrUpdateScheduledWorkloadCache(w *ScheduledWorkload) {
	s.scheduledWorkloadsLock.Lock()
	defer s.scheduledWorkloadsLock.Unlock()
	s.scheduledWorkloads[w.namespace+"/"+w.name] = w
}

func (s *Scheduler) removeScheduledWorkloadCache(w *ScheduledWorkload) {
	s.scheduledWorkloadsLock.Lock()
	defer s.scheduledWorkloadsLock.Unlock()
	delete(s.scheduledWorkloads, w.namespace+"/"+w.name)
}

func (s *Scheduler) NextWorkload() *PendingWorkload {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()

	if s.queue.Len() == 0 {
		return nil
	}
	popedItem := s.queue.Pop()

	return popedItem.(*PriorityQueueItem).workload
}

func (s *Scheduler) EnqueueWorkload(workload *PendingWorkload) {
	s.queueLock.Lock()
	defer s.queueLock.Unlock()
	item := &PriorityQueueItem{
		workload: workload,
	}
	s.queue.Push(item)
}

func (s *Scheduler) availableNodes() []*corev1.Node {
	s.schedulingWorkloadsLock.Lock()
	defer s.schedulingWorkloadsLock.Unlock()
	s.scheduledWorkloadsLock.Lock()
	defer s.scheduledWorkloadsLock.Unlock()

	scheduilngOrScheduledNodes := sets.NewString()
	for _, w := range s.schedulingWorkloads {
		for _, nodeName := range w.schedulingNodeNames {
			scheduilngOrScheduledNodes.Insert(nodeName)
		}
	}
	for _, w := range s.scheduledWorkloads {
		for _, nodeName := range w.scheduledNodeNames {
			scheduilngOrScheduledNodes.Insert(nodeName)
		}
	}

	nodes, _ := s.nodeLister.List(labels.Everything())
	availableNodes := []*corev1.Node{}
	for _, node := range nodes {
		if !scheduilngOrScheduledNodes.Has(node.Name) {
			availableNodes = append(availableNodes, node)
		}
	}

	return availableNodes
}

// isNodeAvailable, check node has already ocupied a pod by this scheduler
// FIXME: should introduce node cache in the scheduler to avoid listing pods every time
func (s *Scheduler) isNodeAvailable(node *corev1.Node) bool {
	pods, _ := s.podLister.Pods("").List(labels.Everything())
	for _, pod := range pods {
		if pod.Spec.NodeName != node.Name && s.isInterestedIn(pod) {
			return false
		}
	}
	return true
}

func (s *Scheduler) Start(ctx context.Context) error {
	// initialize informers and listers
	s.factory = informers.NewSharedInformerFactory(s.clientset, s.resyncPeriod)
	s.broadcaster = events.NewBroadcaster(&events.EventSinkImpl{
		Interface: s.clientset.EventsV1(),
	})
	s.podLister = s.factory.Core().V1().Pods().Lister()
	s.nodeLister = s.factory.Core().V1().Nodes().Lister()

	// register event handlers
	_, err := s.factory.Core().V1().Pods().Informer().AddEventHandler(s)
	if err != nil {
		return err
	}
	_, err = s.factory.Core().V1().Nodes().Informer().AddEventHandler(s)
	if err != nil {
		return err
	}

	// initialize priority queue
	s.queue = make(PriorityQueue, 0)

	// start informers
	s.factory.Start(ctx.Done())
	s.factory.WaitForCacheSync(ctx.Done())
	go s.Run(ctx)

	<-ctx.Done()
	return ctx.Err()
}

func (s *Scheduler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, s.scheduleOne, 0)
}

// Event Handlers
//
// OnAdd implements [cache.ResourceEventHandler].
func (s *Scheduler) OnAdd(obj interface{}, isInInitialList bool) {
	if pod, ok := obj.(*corev1.Pod); ok {
		s.OnPodAdd(pod, isInInitialList)
	}
	if node, ok := obj.(*corev1.Node); ok {
		s.OnNodeAdd(node, isInInitialList)
	}
}

// OnDelete implements [cache.ResourceEventHandler].
func (s *Scheduler) OnDelete(obj interface{}) {
	if pod, ok := obj.(*corev1.Pod); ok {
		s.OnPodDelete(pod)
	}
	if node, ok := obj.(*corev1.Node); ok {
		s.OnNodeDelete(node)
	}
}

// OnUpdate implements [cache.ResourceEventHandler].
func (s *Scheduler) OnUpdate(oldObj interface{}, newObj interface{}) {
	oldPod, oldOk := oldObj.(*corev1.Pod)
	newPod, newOk := newObj.(*corev1.Pod)
	if oldOk && newOk {
		s.OnPodUpdate(oldPod, newPod)
	}

	oldNode, oldOk := oldObj.(*corev1.Node)
	newNode, newOk := newObj.(*corev1.Node)
	if oldOk && newOk {
		s.OnNodeUpdate(oldNode, newNode)
	}
}

func (s *Scheduler) OnPodAdd(pod *corev1.Pod, isInInitialList bool) {
	if !s.isInterestedIn(pod) {
		return
	}

	podGroupName := pod.Annotations[podGroupNameAnnotationKey]

	//  if pod is pure pod, just enqueue it
	if podGroupName == "" && pod.Spec.NodeName == "" {
		pendingWorkload, err := NewPendingWorkload([]*corev1.Pod{pod})
		if err != nil {
			klog.ErrorS(err, "failed to create PendingWorkload for pod", "pod", pod.Name)
			return
		}
		s.EnqueueWorkload(pendingWorkload)
		return
	}

	// pod belongs to a pod group
	podsInGroup := []*corev1.Pod{pod}
	pods, err := s.podLister.Pods(pod.Namespace).List(labels.Everything())
	if err != nil {
		klog.ErrorS(err, "failed to list pods in namespace", "namespace", pod.Namespace)
		return
	}
	for _, p := range pods {
		if !s.isInterestedIn(p) {
			continue
		}

		if p.Spec.NodeName == "" && p.Annotations[podGroupNameAnnotationKey] == podGroupName {
			podsInGroup = append(podsInGroup, p)
		}
	}

	isAllPending := true
	for _, p := range podsInGroup {
		if p.Spec.NodeName != "" {
			isAllPending = false
			break
		}
	}

	if isAllPending {
		// TODO: check this workload is scheduling or scheduled.

		pendingWorkload, err := NewPendingWorkload(podsInGroup)
		if err != nil {
			klog.ErrorS(err, "failed to create PendingWorkload for pod group", "podGroup", podGroupName)
			return
		}
		if pendingWorkload != nil {
			// NewPendingWorkload can return nil when size is not reached yet
			s.EnqueueWorkload(pendingWorkload)
		}
		return
	}

	// NOTE: This spec is subject to discuss
	// if partially scheduled, queue them as a single pods to filling the gang immediately
	for _, p := range podsInGroup {
		if p.Spec.NodeName == "" {
			// remove podGroupName annotation to treat as a single pod
			delete(p.Annotations, podGroupNameAnnotationKey)
			pendingWorkload, err := NewPendingWorkload([]*corev1.Pod{p})
			if err != nil {
				klog.ErrorS(err, "failed to create PendingWorkload for pod", "pod", p.Name)
				continue
			}
			if pendingWorkload != nil {
				s.EnqueueWorkload(pendingWorkload)
			}
		}
	}
}

func (s *Scheduler) OnPodDelete(pod *corev1.Pod) {
	// TODO:
	// - if pod does not belong to any PodGroup, just ignore
	// - if pod belongs to a PodGroup,
	//   - if the workload is scheduled(i.e. in ScheduledWorklaodCache)
	//     - then delete all pods and remove the ScheduledWorkload from the cache
	//   - if the workload is scheduling(i.e. in SchedulingWorklaodCache)
	//     - then delete all pods and remove the SchedulingWorkload from the cache
}

func (s *Scheduler) OnPodUpdate(oldPod *corev1.Pod, newPod *corev1.Pod) {
	// TODO:
	// - if pod does not belong to any PodGroup
	//   - if pod is deleting, remove the workload from scheduledWorkloadCache if exists
	// - if pod belongs to a PodGroup,
	//   - if pod is deleting, remove the workload from scheduledWorkloadCache or schedulingWorkloadCache if exists
}

func (s *Scheduler) OnNodeAdd(node *corev1.Node, isInInitialList bool) {
	// TODO:
	//   I ran-out of time... I needed to more time to consider.
}

func (s *Scheduler) OnNodeDelete(node *corev1.Node) {
	// TODO:
	//   I ran-out of time... I needed to more time to consider.
}

func (s *Scheduler) OnNodeUpdate(oldNode *corev1.Node, newNode *corev1.Node) {
	klog.Info("onNodeUpdate: ", oldNode.Name)
}

func (s *Scheduler) isInterestedIn(pod *corev1.Pod) bool {
	return pod.Spec.SchedulerName == schedulerName
}
