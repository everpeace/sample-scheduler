package scheduler

import "testing"

func TestPriorityQueue(t *testing.T) {
	queue := &PriorityQueue{}

	workload1 := &PendingWorkload{priority: 1}
	workload2 := &PendingWorkload{priority: 2}

	queue.Push(&PriorityQueueItem{workload: workload1})
	queue.Push(&PriorityQueueItem{workload: workload2})

	item := queue.Pop().(*PriorityQueueItem)
	if item.workload.priority != 2 {
		t.Errorf("expected priority 2, got %d", item.workload.priority)
	}

	item = queue.Pop().(*PriorityQueueItem)
	if item.workload.priority != 1 {
		t.Errorf("expected priority 1, got %d", item.workload.priority)
	}
}
