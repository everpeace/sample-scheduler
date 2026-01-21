package scheduler

// PriorityQueue implements a priority queue for PodGroupOrPurePod based on their priority.
// IMPORTANT: This does not thread-safe. Caller must ensure only one goroutine accesses it at a time.
type PriorityQueue []*PriorityQueueItem

// PriorityQueueItem represents an item in the priority queue.
type PriorityQueueItem struct {
	workload *PendingWorkload
	index    int // The index of the item in the heap.
}

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].workload.priority > pq[j].workload.priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x any) {
	n := len(*pq)
	item := x.(*PriorityQueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
