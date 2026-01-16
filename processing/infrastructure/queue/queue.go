package queue

import (
	"container/list"
)

type Queue struct {
	data *list.List
}

func New() *Queue {
	return &Queue{data: list.New()}
}

func (q *Queue) PushBack(value any) {
	q.data.PushBack(value)
}

func (q *Queue) PopFront() any {
	front := q.data.Front()
	q.data.Remove(front)
	return front.Value
}

func (q *Queue) IsEmpty() bool {
	return q.data.Len() == 0
}

func (q *Queue) Len() int {
	return q.data.Len()
}
