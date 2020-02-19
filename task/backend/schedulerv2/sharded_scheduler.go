package schedulerv2

import (
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash"
)

type SchedulerState int

const (
	SchedulerStateUnknown SchedulerState = iota
	SchedulerStateProcessing
	SchedulerStateReady
	SchedulerStateStopped
)

type ShardedScheduler struct {
	schedulers []Scheduler
	wg         sync.WaitGroup
}

// NewShardedScheduler creates a new ShardedScheduler. To ensure even
// distribution, a power of two should be selected for the shard count.
func NewShardedScheduler(count int, createScheduler func() Scheduler) (*ShardedScheduler, error) {
	schedulers := make([]Scheduler, count)

	for i := range schedulers {
		schedulers[i] = createScheduler()
	}

	s := &ShardedScheduler{
		schedulers: schedulers,
	}

	return s, nil
}

func (s *ShardedScheduler) Schedule(task Schedulable) error {
	return s.schedulers[s.hash(task.ID())].Schedule(task)
}

func (s *ShardedScheduler) Release(taskID ID) error {
	return s.schedulers[s.hash(taskID)].Release(taskID)
}

func (s *ShardedScheduler) hash(taskID ID) uint64 {
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], uint64(taskID))
	return xxhash.Sum64(buf[:]) % uint64(len(s.schedulers)) // we just hash so that the number is uniformly distributed
}

func (s *ShardedScheduler) Run() error {
	s.wg.Add(len(s.schedulers))
	for _, shard := range s.schedulers {
		go func(shard Scheduler) {
			shard.Run()
			s.wg.Done()
		}(shard)
	}
	s.wg.Wait()
	return nil
}

func (s *ShardedScheduler) Stop() error {
	for _, shard := range s.schedulers {
		shard.Stop()
	}
	s.wg.Wait()
	return nil
}

func (s *ShardedScheduler) State() SchedulerState {
	return SchedulerStateStopped
}
