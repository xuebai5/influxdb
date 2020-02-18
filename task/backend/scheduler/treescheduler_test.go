package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/btree"
)

type durExecutor time.Duration

func (m durExecutor) Execute(ctx context.Context, id ID, scheduledFor time.Time, runAt time.Time) error {
	if m > 0 {
		time.Sleep(time.Duration(m))
	}
	return nil
}

type durSchedulableService time.Duration

func (m durSchedulableService) UpdateLastScheduled(ctx context.Context, id ID, t time.Time) error {
	if m > 0 {
		time.Sleep(time.Duration(m))
	}
	return nil
}

func newScheduler(executor Executor, checkpointer SchedulableService, opts ...treeSchedulerOptFunc) *TreeScheduler {
	s := &TreeScheduler{
		executor:      executor,
		priorityQueue: btree.New(degreeBtreeScheduled),
		nextTime:      map[ID]int64{},
		onErr:         func(_ context.Context, _ ID, _ time.Time, _ error) {},
		time:          clock.New(),
		done:          make(chan struct{}, 1),
		checkpointer:  checkpointer,
	}

	// apply options
	for i := range opts {
		if err := opts[i](s); err != nil {
			return nil
		}
	}

	if s.workchans == nil {
		s.workchans = make([]chan Item, defaultMaxWorkers)
	}

	s.wg.Add(len(s.workchans))
	for i := 0; i < len(s.workchans); i++ {
		s.workchans[i] = make(chan Item)
		go s.work(context.Background(), s.workchans[i])
	}

	s.sm = NewSchedulerMetrics(s)
	s.when = time.Time{}
	s.timer = s.time.Timer(0)
	s.timer.Stop()

	return s
}

func BenchmarkTreeScheduler(b *testing.B) {
	for _, count := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("%d_tasks_per_worker", count), func(b *testing.B) {
			for _, delay := range []time.Duration{100 * time.Microsecond, 500 * time.Microsecond, 1 * time.Millisecond} {
				b.Run(fmt.Sprintf("%v worker delay", delay), func(b *testing.B) {
					benchN(b, count, delay)
				})
			}
		})
	}
}

func benchN(b *testing.B, n int, delay time.Duration) {
	id := ID(0)
	ls := time.Unix(1000, 0)
	makeSched := func() *mockSchedulable {
		nid := id
		id++
		return &mockSchedulable{
			id:            nid,
			schedule:      mustCron("@every 1s"),
			lastScheduled: ls,
		}
	}

	clk := clock.NewMock()
	clk.Set(ls)
	clk.Add(1 * time.Second)
	exec := durExecutor(delay)
	shed := durSchedulableService(0)
	s := newScheduler(exec, shed, WithTime(clk), WithMaxConcurrentWorkers(1))

	for i := 0; i < n; i++ {
		_ = s.Schedule(makeSched())
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for s.processSchedule() {
		}

		if s.timer != nil {
			s.timer.Stop()
		}
		clk.Set(s.when)
	}
}
