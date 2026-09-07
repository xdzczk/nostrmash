package jobs

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobNotifyChannel is the Postgres LISTEN/NOTIFY channel used to wake claim
// loops when a job becomes runnable. A missed notify is not fatal: claim
// loops still fall back to pollInterval.
const JobNotifyChannel = "nostrmash_jobs"

type notifyExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// notifyJobsAvailable signals claim loops that a job in workerPool is
// (or will soon be) claimable. Failures are ignored: the row is already in
// jobs, and the poll fallback will pick it up.
func notifyJobsAvailable(ctx context.Context, exec notifyExec, workerPool string) {
	if exec == nil {
		return
	}
	_, _ = exec.Exec(ctx, `SELECT pg_notify($1, $2)`, JobNotifyChannel, workerPool)
}

// WaitForWork blocks until a job-notify arrives or max elapses. The first
// call starts a dedicated LISTEN connection (one per Queue) that broadcasts
// to every claim loop in the process. max is the safety-net poll interval:
// if NOTIFY is missed or LISTEN cannot start, we still retry on that cadence.
func (q *Queue) WaitForWork(ctx context.Context, max time.Duration) {
	if q == nil || q.pool == nil {
		sleepWait(ctx, max)
		return
	}
	if max <= 0 {
		max = time.Second
	}
	listener := q.ensureListener(ctx)
	if listener == nil {
		sleepWait(ctx, max)
		return
	}
	listener.Wait(ctx, max)
}

func (q *Queue) ensureListener(ctx context.Context) *notifyListener {
	q.listenerOnce.Do(func() {
		listener, err := startNotifyListener(ctx, q.pool)
		if err != nil {
			return
		}
		q.listener = listener
	})
	return q.listener
}

type notifyListener struct {
	conn *pgxpool.Conn
	mu   sync.Mutex
	gen  chan struct{}
}

func startNotifyListener(ctx context.Context, pool *pgxpool.Pool) (*notifyListener, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `LISTEN `+JobNotifyChannel); err != nil {
		conn.Release()
		return nil, err
	}
	l := &notifyListener{
		conn: conn,
		gen:  make(chan struct{}),
	}
	go l.loop(ctx)
	return l, nil
}

func (l *notifyListener) loop(ctx context.Context) {
	defer func() {
		_, _ = l.conn.Exec(context.Background(), `UNLISTEN `+JobNotifyChannel)
		l.conn.Release()
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := l.conn.Conn().WaitForNotification(ctx); err != nil {
			return
		}
		l.broadcast()
	}
}

func (l *notifyListener) broadcast() {
	l.mu.Lock()
	close(l.gen)
	l.gen = make(chan struct{})
	l.mu.Unlock()
}

func (l *notifyListener) Wait(ctx context.Context, max time.Duration) {
	l.mu.Lock()
	ch := l.gen
	l.mu.Unlock()
	timer := time.NewTimer(max)
	defer timer.Stop()
	select {
	case <-ch:
	case <-timer.C:
	case <-ctx.Done():
	}
}

func sleepWait(ctx context.Context, max time.Duration) {
	timer := time.NewTimer(max)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
