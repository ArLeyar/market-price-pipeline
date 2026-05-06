package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/arleyar/market-price-pipeline/internal/domain"
)

type fakeDB struct {
	calls int32
	err   error
}

func (f *fakeDB) Save(ctx context.Context, _ domain.Price) error {
	atomic.AddInt32(&f.calls, 1)
	return f.err
}

type fakeCache struct {
	calls int32
	err   error
}

func (f *fakeCache) SetLatest(ctx context.Context, _ domain.Price) error {
	atomic.AddInt32(&f.calls, 1)
	return f.err
}

type fakeEvent struct {
	calls int32
	err   error
}

func (f *fakeEvent) Publish(ctx context.Context, _ domain.Price) error {
	atomic.AddInt32(&f.calls, 1)
	return f.err
}

func sampleTick() domain.Price {
	return domain.Price{
		Exchange: "binance",
		Symbol:   "BTCUSDT",
		TS:       time.Now().UTC(),
		Price:    decimal.NewFromInt(60000),
	}
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestHandleTick(t *testing.T) {
	dbErr := errors.New("db boom")
	cacheErr := errors.New("redis boom")
	eventErr := errors.New("kafka boom")

	cases := []struct {
		name              string
		dbErr, cacheErr   error
		eventErr          error
		wantErr           bool
		wantCacheCalled   bool
		wantEventCalled   bool
	}{
		{name: "all sinks succeed", wantCacheCalled: true, wantEventCalled: true},
		{name: "db failure aborts tick", dbErr: dbErr, wantErr: true},
		{name: "cache failure ignored", cacheErr: cacheErr, wantCacheCalled: true, wantEventCalled: true},
		{name: "event failure ignored", eventErr: eventErr, wantCacheCalled: true, wantEventCalled: true},
		{name: "cache and event both fail, still ok", cacheErr: cacheErr, eventErr: eventErr, wantCacheCalled: true, wantEventCalled: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := &fakeDB{err: tc.dbErr}
			cache := &fakeCache{err: tc.cacheErr}
			event := &fakeEvent{err: tc.eventErr}
			p := New(db, cache, event, quietLogger())

			err := p.HandleTick(context.Background(), sampleTick())
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := atomic.LoadInt32(&db.calls); got != 1 {
				t.Errorf("db.Save calls: got %d, want 1", got)
			}
			cacheCalls := atomic.LoadInt32(&cache.calls)
			if tc.wantCacheCalled && cacheCalls != 1 {
				t.Errorf("cache.SetLatest calls: got %d, want 1", cacheCalls)
			}
			if !tc.wantCacheCalled && cacheCalls != 0 {
				t.Errorf("cache.SetLatest unexpectedly called %d times", cacheCalls)
			}
			eventCalls := atomic.LoadInt32(&event.calls)
			if tc.wantEventCalled && eventCalls != 1 {
				t.Errorf("event.Publish calls: got %d, want 1", eventCalls)
			}
			if !tc.wantEventCalled && eventCalls != 0 {
				t.Errorf("event.Publish unexpectedly called %d times", eventCalls)
			}
		})
	}
}

// blockingDB sleeps until ctx cancels, simulating a wedged dependency.
type blockingDB struct{}

func (blockingDB) Save(ctx context.Context, _ domain.Price) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestHandleTick_PerSinkTimeout(t *testing.T) {
	// Even with a parent ctx that has no deadline, the per-sink timeout must
	// kick in and unblock HandleTick within seconds.
	p := New(blockingDB{}, &fakeCache{}, &fakeEvent{}, quietLogger())

	done := make(chan error, 1)
	go func() { done <- p.HandleTick(context.Background(), sampleTick()) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error from blocking DB, got nil")
		}
	case <-time.After(dbTimeout + 2*time.Second):
		t.Fatal("HandleTick did not return within dbTimeout+slack — per-sink timeout not enforced")
	}
}
