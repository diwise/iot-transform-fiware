package projection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func withTestTracer(t *testing.T) (*tracetest.SpanRecorder, func()) {
	t.Helper()
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return recorder, func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	}
}

func TestProjectionBrokerFailureThenReplay(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	mustApply(t, s, setChange("tenant-a", "from-1", "to-2", 2), nil, planner.plan())

	// DB-framgång följd av brokerfel: första leveransen lyckas, andra
	// felar, tredje lyckas — slutförda bevaras i rapporten.
	calls := atomic.Int32{}
	broker := func(ctx context.Context, item PendingItem) error {
		if calls.Add(1) == 2 {
			return errors.New("broker 503")
		}
		return nil
	}
	completed, failed, err := s.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, broker)
	if err != nil {
		t.Fatal(err)
	}
	if completed != 1 || failed != 1 {
		t.Fatalf("completed=%d failed=%d, want 1/1 (partial preserved)", completed, failed)
	}
	// Replay: felet är läkt, återstående arbete levereras.
	completed, failed, err = s.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, func(context.Context, PendingItem) error { return nil })
	if err != nil || completed != 1 || failed != 0 {
		t.Fatalf("replay completed=%d failed=%d err=%v", completed, failed, err)
	}
	count, _ := s.PendingCount(t.Context(), "tenant-a")
	if count != 0 {
		t.Fatalf("pending=%d, want 0 after replay", count)
	}
}

func TestProjectionRestartWithoutLostWork(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())
	s.Close()

	// Omstart: nytt lager mot samma DB ser både projektion och väntande arbete.
	reopened := pgTestStoreNoTruncate(t)
	if got := outgoingToIDs(t, reopened, "tenant-a", "contains", "from-1"); len(got) != 1 {
		t.Fatalf("projection lost on restart: %v", got)
	}
	count, err := reopened.PendingCount(t.Context(), "tenant-a")
	if err != nil || count != 1 {
		t.Fatalf("pending lost on restart: %d %v", count, err)
	}
	delivered := atomic.Int32{}
	completed, failed, err := reopened.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, func(context.Context, PendingItem) error {
		delivered.Add(1)
		return nil
	})
	if err != nil || completed != 1 || failed != 0 || delivered.Load() != 1 {
		t.Fatalf("drain after restart: %d/%d delivered=%d err=%v", completed, failed, delivered.Load(), err)
	}
}

func TestProjectionTraceSurvivesRestart(t *testing.T) {
	recorder, done := withTestTracer(t)
	defer done()

	s := pgTestStore(t)
	planner := &recordingPlanner{}

	// Transportens trace fångas och lagras separat från payload.
	ctx, root := otel.Tracer("test").Start(t.Context(), "transport-parent")
	trace := captureTrace(ctx)
	if len(trace) == 0 {
		t.Fatal("no carrier captured")
	}
	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), trace, planner.plan())
	root.End()
	s.Close()

	reopened := pgTestStoreNoTruncate(t)
	items, err := reopened.ClaimPending(t.Context(), "tenant-a", 10, time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("claim=%d err=%v", len(items), err)
	}
	if len(items[0].Trace) == 0 {
		t.Fatal("stored trace lost")
	}
	for _, item := range items {
		if err := reopened.MarkDone(t.Context(), item.ID); err != nil {
			t.Fatal(err)
		}
	}

	// Dränering länkar till lagrad context (aldrig tyst ersatt ursprung).
	mustApply(t, reopened, setChange("tenant-a", "from-1", "to-2", 2), trace, planner.plan())
	_, _, err = reopened.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, func(context.Context, PendingItem) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	var wantTraceID string
	for _, span := range recorder.Ended() {
		if span.Name() == "transport-parent" {
			wantTraceID = span.SpanContext().TraceID().String()
		}
	}
	for _, span := range recorder.Ended() {
		if span.Name() != "projection-deliver" {
			continue
		}
		for _, link := range span.Links() {
			if link.SpanContext.TraceID().String() == wantTraceID {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("no deliver span links to stored trace %s", wantTraceID)
	}
}

func TestProjectionClaimDistributesAcrossWorkers(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	for i := range 20 {
		c := setChange("tenant-a", "from-1", fmt.Sprintf("to-%d", i), int64(i+1))
		mustApply(t, s, c, nil, planner.plan())
	}
	// 20 poster; två samtidiga claims får disjunkta mängder (SKIP LOCKED).
	var wg sync.WaitGroup
	claimed := make([][]PendingItem, 2)
	errs := make([]error, 2)
	for w := range 2 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			items, err := s.ClaimPending(t.Context(), "tenant-a", 20, time.Minute)
			claimed[w], errs[w] = items, err
		}(w)
	}
	wg.Wait()
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("claims: %v %v", errs[0], errs[1])
	}
	seen := map[string]bool{}
	for _, batch := range claimed {
		for _, item := range batch {
			if seen[item.ID] {
				t.Fatalf("item %s claimed twice", item.ID)
			}
			seen[item.ID] = true
		}
	}
	if len(seen) != 20 {
		t.Fatalf("claimed=%d, want 20 disjoint", len(seen))
	}
}

func TestProjectionMarkDoneNotFound(t *testing.T) {
	s := pgTestStore(t)
	if err := s.MarkDone(t.Context(), "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mark done missing: %v", err)
	}
	if err := s.MarkFailed(t.Context(), "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mark failed missing: %v", err)
	}
	if _, err := s.ClaimPending(t.Context(), "", 0, time.Minute); !errors.Is(err, ErrInvalid) {
		t.Fatalf("claim limit 0: %v", err)
	}
	if _, _, err := s.DrainOnce(t.Context(), "", 10, time.Minute, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("drain without broker: %v", err)
	}
}

func TestProjectionDeleteEndpoint(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "mid", "leaf", 1), nil, planner.plan())
	rootLink := setChange("tenant-a", "root", "mid", 1)
	rootLink.RelationID = "v1/tenant-a/root/relations/contains/1/set"
	mustApply(t, s, rootLink, nil, planner.plan())
	other := setChange("tenant-a", "root", "other", 2)
	other.RelationID = "v1/tenant-a/root/relations/contains/2/set"
	mustApply(t, s, other, nil, planner.plan())

	// Delete-policy: båda riktningarna lossas, removed:true per länk +
	// lifecycle deleted planeras av C2/E från snapshoten.
	res, err := s.DeleteEndpoint(t.Context(), "tenant-a", "mid", nil, planner.plan())
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Removed) != 2 {
		t.Fatalf("detached=%+v, want both directions", res.Removed)
	}
	snap := planner.last()
	if snap.EndpointDeleted != "mid" || len(snap.Detached) != 2 {
		t.Fatalf("delete snapshot incomplete: %+v", snap)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "mid"); len(got) != 0 {
		t.Fatalf("mid outgoing=%v", got)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "root"); len(got) != 1 || got[0] != "other" {
		t.Fatalf("root outgoing=%v, want only other", got)
	}
	rev, _ := s.Reverse(t.Context(), "tenant-a", "contains", "leaf")
	if len(rev) != 0 {
		t.Fatalf("leaf reverse=%+v", rev)
	}
	// Oberörd tenant påverkas inte.
	if _, err := s.DeleteEndpoint(t.Context(), "tenant-b", "mid", nil, planner.plan()); err != nil {
		t.Fatal(err)
	}
}

func TestProjectionWorkerRunsAndStops(t *testing.T) {
	s := pgTestStore(t)
	planner := &recordingPlanner{}

	mustApply(t, s, setChange("tenant-a", "from-1", "to-1", 1), nil, planner.plan())

	delivered := make(chan PendingItem, 4)
	broker := func(ctx context.Context, item PendingItem) error {
		delivered <- item
		return nil
	}
	worker, err := NewWorker(s, broker, 20*time.Millisecond, 10, time.Minute, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	select {
	case item := <-delivered:
		if item.Tenant != "tenant-a" || item.Operation == "" || len(item.Payload) == 0 {
			t.Fatalf("item=%+v", item)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not deliver")
	}
	// Stopp: inflöde stoppas, pågående inväntas inom budget, Run returnerar.
	stop()
	select {
	case rerr := <-done:
		if !errors.Is(rerr, context.Canceled) {
			t.Fatalf("run returned %v, want context.Canceled", rerr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop within budget")
	}
	if _, err := NewWorker(nil, broker, time.Second, 1, time.Minute, time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil store worker: %v", err)
	}
	if _, err := NewWorker(s, nil, time.Second, 1, time.Minute, time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil broker worker: %v", err)
	}
}
