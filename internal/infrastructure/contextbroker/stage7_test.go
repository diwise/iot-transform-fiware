package contextbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStage7PartialReportPreservedAndStatusPolicy(t *testing.T) {
	body := `{"updated":["first"],"notUpdated":[{"attributeName":"second","reason":"secret-response-data"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(207); _, _ = w.Write([]byte(body)) }))
	defer server.Close()
	cb, _ := NewContextBrokerClientFactory(t.Context(), server.URL, "test", "1", "", "", "", false)("tenant-a")
	fragment, _ := entities.NewFragment(decorators.Number("first", 1))
	_, err := cb.MergeEntity(t.Context(), "urn:example:object", fragment, nil)
	var partial *PartialUpdateError
	if !errors.As(err, &partial) || len(partial.Result.Updated) != 1 || len(partial.Result.NotUpdated) != 1 || string(partial.Body) != body {
		t.Fatalf("report lost: %v", err)
	}
	if strings.Contains(err.Error(), "secret-response-data") {
		t.Fatal("error string logs raw response")
	}
	for _, status := range []int{400, 401, 403, 408, 413, 422, 429, 500, 503} {
		want := status == 400 || status == 413 || status == 422
		if got := PermanentBrokerError(statusError("PATCH", status, nil)); got != want {
			t.Fatalf("status %d permanent=%v want %v", status, got, want)
		}
	}
}

func TestStage7TraceParentsAndFailureStatus(t *testing.T) {
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})
	var traceHeader atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceHeader.Store(r.Header.Get("traceparent"))
		w.WriteHeader(503)
	}))
	defer server.Close()
	ctx, root := otel.Tracer("test").Start(t.Context(), "transport-parent")
	cb, _ := NewContextBrokerClientFactory(ctx, server.URL, "test", "1", "", "", "", false)("tenant-a")
	writes := []transform.EntityWrite{{RuleID: "rule-1", RuleRevision: 2, SnapshotVersion: 7, EntityID: "urn:example:object", TypeName: "Generic", Props: []entities.EntityDecoratorFunc{decorators.Number("reading", 1)}}}
	if err := ApplyEntityWrites(ctx, cb, writes, ""); err == nil {
		t.Fatal("expected broker failure")
	}
	root.End()
	var apply, sending sdktrace.ReadOnlySpan
	for _, span := range recorder.Ended() {
		if span.Name() == "apply-writes" {
			apply = span
		}
		if span.Name() == "merge-entity" {
			sending = span
		}
	}
	if apply == nil || sending == nil || apply.Parent().SpanID() != root.SpanContext().SpanID() || sending.Parent().SpanID() != apply.SpanContext().SpanID() {
		t.Fatal("span parent chain broken")
	}
	if apply.Status().Code != codes.Error || sending.Status().Code != codes.Error || traceHeader.Load() == "" {
		t.Fatal("failure status or HTTP propagation lost")
	}
	attrs := map[string]any{}
	for _, attr := range apply.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	if attrs["rule_id"] != "rule-1" || attrs["rule_revision"] != int64(2) || attrs["snapshot_version"] != int64(7) {
		t.Fatalf("rule trace metadata lost: %v", attrs)
	}
}

func TestStage7WriteReplayAndCreateRace(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var values = map[string]float64{}
	failSecond := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		id := strings.TrimPrefix(r.URL.Path, "/ngsi-ld/v1/entities/")
		calls = append(calls, r.Method+":"+id)
		if r.Header.Get("NGSILD-Tenant") != "tenant-a" || r.Header.Get("User-Agent") != "test/1" {
			t.Error("correlation headers lost")
		}
		if id == "urn:example:second" && failSecond {
			w.WriteHeader(503)
			return
		}
		if r.Method == "DELETE" {
			delete(values, id)
			w.WriteHeader(404)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.Method == "POST" {
			id = body["id"].(string)
			if _, exists := values[id]; exists {
				w.WriteHeader(409)
				return
			}
		}
		values[id] = body["reading"].(map[string]any)["value"].(float64)
		if r.Method == "POST" {
			w.WriteHeader(201)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	cb, _ := NewContextBrokerClientFactory(t.Context(), server.URL, "test", "1", "", "", "", false)("tenant-a")
	writes := []transform.EntityWrite{{EntityID: "urn:example:first", TypeName: "Generic", Props: []entities.EntityDecoratorFunc{decorators.Number("reading", 1)}}, {EntityID: "urn:example:second", TypeName: "Generic", Props: []entities.EntityDecoratorFunc{decorators.Number("reading", 2)}}}
	err := ApplyEntityWrites(t.Context(), cb, writes, "")
	var batch *WriteBatchError
	mu.Lock()
	if !errors.As(err, &batch) || batch.Completed != 1 || values["urn:example:first"] != 1 {
		t.Fatalf("partial batch report: %v", err)
	}
	failSecond = false
	mu.Unlock()
	if err := ApplyEntityWrites(t.Context(), cb, writes, ""); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	validValues := len(values) == 2 && values["urn:example:first"] == 1 && values["urn:example:second"] == 2
	mu.Unlock()
	if !validValues {
		t.Fatal("replay did not converge")
	}
	if err := ApplyEntityWrites(t.Context(), cb, []transform.EntityWrite{{EntityID: "urn:example:first", TypeName: "Generic", Delete: true}}, ""); err != nil {
		t.Fatal(err)
	}
	_ = calls
	// Creation race: PATCH 404, POST 409, then exactly one PATCH.
	var count atomic.Int64
	raceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := count.Add(1)
		if n == 1 {
			w.WriteHeader(404)
		} else if n == 2 {
			w.WriteHeader(409)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer raceServer.Close()
	raceClient, _ := NewContextBrokerClientFactory(t.Context(), raceServer.URL, "test", "1", "", "", "", false)("tenant-a")
	if err := ApplyEntityWrites(t.Context(), raceClient, writes[:1], ""); err != nil || count.Load() != 3 {
		t.Fatalf("race not bounded: calls=%d err=%v", count.Load(), err)
	}
}

func TestStage7TokenBudgetCancellationAndCaching(t *testing.T) {
	var tokenCalls atomic.Int64
	var brokerCalls atomic.Int64
	var slow atomic.Bool
	slow.Store(true)
	release := make(chan struct{})
	tokens := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		if slow.Load() {
			select {
			case <-r.Context().Done():
			case <-release:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"test-secret-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokens.Close()
	defer close(release)
	broker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer test-secret-token" {
			t.Error("token not attached")
		}
		w.WriteHeader(204)
	}))
	defer broker.Close()
	factory := NewContextBrokerClientFactory(t.Context(), broker.URL, "test", "1", "client", "secret", tokens.URL, false, WithTimeouts(time.Second, 25*time.Millisecond))
	cb, err := factory("tenant-a")
	if err != nil || tokenCalls.Load() != 0 {
		t.Fatal("factory performed token I/O")
	}
	fragment, _ := entities.NewFragment(decorators.Number("reading", 1))
	_, err = cb.MergeEntity(t.Context(), "urn:example:object", fragment, nil)
	if !errors.Is(err, context.DeadlineExceeded) || brokerCalls.Load() != 0 {
		t.Fatalf("token timeout lost: %v calls=%d", err, brokerCalls.Load())
	}
	slow.Store(false)
	for range 2 {
		if _, err := cb.MergeEntity(t.Context(), "urn:example:object", fragment, nil); err != nil {
			t.Fatal(err)
		}
	}
	if tokenCalls.Load() != 2 || brokerCalls.Load() != 2 {
		t.Fatalf("token not cached: tokens=%d broker=%d", tokenCalls.Load(), brokerCalls.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := cb.MergeEntity(ctx, "urn:example:object", fragment, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestStage7KeyLockCancellation(t *testing.T) {
	locks := newKeyedLocks()
	unlock, err := locks.lock(t.Context(), "same")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := locks.lock(ctx, "same"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock did not honor cancellation: %v", err)
	}
	unlock()
	if len(locks.locks) != 0 {
		t.Fatal("canceled waiter leaked lock entry")
	}
}

func TestStage7PartialHTTPResponseIsNotSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write([]byte(`{"updated":["first"],"notUpdated":[{"attributeName":"second","reason":"temporary dependency failure"}]}`))
	}))
	defer server.Close()
	factory := NewContextBrokerClientFactory(t.Context(), server.URL, "test", "test", "", "", "", false)
	cb, err := factory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := entities.NewFragment(decorators.Number("first", 1), decorators.Number("second", 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cb.MergeEntity(t.Context(), "urn:example:object", fragment, nil); err == nil {
		t.Fatal("HTTP 207 with failed attribute reported as success")
	}
}

func TestStage7Malformed207IsNotSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(207); _, _ = w.Write([]byte(`{}`)) }))
	defer server.Close()
	cb, err := NewContextBrokerClientFactory(t.Context(), server.URL, "test", "test", "", "", "", false)("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	fragment, _ := entities.NewFragment(decorators.Number("reading", 1))
	if _, err := cb.MergeEntity(t.Context(), "urn:example:object", fragment, nil); err == nil {
		t.Fatal("unverifiable 207 reported as success")
	}
}
