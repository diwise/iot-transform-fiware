package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

// Neutral types/attributes demonstrate that projection and execution know
// nothing about the tank/greenhouse example's models or URN prefixes.
func aggregateEngine(t *testing.T) *transform.Engine {
	t.Helper()
	rules := []transform.Rule{
		{ID: "set", Revision: 1, Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: "partOf", Type: "sample", ToType: "group"},
			Entities: []transform.Entity{
				{ID: "urn:example:Sample:{{relation.from.thingId}}", Type: "Sample", Properties: []transform.Property{{Target: "parent", Type: transform.TypeRelationship, Value: "urn:example:Group:{{relation.to.thingId}}"}}},
				{ID: "urn:example:Group:{{relation.to.thingId}}", Type: "Group", Properties: []transform.Property{{Target: "members", Type: transform.TypeRelationship, Value: "urn:example:Sample:{{relation.from.thingId}}", Aggregate: &transform.RelationAggregate{Direction: "reverse"}}}},
			}},
		{ID: "remove", Revision: 1, Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: "partOf", Type: "sample", ToType: "group", RelationRemoved: true},
			Entities: []transform.Entity{{ID: "urn:example:Sample:{{relation.from.thingId}}", Type: "Sample", RemoveAttributes: []string{"parent"}}}},
		{ID: "delete-sample", Revision: 1, Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: "deleted", Type: "sample"}, Entities: []transform.Entity{{ID: "urn:example:Sample:{{id}}", Type: "Sample", Delete: true}}},
		{ID: "delete-group", Revision: 1, Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: "deleted", Type: "group"}, Entities: []transform.Entity{{ID: "urn:example:Group:{{id}}", Type: "Group", Delete: true}}},
	}
	if err := transform.Validate(rules); err != nil {
		t.Fatal(err)
	}
	return transform.NewEngine(rules)
}

func aggregateChange(tenant, from, to string, rev int64, removed bool) relations.Change {
	return relations.Change{Tenant: tenant, Relation: "partOf", FromID: from, ToID: to, Revision: rev, Removed: removed,
		RelationID: relations.BuildRelationID(tenant, from, "partOf", to, rev, removed), FromKind: "sample", ToKind: "group"}
}

func aggregatePlan(e *transform.Engine, c relations.Change) (relations.PlanFunc, error) {
	writes, err := e.Transform(transform.RelationInput(relations.LinkFromChange(c), c.Removed))
	if err != nil {
		return nil, err
	}
	return e.RelationPlanner(writes, ""), nil
}

func applyAggregate(t *testing.T, s *Store, e *transform.Engine, c relations.Change) Result {
	t.Helper()
	p, err := aggregatePlan(e, c)
	if err != nil {
		t.Fatal(err)
	}
	return mustApply(t, s, c, nil, p)
}

// A protocol-level broker state model. The real NGSI-LD payload produced by
// the engine is interpreted, not merely counts of planner calls.
type aggregateBroker struct {
	mu   sync.Mutex
	docs map[string]map[string]any
}

func newAggregateBroker() *aggregateBroker {
	return &aggregateBroker{docs: map[string]map[string]any{}}
}

func (b *aggregateBroker) deliver(_ context.Context, item PendingItem) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	key := item.Tenant + "\x00" + item.EntityID
	var payload map[string]any
	if err := json.Unmarshal(item.Payload, &payload); err != nil {
		return err
	}
	switch item.Operation {
	case relations.OpDelete:
		delete(b.docs, key)
	case relations.OpRemoveAttributes:
		attrs, _ := payload["removeAttributes"].([]any)
		for _, a := range attrs {
			delete(b.docs[key], a.(string))
		}
	default:
		if b.docs[key] == nil {
			b.docs[key] = map[string]any{}
		}
		for k, v := range payload {
			if k != "id" && k != "type" && k != "@context" {
				b.docs[key][k] = v
			}
		}
	}
	return nil
}

func (b *aggregateBroker) objects(tenant, entity, attribute string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	doc, _ := b.docs[tenant+"\x00"+entity][attribute].(map[string]any)
	switch v := doc["object"].(type) {
	case string:
		return []string{v}
	case []any:
		out := make([]string, 0, len(v))
		for _, s := range v {
			out = append(out, s.(string))
		}
		return out
	}
	return nil
}

func drainAggregate(t *testing.T, s *Store, b *aggregateBroker) {
	t.Helper()
	for {
		n, failed, err := s.DrainOnce(t.Context(), "", 100, time.Minute, b.deliver)
		if err != nil || failed != 0 {
			t.Fatalf("drain %d/%d: %v", n, failed, err)
		}
		if n == 0 {
			break
		}
	}
}

func expectMembers(t *testing.T, b *aggregateBroker, tenant, owner string, members ...string) {
	t.Helper()
	want := make([]string, 0, len(members))
	for _, id := range members {
		want = append(want, "urn:example:Sample:"+id)
	}
	sort.Strings(want)
	got := b.objects(tenant, "urn:example:Group:"+owner, "members")
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s/%s members=%v want %v", tenant, owner, got, want)
	}
}

func TestAggregateProjectionFourRemoveMoveLastAndReplay(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	for _, from := range []string{"4", "1", "3", "2"} {
		applyAggregate(t, s, e, aggregateChange("t", from, "a", 1, false))
	}
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1", "2", "3", "4")
	for _, id := range []string{"1", "2", "3", "4"} {
		if got := b.objects("t", "urn:example:Sample:"+id, "parent"); !reflect.DeepEqual(got, []string{"urn:example:Group:a"}) {
			t.Fatalf("forward relation=%v", got)
		}
	}
	applyAggregate(t, s, e, aggregateChange("t", "2", "a", 2, true))
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1", "3", "4")
	move := aggregateChange("t", "1", "b", 2, false)
	move.PreviousToID = "a"
	applyAggregate(t, s, e, move)
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "3", "4")
	expectMembers(t, b, "t", "b", "1")
	if got := b.objects("t", "urn:example:Sample:1", "parent"); !reflect.DeepEqual(got, []string{"urn:example:Group:b"}) {
		t.Fatal(got)
	}
	applyAggregate(t, s, e, aggregateChange("t", "3", "a", 2, true))
	applyAggregate(t, s, e, aggregateChange("t", "4", "a", 2, true))
	// Old exact replay after detach MUST NOT reinsert the link, even though
	// its earlier pending work has already been delivered and deleted.
	res := applyAggregate(t, s, e, aggregateChange("t", "4", "a", 1, false))
	if !res.Duplicate {
		t.Fatal("old known event not deduplicated")
	}
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a")
	if got, _ := s.Reverse(t.Context(), "t", "partOf", "a"); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestAggregateProjectionReturnToSameStateBeforeDelivery(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	// Same payload at revisions 1 and 3, with an intervening clear, must
	// not be deduplicated just because revision 1 is still pending.
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 1, false))
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 2, true))
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 3, false))
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1")
}

func deleteAggregate(t *testing.T, s *Store, e *transform.Engine, tenant, id, kind, eventID string) Result {
	t.Helper()
	writes, err := e.Transform(transform.Thing{ID: id, Type: kind, Tenant: tenant, Event: transform.EventThingsV1Lifecycle, Lifecycle: "deleted"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.DeleteEndpointEvent(t.Context(), tenant, id, eventID, nil, e.RelationPlanner(writes, ""))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestAggregateProjectionEndpointDeletionAndDuplicateLifecycle(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	for _, from := range []string{"1", "2", "3", "4"} {
		applyAggregate(t, s, e, aggregateChange("t", from, "a", 1, false))
	}
	applyAggregate(t, s, e, aggregateChange("other", "1", "a", 1, false))
	drainAggregate(t, s, b)
	deleteAggregate(t, s, e, "t", "2", "sample", "deleted-2")
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1", "3", "4")
	expectMembers(t, b, "other", "a", "1")
	if got := deleteAggregate(t, s, e, "t", "2", "sample", "deleted-2"); !got.Duplicate {
		t.Fatal("duplicate lifecycle was reapplied")
	}
	// Producer also emits removed after deleted; it must not recreate 2.
	applyAggregate(t, s, e, aggregateChange("t", "2", "a", 1, true))
	drainAggregate(t, s, b)
	if _, ok := b.docs["t\x00urn:example:Sample:2"]; ok {
		t.Fatal("deleted source recreated")
	}
	// Source contract blocks deleting a partOf target with live children.
	// If a valid deleted event still arrives (other relation names, recovery),
	// transform detaches both endpoints and cleans surviving forward refs.
	deleteAggregate(t, s, e, "t", "a", "group", "deleted-a")
	drainAggregate(t, s, b)
	if _, ok := b.docs["t\x00urn:example:Group:a"]; ok {
		t.Fatal("deleted target recreated")
	}
	for _, from := range []string{"1", "3", "4"} {
		if got := b.objects("t", "urn:example:Sample:"+from, "parent"); len(got) != 0 {
			t.Fatal(got)
		}
	}
	expectMembers(t, b, "other", "a", "1")
}

func TestAggregateProjectionBrokerRetryRestartAndOrdering(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 1, false))
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 2, true))
	failedID := ""
	_, failed, err := s.DrainOnce(t.Context(), "t", 50, time.Minute, func(ctx context.Context, item PendingItem) error {
		if item.EntityID == "urn:example:Group:a" {
			failedID = item.ID
			return errors.New("broker 503")
		}
		return b.deliver(ctx, item)
	})
	if err != nil || failed != 1 || failedID == "" {
		t.Fatalf("failure=%d %v", failed, err)
	}
	// Old failed set blocks its younger clear. Restart retains both and
	// delivery order: the old list must never be the final broker write.
	s.Close()
	s = pgTestStoreNoTruncate(t)
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a")
	if count, err := s.PendingCount(t.Context(), ""); count != 0 || err != nil {
		t.Fatal(count, err)
	}
}

func TestAggregateProjectionDeliveryCancellationAndTrace(t *testing.T) {
	for _, succeeded := range []bool{true, false} {
		t.Run(fmt.Sprintf("broker_success=%t", succeeded), func(t *testing.T) {
			recorder, stopTracing := withTestTracer(t)
			defer stopTracing()
			s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
			transportCtx, parent := otel.Tracer("test").Start(t.Context(), "aggregate-transport")
			origin := parent.SpanContext()
			// Aggregate-only planning keeps two successive lists on one entity.
			for _, id := range []string{"1", "2"} {
				mustApply(t, s, aggregateChange("t", id, "a", 1, false), CaptureTrace(transportCtx), e.RelationPlanner(nil, ""))
			}
			parent.End()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			completed, failed, err := s.DrainOnce(ctx, "t", 1, time.Minute, func(itemCtx context.Context, item PendingItem) error {
				cancel()
				if !errors.Is(itemCtx.Err(), context.Canceled) {
					t.Fatal("broker did not receive the request cancellation")
				}
				if !succeeded {
					return itemCtx.Err()
				}
				// HTTP may have succeeded just as its caller canceled. The
				// bounded acknowledgement still owns the row lock at this point.
				return b.deliver(itemCtx, item)
			})
			if err != nil || completed+failed != 1 || (completed == 1) != succeeded {
				t.Fatalf("canceled delivery=%d/%d: %v", completed, failed, err)
			}
			wantPending := 2
			if succeeded {
				wantPending = 1
			}
			if count, err := s.PendingCount(t.Context(), "t"); err != nil || count != wantPending {
				t.Fatalf("pending=%d, want %d: %v", count, wantPending, err)
			}
			found := false
			for _, span := range recorder.Ended() {
				if span.Name() != "projection-deliver" {
					continue
				}
				found = true
				if (span.Status().Code == codes.Error) == succeeded {
					t.Fatalf("delivery span status=%v, broker success=%t", span.Status(), succeeded)
				}
				linked := false
				for _, link := range span.Links() {
					linked = linked || (link.SpanContext.TraceID() == origin.TraceID() && link.SpanContext.SpanID() == origin.SpanID())
				}
				if !linked {
					t.Fatal("delivery span lost its persisted transport origin")
				}
			}
			if !found {
				t.Fatal("no delivery span")
			}
			drainAggregate(t, s, b)
			expectMembers(t, b, "t", "a", "1", "2")
		})
	}
}

func TestAggregateProjectionImportCancellationRollsBack(t *testing.T) {
	s, e := pgTestStore(t), aggregateEngine(t)
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 1, false))
	before, err := s.ExportTenant(t.Context(), "t")
	if err != nil {
		t.Fatal(err)
	}
	pendingBefore, err := s.PendingCount(t.Context(), "t")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replacement := []relations.Link{relations.LinkFromChange(aggregateChange("t", "2", "b", 1, false))}
	planner := e.RelationPlanner(nil, "")
	_, err = s.ImportTenant(ctx, "t", replacement, nil, func(snap relations.Snapshot) ([]relations.PendingWrite, error) {
		// Cancellation arrives after replacing source_links but before the
		// corresponding queue insert/commit. Neither state may leak out.
		cancel()
		return planner(snap)
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("import lost cancellation: %v", err)
	}
	after, err := s.ExportTenant(t.Context(), "t")
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("canceled import changed source state: %v, %v", after, err)
	}
	if count, err := s.PendingCount(t.Context(), "t"); err != nil || count != pendingBefore {
		t.Fatalf("canceled import changed pending: %d, %v", count, err)
	}
}

func TestAggregateProjectionConcurrentInstancesAndDrains(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	other := pgTestStoreNoTruncate(t)
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := range 24 {
		wg.Go(func() {
			c := aggregateChange("t", fmt.Sprint(i), "a", 1, false)
			p, err := aggregatePlan(e, c)
			if err == nil {
				_, err = other.ApplyChange(t.Context(), c, nil, p)
			}
			if err != nil {
				errs <- err
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			for range 10 {
				if _, _, err := s.DrainOnce(t.Context(), "", 5, time.Minute, b.deliver); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	drainAggregate(t, s, b)
	want := make([]string, 24)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	expectMembers(t, b, "t", "a", want...)
}

func TestAggregateProjectionLockedDeliveryCannotBeOvertaken(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 1, false))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var block sync.Once
	go func() {
		_, _, err := s.DrainOnce(t.Context(), "t", 10, time.Nanosecond, func(ctx context.Context, item PendingItem) error {
			if item.EntityID == "urn:example:Group:a" {
				block.Do(func() { close(entered); <-release })
			}
			return b.deliver(ctx, item)
		})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery not started")
	}
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 2, true))
	// Tiny/expired lease cannot let another worker pass the row lock.
	_, _, err := s.DrainOnce(t.Context(), "t", 10, time.Nanosecond, b.deliver)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a")
}

func TestAggregateProjectionUnseenLateEventLimitation(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 3, true))
	// Contract explicitly has no source-revision recency filter. A never
	// before seen older set is applied; this is NOT convergence evidence.
	applyAggregate(t, s, e, aggregateChange("t", "1", "a", 1, false))
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1")
}

func TestAggregateProjectionRebuildImportAndRollback(t *testing.T) {
	s, e, b := pgTestStore(t), aggregateEngine(t), newAggregateBroker()
	// Links received before rule activation are retained even without writes.
	for _, id := range []string{"1", "2", "3", "4"} {
		mustApply(t, s, aggregateChange("t", id, "a", 1, false), nil, nil)
	}
	res, err := s.Rebuild(t.Context(), "t", "partOf", nil, e.RelationPlanner(nil, ""), 0)
	if err != nil || res.Pending != 1 {
		t.Fatalf("rebuild=%+v %v", res, err)
	}
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a", "1", "2", "3", "4")
	links, err := s.ExportTenant(t.Context(), "t")
	if err != nil {
		t.Fatal(err)
	}
	// Failed planner cannot change links, queue or seen IDs.
	_, err = s.ImportTenant(t.Context(), "t", nil, nil, func(relations.Snapshot) ([]relations.PendingWrite, error) { return nil, errors.New("planner failed") })
	if err == nil {
		t.Fatal("expected rollback")
	}
	after, err := s.ExportTenant(t.Context(), "t")
	if err != nil || !reflect.DeepEqual(after, links) {
		t.Fatal(after, err)
	}
	// Authoritative replacement includes an offline move and removals. Both
	// old owner (now empty) and new owner must be reconciled atomically.
	links[0].ToID = "b"
	res, err = s.ImportTenant(t.Context(), "t", links[:1], nil, e.RelationPlanner(nil, ""))
	if err != nil || res.Pending < 2 {
		t.Fatal(res, err)
	}
	drainAggregate(t, s, b)
	expectMembers(t, b, "t", "a")
	expectMembers(t, b, "t", "b", "1")
	foreign := append([]relations.Link(nil), links[:1]...)
	foreign[0].Tenant = "another"
	if _, err := s.ImportTenant(t.Context(), "t", foreign, nil, e.RelationPlanner(nil, "")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// A current empty projection cannot manufacture old, never-seen links.
	res, err = s.Rebuild(t.Context(), "never-seen", "", nil, e.RelationPlanner(nil, ""), 0)
	if err != nil || res.Groups != 0 || res.Pending != 0 {
		t.Fatal(res, err)
	}
}
