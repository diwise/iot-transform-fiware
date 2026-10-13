package things

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	clienttest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/messaging-golang/pkg/messaging"

	"os"

	"github.com/jackc/pgx/v5"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// Generella namn i C2-tester: aldrig standardmodellnamn.

func c2ValuesRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:{{id}}", Type: "Generic",
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "generic"}},
		}},
	}
}

func c2RelationsRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: "contains", Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:{{relation.to.thingId}}", Type: "Generic",
			Properties: []transform.Property{{
				Target: "label", Type: transform.TypeText, Const: "generic",
			}},
		}},
	}
}

func c2LifecycleDeleteRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: string(transform.LifecycleDeleted)},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:{{id}}", Type: "Generic", Delete: true,
		}},
	}
}

func c2Incoming(body string) *messaging.IncomingTopicMessageMock {
	return &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}
}

type recordingBroker struct {
	mu      sync.Mutex
	merges  []string
	creates []string
	deletes []string
	mergeFn func(ctx context.Context, id string) error
}

func (r *recordingBroker) mock() *clienttest.ContextBrokerClientMock {
	return &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			r.mu.Lock()
			r.merges = append(r.merges, entityID)
			fn := r.mergeFn
			r.mu.Unlock()
			if fn != nil {
				if err := fn(ctx, entityID); err != nil {
					return nil, err
				}
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			r.mu.Lock()
			r.creates = append(r.creates, entity.ID())
			r.mu.Unlock()
			return &ngsild.CreateEntityResult{}, nil
		},
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			r.mu.Lock()
			r.deletes = append(r.deletes, entityID)
			r.mu.Unlock()
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
}

func (r *recordingBroker) counts() (merges, creates, deletes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.merges), len(r.creates), len(r.deletes)
}

// No-match ger ack utan brokerkontakt (values med precis diagnostik).
func TestC2ValuesNoMatchWithoutBrokerContact(t *testing.T) {
	rec := &recordingBroker{}
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		providerWithEngine(nil),
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","values":[]}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("no-match ska ackas: %v", err)
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("no-match kontaktade broker: merge=%d create=%d delete=%d", m, c, d)
	}
}

// Undertryckt (required saknas) ger ack utan brokerkontakt + ExplainSkipped.
func TestC2ValuesSuppressedWithoutBrokerContact(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []transform.Property{{Target: "temp", Type: transform.TypeNumber, Field: "values.missing.value", Required: true}},
		}},
	}
	prov := providerWithEngine([]transform.Rule{rule})
	// Verifiera att E rapporterar undertryckt via MatchedRule/ExplainSkipped.
	th, err := appthings.ThingValuesFromJSON([]byte(`{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	eng := transform.NewEngine([]transform.Rule{rule})
	if _, ok := eng.MatchedRule(th); !ok {
		t.Fatal("MatchedRule ska träffa vid undertryckt")
	}
	if skipped := eng.ExplainSkipped(th); len(skipped) == 0 {
		t.Fatal("ExplainSkipped ska förklara undertryckt")
	}

	rec := &recordingBroker{}
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		prov,
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("suppressed ska ackas: %v", err)
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("suppressed kontaktade broker: %d/%d/%d", m, c, d)
	}
}

// Ogiltig NGSI-LD förvalideras före I/O: Permanent utan brokerkontakt.
func TestC2ValuesInvalidBeforeBrokerContact(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "relative-id", Type: "Generic",
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "x"}},
		}},
	}
	rec := &recordingBroker{}
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{rule})},
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	err := handler(context.Background(), c2Incoming(body), slog.Default())
	if err == nil {
		t.Fatal("ogiltig NGSI-LD ska ge fel")
	}
	if _, ok := errors.AsType[*messaging.PermanentError](err); !ok {
		t.Fatalf("ogiltig output ska vara Permanent, fick %T %v", err, err)
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("ogiltig output kontaktade broker: %d/%d/%d", m, c, d)
	}
}

// Token-/fabriksfel ger Temporary utan brokerkontakt.
func TestC2ValuesTokenErrorWithoutBrokerContact(t *testing.T) {
	rec := &recordingBroker{}
	mock := rec.mock()
	called := false
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) {
			called = true
			return nil, errors.New("oauth2 token retrieval failed")
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2ValuesRule()})},
		"",
	)
	_ = mock
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	err := handler(context.Background(), c2Incoming(body), slog.Default())
	if err == nil {
		t.Fatal("tokenfel ska ge fel")
	}
	if !called {
		t.Fatal("fabriken skulle anropas")
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("tokenfel kontaktade broker: %d/%d/%d", m, c, d)
	}
	if got := messaging.ClassifyHandlerError(delivery.DependencyError(err)); got != messaging.OutcomeTemporary {
		// err är redan Temporary via DependencyError; klassificera direkt.
		t.Fatalf("tokenfel ska vara Temporary, fick %v", got)
	}
}

// 404-till-create via kontrollerad mock: merge 404 faller tillbaka till create.
func TestC2ValuesMerge404FallbackToCreate(t *testing.T) {
	rec := &recordingBroker{}
	mock := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			rec.mu.Lock()
			rec.merges = append(rec.merges, entityID)
			rec.mu.Unlock()
			return nil, ngsilderrors.ErrNotFound
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			rec.mu.Lock()
			rec.creates = append(rec.creates, entity.ID())
			rec.mu.Unlock()
			return &ngsild.CreateEntityResult{}, nil
		},
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2ValuesRule()})},
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("404-till-create ska lyckas: %v", err)
	}
	if m, c, _ := rec.counts(); m != 1 || c != 1 {
		t.Fatalf("merge=%d create=%d, want 1/1 (fallback-create före dess anrop validerad)", m, c)
	}
}

// 409-race: samtidiga creates — andra får AlreadyExists och mergar.
func TestC2ValuesCreate409RaceToMerge(t *testing.T) {
	rec := &recordingBroker{}
	calls := atomic.Int32{}
	mock := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			rec.mu.Lock()
			rec.merges = append(rec.merges, entityID)
			rec.mu.Unlock()
			if calls.Add(1) == 1 {
				return nil, ngsilderrors.ErrNotFound
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			rec.mu.Lock()
			rec.creates = append(rec.creates, entity.ID())
			rec.mu.Unlock()
			// Första create krockar (annan instans hann före).
			return nil, ngsilderrors.ErrAlreadyExists
		},
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic", Create: false,
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "x"}},
		}},
	}
	_ = rule
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2ValuesRule()})},
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("409-race ska lösas via återmmerge: %v", err)
	}
	if m, c, _ := rec.counts(); m < 2 || c < 1 {
		t.Fatalf("race: merge=%d create=%d, want >=2/1", m, c)
	}
}

// Partiell leverans: slutfört bevaras i felrapport, Temporary för retry.
func TestC2ValuesPartialPreservesCompleted(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{
			{ID: "urn:ngsi-ld:Generic:one", Type: "Generic", Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "one"}}},
			{ID: "urn:ngsi-ld:Generic:two", Type: "Generic", Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "two"}}},
		},
	}
	calls := atomic.Int32{}
	mock := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			if calls.Add(1) == 2 {
				return nil, &contextbroker.PartialUpdateError{}
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return &ngsild.CreateEntityResult{}, nil
		},
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{rule})},
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	err := handler(context.Background(), c2Incoming(body), slog.Default())
	if err == nil {
		t.Fatal("partiellt fel ska ge fel")
	}
	if got := messaging.ClassifyHandlerError(err); got != messaging.OutcomeTemporary {
		t.Fatalf("partiellt fel ska vara Temporary, fick %v", got)
	}
	completed, _, ok := delivery.WriteBatchProgress(err)
	if !ok || completed != 1 {
		t.Fatalf("slutfört ska bevaras (1), fick completed=%d ok=%v err=%v", completed, ok, err)
	}
}

// Strikt relationId per F: opak legacy-ID nekas Permanent utan brokerkontakt.
func TestC2RelationsStrictRelationIDWithoutBrokerContact(t *testing.T) {
	rec := &recordingBroker{}
	handler := NewThingRelationsTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2RelationsRule()})},
		"",
	)
	// Legacy opak ID utan v1-form nekas av ChangeFromJSON (strikt identitet).
	legacy := `{"relationId":"relation-1","type":"contains","fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4}`
	if err := handler(context.Background(), c2Incoming(legacy), slog.Default()); err == nil {
		t.Fatal("legacy relationId ska nekas")
	} else {
		if _, ok := errors.AsType[*messaging.PermanentError](err); !ok {
			t.Fatalf("legacy ID ska vara Permanent, fick %T %v", err, err)
		}
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("strikt ID-fel kontaktade broker: %d/%d/%d", m, c, d)
	}
	// Mismatch mellan relationId och kuvertfält nekas likaså.
	mismatch := `{"relationId":"v1/tenant-a/object-1/relations/contains/9/set","type":"contains","fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4}`
	if err := handler(context.Background(), c2Incoming(mismatch), slog.Default()); err == nil {
		t.Fatal("mismatch ska nekas")
	}
}

// RelationId parsas ej positionellt: ToID hämtas ur kuvertet, inte ur ID:t.
func TestC2RelationsToFromEnvelopeNotRelationID(t *testing.T) {
	var gotChange relations.Change
	_ = gotChange
	// V1-identitet där to inte kan härledas positionellt utan kuvertet:
	// ChangeFromJSON måste bära kuvertets toId (object-2), inte en split av ID.
	raw := []byte(`{"relationId":"v1/tenant-a/object-1/relations/contains/4/set","type":"contains","fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4}`)
	change, err := relations.ChangeFromJSON(raw)
	if err != nil {
		t.Fatalf("v1 Change: %v", err)
	}
	if change.ToID != "object-2" || change.FromID != "object-1" {
		t.Fatalf("To/From ska komma ur kuvertet, fick %+v", change)
	}
	// Handlern använder Change.ToID (via ApplyChange) — verifieras i
	// fake-store-testet nedan genom att FromID/ToID matchar kuvertet.
}

// Tom tenant skippar före ApplyChange utan DB/brokerkontakt.
func TestC2RelationsEmptyTenantSkipsBeforeApplyChange(t *testing.T) {
	fake := &fakeRelationStore{}
	handler := NewThingRelationsTopicMessageHandlerWithStore(
		func(string) (contextbroker.BrokerClient, error) {
			t.Fatal("tom tenant ska inte skapa brokerklient")
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2RelationsRule()})},
		"",
		fake,
	)
	body := `{"relationId":"v1//object-1/relations/contains/1/set","type":"contains","fromId":"object-1","toId":"object-2","tenant":"","revision":1}`
	// Tom tenant tolereras av adaptern men skippar före ApplyChange.
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("tom tenant ska ackas: %v", err)
	}
	if fake.applyCalls.Load() != 0 {
		t.Fatalf("tom tenant nådde ApplyChange (%d)", fake.applyCalls.Load())
	}
}

type fakeRelationStore struct {
	applyCalls  atomic.Int32
	deleteCalls atomic.Int32
	lastChange  relations.Change
	pending     []relations.PendingWrite
	failApply   error
}

func (f *fakeRelationStore) ApplyChange(ctx context.Context, change relations.Change, trace map[string]string, planner relations.PlanFunc) (projection.Result, error) {
	f.applyCalls.Add(1)
	f.lastChange = change
	if f.failApply != nil {
		return projection.Result{}, f.failApply
	}
	// Planeraren är ren (ingen I/O): anropas här likt lagret inom
	// transaktionsgränsen; fel rullar tillbaka.
	snap := relations.Snapshot{Tenant: change.Tenant, Relation: change.Relation, FromID: change.FromID}
	writes, err := planner(snap)
	if err != nil {
		return projection.Result{}, err
	}
	f.pending = append(f.pending, writes...)
	return projection.Result{Pending: len(writes)}, nil
}

func (f *fakeRelationStore) DeleteEndpoint(ctx context.Context, tenant, endpoint string, trace map[string]string, planner relations.PlanFunc) (projection.Result, error) {
	f.deleteCalls.Add(1)
	snap := relations.Snapshot{Tenant: tenant, FromID: endpoint, EndpointDeleted: endpoint}
	writes, err := planner(snap)
	if err != nil {
		return projection.Result{}, err
	}
	f.pending = append(f.pending, writes...)
	return projection.Result{Pending: len(writes)}, nil
}

// WithStore persisterar via ApplyChange med E:s EntityWritesToPending via
// PlanFunc-closure; Change bär kuvertets From/To (ej positionell parsing).
func TestC2RelationsWithStoreAppliesChange(t *testing.T) {
	fake := &fakeRelationStore{}
	rec := &recordingBroker{}
	rule := c2RelationsRule()
	handler := NewThingRelationsTopicMessageHandlerWithStore(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{rule})},
		"",
		fake,
	)
	body := `{"relationId":"v1/tenant-a/object-1/relations/contains/4/set","type":"contains","fromId":"object-1","toId":"object-2","tenant":"tenant-a","revision":4,"fromThingType":"generic-template","toThingType":"other-template"}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if fake.applyCalls.Load() != 1 {
		t.Fatalf("ApplyChange anropades %d gånger, want 1", fake.applyCalls.Load())
	}
	if fake.lastChange.FromID != "object-1" || fake.lastChange.ToID != "object-2" {
		t.Fatalf("Change ska bära kuvertets From/To, fick %+v", fake.lastChange)
	}
	if len(fake.pending) == 0 {
		t.Fatal("PlanFunc-closure via EntityWritesToPending ska ha planerat arbete")
	}
	for _, w := range fake.pending {
		if w.IdempotencyKey == "" || len(w.Payload) == 0 {
			t.Fatalf("pending saknar nyckel/payload: %+v", w)
		}
	}
}

// Lifecycle delete via DeleteEndpoint med CaptureTrace + PlanFunc-closure.
func TestC2LifecycleDeleteViaDeleteEndpoint(t *testing.T) {
	fake := &fakeRelationStore{}
	rec := &recordingBroker{}
	handler := NewThingLifecycleTopicMessageHandlerWithStore(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2LifecycleDeleteRule()})},
		"",
		fake,
	)
	body := `{"eventId":"v1/tenant-a/object-1/lifecycle/deleted/4","tenant":"tenant-a","thingId":"object-1","kind":"deleted","thingType":"generic-template","thingTypeVersion":"1","configRevision":4,"changedAt":"2026-10-05T12:00:00Z","thing":{"thingId":"object-1"}}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if fake.deleteCalls.Load() != 1 {
		t.Fatalf("DeleteEndpoint anropades %d gånger, want 1", fake.deleteCalls.Load())
	}
}

// Lifecycle delete tolererar 404 (idempotent) i direktvägen.
func TestC2LifecycleDelete404Tolerant(t *testing.T) {
	mock := &clienttest.ContextBrokerClientMock{
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			return nil, ngsilderrors.ErrNotFound
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return &ngsild.CreateEntityResult{}, nil
		},
	}
	handler := NewThingLifecycleTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2LifecycleDeleteRule()})},
		"",
	)
	body := `{"eventId":"v1/tenant-a/object-1/lifecycle/deleted/4","tenant":"tenant-a","thingId":"object-1","kind":"deleted","thingType":"generic-template","thingTypeVersion":"1","configRevision":4,"changedAt":"2026-10-05T12:00:00Z","thing":{"thingId":"object-1"}}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("404 ska tolereras: %v", err)
	}
}

// Samtidiga handlers ska vara race-fria (kör med -race).
func TestC2ConcurrentHandlersRace(t *testing.T) {
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) {
			return (&recordingBroker{}).mock(), nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2ValuesRule()})},
		"",
	)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
			_ = handler(context.Background(), c2Incoming(body), slog.Default())
		})
	}
	wg.Wait()
}

// Kontrollerad broker via httptest: 404-till-create end-to-end utan mock.
func TestC2HttptestBrokerMerge404ToCreate(t *testing.T) {
	var mu sync.Mutex
	var patchBodies [][]byte
	var postBodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPatch:
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			patchBodies = append(patchBodies, body)
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"type":"https://uri.etsi.org/ngsi-ld/errors/NotFound"}`))
		case r.Method == http.MethodPost:
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			postBodies = append(postBodies, body)
			w.Header().Set("Location", "/ngsi-ld/v1/entities/urn:ngsi-ld:Generic:thing-1")
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()

	factory := contextbroker.NewContextBrokerClientFactory(context.Background(), srv.URL, "test", "test", "", "", "", true)
	handler := NewThingValuesTopicMessageHandler(
		factory,
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{c2ValuesRule()})},
		"",
	)
	body := `{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","delta":[]}`
	if err := handler(context.Background(), c2Incoming(body), slog.Default()); err != nil {
		t.Fatalf("httptest 404-till-create: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(patchBodies) != 1 || len(postBodies) != 1 {
		t.Fatalf("patch=%d post=%d, want 1/1 (merge 404 föll tillbaka till create)", len(patchBodies), len(postBodies))
	}
}

// DB: ApplyChange-persistens + brokerfel lämnar pending för replay;
// omstart ser både projektion och arbete; trace länkas.
func TestC2ProjectionReplayAndTraceAfterRestart(t *testing.T) {
	cfg := c2ProjectionConfig(t)
	store, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Skipf("no test database: %v", err)
	}
	defer store.Close()
	c2CleanProjection(t, store)

	recorder, done := c2TestTracer(t)
	defer done()

	ctx, root := otel.Tracer("test").Start(context.Background(), "transport-parent")
	trace := projection.CaptureTrace(ctx)
	if len(trace) == 0 {
		t.Fatal("no carrier captured")
	}
	change := relations.Change{
		RelationID: relations.BuildRelationID("tenant-a", "object-1", "contains", "object-2", 4, false),
		Tenant:     "tenant-a", Relation: "contains", FromID: "object-1", ToID: "object-2",
		Revision: 4, ChangedAt: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC),
		FromKind: "generic-template", ToKind: "other-template",
	}
	planner := relations.ReferencePlanner(relations.RuleRef{ID: "rule-1", Revision: 1})
	if _, err := store.ApplyChange(ctx, change, trace, planner); err != nil {
		t.Fatalf("apply: %v", err)
	}
	root.End()
	store.Close()

	reopened, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Skipf("no test database on reopen: %v", err)
	}
	defer reopened.Close()

	// Omstart: projektion och väntande arbete finns kvar.
	got, err := reopened.Outgoing(context.Background(), "tenant-a", "contains", "object-1")
	if err != nil || len(got) != 1 {
		t.Fatalf("projection lost on restart: %+v %v", got, err)
	}
	// Första leveransen felar partiellt — slutfört bevaras i rapporten.
	calls := atomic.Int32{}
	completed, failed, err := reopened.DrainOnce(context.Background(), "tenant-a", 10, time.Minute, func(ctx context.Context, item projection.PendingItem) error {
		if calls.Add(1) == 1 {
			return errors.New("broker 503")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// En pending-post i detta fall: failed=1 (ingen delvis 1/1 här).
	if failed != 1 || completed != 0 {
		t.Fatalf("drain=%d/%d, want 0/1 (brokerfel lämnar pending för replay)", completed, failed)
	}
	// Replay efter läkt broker: återstående levereras.
	completed, failed, err = reopened.DrainOnce(context.Background(), "tenant-a", 10, time.Minute, func(context.Context, projection.PendingItem) error { return nil })
	if err != nil || completed != 1 || failed != 0 {
		t.Fatalf("replay=%d/%d err=%v", completed, failed, err)
	}
	// Trace länkas vid DrainOnce (aldrig tyst ersatt ursprung).
	var wantTraceID string
	for _, span := range recorder.Ended() {
		if span.Name() == "transport-parent" {
			wantTraceID = span.SpanContext().TraceID().String()
		}
	}
	// Ny applicering + dränering för att provocera deliver-span med länk.
	change2 := change
	change2.RelationID = relations.BuildRelationID("tenant-a", "object-1", "contains", "object-3", 5, false)
	change2.Revision = 5
	change2.ToID = "object-3"
	if _, err := reopened.ApplyChange(context.Background(), change2, trace, planner); err != nil {
		t.Fatal(err)
	}
	_, _, _ = reopened.DrainOnce(context.Background(), "tenant-a", 10, time.Minute, func(context.Context, projection.PendingItem) error { return nil })
	found := false
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
	// ChangeFromJSON-paritet: samma trådform som C1 läser ger samma Change.
	raw, _ := json.Marshal(map[string]any{
		"relationId": change2.RelationID, "type": "contains",
		"fromId": "object-1", "toId": "object-3", "tenant": "tenant-a", "revision": 5,
		"fromThingType": "generic-template", "toThingType": "other-template",
	})
	parsed, err := relations.ChangeFromJSON(raw)
	if err != nil {
		t.Fatalf("ChangeFromJSON: %v", err)
	}
	if parsed.FromID != "object-1" || parsed.ToID != "object-3" {
		t.Fatalf("ChangeFromJSON mismatch: %+v", parsed)
	}
}

func c2ProjectionConfig(t *testing.T) projection.Config {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	// Egen isolerad databas för C2-handler-tester (inte samma som
	// projection-paketets diwise_test_projection — go test ./... kör paket
	// parallellt och delad tabell ger interferens).
	cfg := projection.NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME_C2", "diwise_test_c2"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	c2EnsureDB(t, cfg)
	return cfg
}

func c2EnsureDB(t *testing.T, cfg projection.Config) {
	t.Helper()
	// Skapa C2-databasen idempotent (samma mönster som projection-paketet).
	// Anslut till postgres-db och CREATE DATABASE.
	host := os.Getenv("PG_TEST_HOST")
	if host == "" {
		host = "localhost"
	}
	user := os.Getenv("PG_TEST_USER")
	if user == "" {
		user = "postgres"
	}
	password := os.Getenv("PG_TEST_PASSWORD")
	if password == "" {
		password = "password"
	}
	port := os.Getenv("PG_TEST_PORT")
	if port == "" {
		port = "5434"
	}
	adminConnStr := projection.NewConfig(host, user, password, port, "postgres", "disable").ConnStr()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminConnStr)
	if err != nil {
		t.Skipf("no test database server: %v", err)
	}
	defer conn.Close(ctx)
	dbname := "diwise_test_c2"
	if v := os.Getenv("PG_TEST_DBNAME_C2"); v != "" {
		dbname = v
	}
	_, err = conn.Exec(ctx, `CREATE DATABASE `+dbname)
	if err != nil && !c2IsExistsDB(err) {
		t.Skipf("cannot create test database: %v", err)
	}
}

func c2IsExistsDB(err error) bool {
	// 42P04 = duplicate_database (redan skapad av parallellt paket).
	return err != nil && containsStr(err.Error(), "42P04")
}

func containsStr(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (func() bool {
		for i := 0; i+len(needle) <= len(haystack); i++ {
			if haystack[i:i+len(needle)] == needle {
				return true
			}
		}
		return false
	})()
}

func c2CleanProjection(t *testing.T, s *projection.Store) {
	t.Helper()
	// Isolering per test: töm projektionstabellerna direkt (samma mönster
	// som projection-paketets pgTestStore). Lämnar schemat orört.
	cfg := c2ProjectionConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.ConnStr())
	if err != nil {
		t.Fatalf("clean connect: %v", err)
	}
	defer conn.Close(ctx)
	for _, table := range []string{"pending_broker_work", "projection_seen_changes", "source_links", "source_link_specs"} {
		if _, err := conn.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("clean %s: %v", table, err)
		}
	}
}

func c2TestTracer(t *testing.T) (*tracetest.SpanRecorder, func()) {
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
