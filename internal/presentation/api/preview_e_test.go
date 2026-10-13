package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	clienttest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
)

func ePreviewRule() transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template", Tenant: "tenant-a"},
		Entities: []transform.Entity{{
			Name: "primary",
			ID:   "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []transform.Property{
				{Target: "temp", Type: transform.TypeNumber, Field: "delta.temperature.value"},
				{Target: "hum", Type: transform.TypeNumber, Field: "snapshot.humidity.value"},
			},
		}},
	}
}

func ePreviewMessage() json.RawMessage {
	return json.RawMessage(`{"thingId":"thing-1","tenant":"tenant-a","thingType":"generic-template","thingTypeVersion":"2","configRevision":7,
	  "thing":{"thingId":"thing-1"},
	  "delta":[{"propertyId":"temperature","value":22,"quality":"ok","observedAt":"2026-10-01T12:02:01Z"}],
	  "snapshot":[{"propertyId":"temperature","value":22,"quality":"ok","observedAt":"2026-10-01T12:02:01Z"},
	              {"propertyId":"humidity","value":50,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}]}`)
}

// Delta/snapshot-paritet produktion/preview: samma C1-normalisering och
// engine ger identiska skrivningar (delta deluppdatering + snapshot full).
func TestEPreviewDeltaSnapshotParity(t *testing.T) {
	rule := ePreviewRule()
	raw := ePreviewMessage()
	th, err := things.ThingValuesFromJSON([]byte(raw))
	if err != nil {
		t.Fatalf("C1: %v", err)
	}
	production, err := transform.NewEngine([]transform.Rule{rule}).Transform(th)
	if err != nil || len(production) != 1 {
		t.Fatalf("produktion: %v %v", production, err)
	}
	detail, err := previewDetailed(context.Background(), previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: raw}})
	if err != nil || len(detail.Writes) != 1 {
		t.Fatalf("preview: %v %v", detail.Writes, err)
	}
	if detail.Writes[0].EntityID != production[0].EntityID || len(detail.Writes[0].Props) != len(production[0].Props) {
		t.Fatalf("paritet bruten: %+v vs %+v", detail.Writes[0], production[0])
	}
	if !detail.Matched || detail.Observations != 1 {
		t.Fatalf("matched/observations: %+v", detail)
	}
	// Preview via HTTP bär samma entitet, operation merge och effektiv context.
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: raw}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview HTTP %d: %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched || len(out.Entities) != 1 || out.Entities[0].Operation != "merge" {
		t.Fatalf("preview svar: %+v", out)
	}
	if out.Entities[0].EffectiveContext == "" {
		t.Fatal("effektiv context saknas")
	}
}

// Vald regel/revision/snapshotversion + konkurrerande tenant-avgränsat +
// precis skiporsak vid matchad men undertryckt regel.
//
// Inline-preview bär ingen lagrad identitet (Rule.ID/Revision/
// SnapshotVersion har json:"-" och kommer från lagret, aldrig från
// regelpayloaden): svaret är ärligt tomt här, medan produktionssnapshoten
// bär full identitet via writes (se snapshot_test). Konkurrerande
// matchningar och precisa skiporsaker exponeras ändå.
func TestEPreviewRuleIdentityCompetingSkipped(t *testing.T) {
	rule := ePreviewRule()
	rule.Entities[0].Properties = []transform.Property{
		{Target: "missing", Type: transform.TypeNumber, Field: "values.absent.value", Required: true},
	}
	raw := ePreviewMessage()
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: raw}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d: %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched {
		t.Fatal("matched=false, want true (matchad men undertryckt)")
	}
	if len(out.Entities) != 0 {
		t.Fatalf("entities=%d, want 0", len(out.Entities))
	}
	// Inline saknar lagrad identitet (json:"-"): tomt är ärligt, ej läcka.
	if out.RuleID != "" || out.RuleRevision != 0 || out.SnapshotVersion != 0 {
		t.Fatalf("inline identitet ska vara tom: %+v", out)
	}
	if len(out.Competing) != 1 || !out.Competing[0].Winner {
		t.Fatalf("konkurrerande: %+v", out.Competing)
	}
	if len(out.Skipped) == 0 || out.Skipped[0].Code != transform.CodeRequiredMissing {
		t.Fatalf("precis skiporsak: %+v", out.Skipped)
	}
	if out.Skipped[0].Entity != "primary" || out.Skipped[0].Attribute != "missing" {
		t.Fatalf("skip målroll/attribut: %+v", out.Skipped[0])
	}
	if out.SkippedReason == "" {
		t.Fatal("skippedReason saknas (bakåtkompatibilitet)")
	}
	// Främmande tenant ger no-match (tenant-avgränsning), ej läcka.
	foreign := rule
	foreign.Match.Tenant = "other-tenant"
	rr = doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: foreign, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: raw}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("foreign %d: %s", rr.Code, rr.Body.String())
	}
	var fout previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &fout); err != nil {
		t.Fatal(err)
	}
	if fout.Matched {
		t.Fatalf("främmande tenant matchade: %+v", fout)
	}
}

// Målroller, removeAttributes och effektiv context exponeras per operation.
func TestEPreviewRemoveAttributesEffectiveContext(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: "partOf", Type: "generic-template", Tenant: "tenant-a"},
		Entities: []transform.Entity{{
			Name: "secondary", ID: "urn:ngsi-ld:Generic:thing-2", Type: "Generic",
			Context:          "https://example.invalid/ctx.jsonld",
			RemoveAttributes: []string{"stale"},
		}},
	}
	raw := json.RawMessage(`{"fromId":"thing-1","toId":"thing-2","tenant":"tenant-a","type":"partOf","revision":3,"changedAt":"2026-10-01T12:00:00Z",
	  "fromThingType":"generic-template","from":{"thingId":"thing-1"},"toThingType":"other","to":{"thingId":"thing-2"}}`)
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Relations, Message: raw}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d: %s", rr.Code, rr.Body.String())
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched || len(out.Entities) != 1 {
		t.Fatalf("svar: %+v", out)
	}
	ent := out.Entities[0]
	if ent.Operation != "remove" || len(ent.RemoveAttributes) != 1 || ent.RemoveAttributes[0] != "stale" {
		t.Fatalf("remove: %+v", ent)
	}
	if ent.EffectiveContext != "https://example.invalid/ctx.jsonld" || ent.Context != "https://example.invalid/ctx.jsonld" {
		t.Fatalf("context: %+v", ent)
	}
	// Utan explicit context faller effektiv context tillbaka till default.
	rule.Entities[0].Context = ""
	rr = doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Relations, Message: raw}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d", rr.Code)
	}
	var out2 previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out2); err != nil {
		t.Fatal(err)
	}
	if out2.Entities[0].EffectiveContext == "" || out2.Entities[0].Context != "" {
		t.Fatalf("default context: %+v", out2.Entities[0])
	}
}

// Klientfel vid felaktig exempeldata (400, ej 500) och ingen brokerkontakt
// vid preview (torr körning utan nätverk).
func TestEPreviewClientErrorNoBrokerContact(t *testing.T) {
	rule := ePreviewRule()
	// Ogiltigt kuvert → 400.
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: json.RawMessage(`{"tenant":"tenant-a"}`)}}, nil)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("kuvert: %d %s", rr.Code, rr.Body.String())
	}
	// Regel+exempel som ger motorfel (okänd målroll) → 400 med precis fel.
	bad := ePreviewRule()
	bad.Entities[0].Properties = []transform.Property{{Target: "label", Type: transform.TypeText, Value: "{{entities.missing.id}}", ObservedAt: transform.ObservedAtNone}}
	rr = doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: bad, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: ePreviewMessage()}}, nil)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "missing") {
		t.Fatalf("motorfel: %d %s", rr.Code, rr.Body.String())
	}
	// Giltig preview lyckas utan broker (ingen klient, inget nätverk).
	rr = doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: ePreviewMessage()}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("torr preview kräver broker: %d %s", rr.Code, rr.Body.String())
	}
}

// Versions-/variantstyrd preview: exakt match och wildcard via samma
// C1-vyer som produktion.
func TestEPreviewVersionVariant(t *testing.T) {
	rule := ePreviewRule()
	rule.Match.ThingTypeVersion = "2"
	rr := doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: ePreviewMessage()}}, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("preview %d", rr.Code)
	}
	var out previewResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !out.Matched {
		t.Fatalf("version 2 ska matcha: %+v", out)
	}
	rule.Match.ThingTypeVersion = "9"
	rr = doModel((&handler{}).previewModel, http.MethodPost, "", previewRequest{Rule: rule, Event: previewEvent{Kind: transform.EventThingsV1Values, Message: ePreviewMessage()}}, nil)
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Matched {
		t.Fatalf("version 9 ska ge no-match: %+v", out)
	}
}

// Backend-previewtester (G/I/J): offline utan HTTP/auth/broker, samma
// normalisering som produktion/preview.
func TestEPreviewTesterOffline(t *testing.T) {
	res, err := RunPreviewTest(context.Background(), PreviewTestInput{
		Rule: ePreviewRule(), Kind: transform.EventThingsV1Values, Message: ePreviewMessage(),
	})
	if err != nil {
		t.Fatalf("tester: %v", err)
	}
	if !res.Matched || len(res.Entities) != 1 || res.Observations != 1 {
		t.Fatalf("tester: %+v", res)
	}
	if res.Entities[0].Operation != "merge" || res.Entities[0].EffectiveContext == "" {
		t.Fatalf("tester entitet: %+v", res.Entities[0])
	}
	// Felaktig exempeldata ger klientfel utan brokerkontakt.
	if _, err := RunPreviewTest(context.Background(), PreviewTestInput{
		Rule: ePreviewRule(), Kind: transform.EventThingsV1Values, Message: json.RawMessage(`{"tenant":"x"}`),
	}); err == nil {
		t.Fatal("ogiltigt kuvert ska ge fel")
	}
}

// serialiserad NGSI-LD ger fel utan ett enda brokeranrop (hela batchen
// förvalideras). Giltig PATCH (delta) ≠ giltig create visas i motorn.
func TestEPreviewBatchValidatedBeforeIO(t *testing.T) {
	calls := 0
	mock := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: func(_ context.Context, _ types.Entity, _ map[string][]string) (*ngsild.CreateEntityResult, error) {
			calls++
			return &ngsild.CreateEntityResult{}, nil
		},
		MergeEntityFunc: func(_ context.Context, _ string, _ types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
			calls++
			return &ngsild.MergeEntityResult{}, nil
		},
		DeleteEntityFunc: func(_ context.Context, _ string) (*ngsild.DeleteEntityResult, error) {
			calls++
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	// Relativt ID är ogiltig NGSI-LD: valideras före I/O.
	bad := []transform.EntityWrite{{EntityID: "relative-id", TypeName: "Generic"}}
	if err := contextbroker.ApplyEntityWrites(context.Background(), mock, bad, ""); err == nil {
		t.Fatal("ogiltig batch ska nekas")
	}
	if calls != 0 {
		t.Fatalf("broker kontaktades trots ogiltig batch: %d", calls)
	}
}

// Samtidiga create (404/409-race) konvergerar via merge-or-create; delvis
// lyckad batch bevarar slutfört i felrapporten.
func TestEPreviewConcurrentCreateAndPartialBatch(t *testing.T) {
	// 404 → create → 409 → merge: exakt ett create-försök, sedan merge.
	var mu sync.Mutex
	merges, creates := 0, 0
	seen := map[string]bool{}
	mock := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: func(_ context.Context, e types.Entity, _ map[string][]string) (*ngsild.CreateEntityResult, error) {
			mu.Lock()
			defer mu.Unlock()
			creates++
			if seen[e.ID()] {
				return nil, ngsilderrors.ErrAlreadyExists
			}
			seen[e.ID()] = true
			return &ngsild.CreateEntityResult{}, nil
		},
		MergeEntityFunc: func(_ context.Context, id string, _ types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
			mu.Lock()
			defer mu.Unlock()
			// Första merge för saknad entitet → 404, senare → OK.
			if !seen[id] {
				return nil, ngsilderrors.ErrNotFound
			}
			merges++
			return &ngsild.MergeEntityResult{}, nil
		},
		DeleteEntityFunc: func(_ context.Context, _ string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	writes := []transform.EntityWrite{{
		EntityID: "urn:ngsi-ld:Generic:race", TypeName: "Generic",
		Props: []entities.EntityDecoratorFunc{},
	}}
	_ = writes
	// Använd verkliga props via motorn (ingen modellberoende specialkod).
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "generic-template"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:race", Type: "Generic",
			Properties: []transform.Property{{Target: "temp", Type: transform.TypeNumber, Const: 1.0}},
		}},
	}
	th := transform.Thing{ID: "thing-1", Type: "generic-template", Tenant: "t", Event: transform.EventThingsV1Values, Fields: map[string]any{}}
	engineWrites, err := transform.NewEngine([]transform.Rule{rule}).Transform(th)
	if err != nil || len(engineWrites) != 1 {
		t.Fatalf("fixture: %v %v", engineWrites, err)
	}
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = contextbroker.ApplyEntityWrites(context.Background(), mock, engineWrites, "")
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("race: %v", err)
		}
	}

	// Delvis lyckad batch: första merge OK, andra brokerfel → Completed=1.
	boom := errors.New("broker down")
	flaky := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: mock.CreateEntityFunc,
		MergeEntityFunc: func(ctx context.Context, id string, f types.EntityFragment, h map[string][]string) (*ngsild.MergeEntityResult, error) {
			if strings.HasSuffix(id, ":second") {
				return nil, boom
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		DeleteEntityFunc: mock.DeleteEntityFunc,
	}
	batch := []transform.EntityWrite{
		{EntityID: "urn:ngsi-ld:Generic:first", TypeName: "Generic", Props: engineWrites[0].Props},
		{EntityID: "urn:ngsi-ld:Generic:second", TypeName: "Generic", Props: engineWrites[0].Props},
	}
	// Första entiteten måste existera för merge (ingen 404-väg här).
	err = contextbroker.ApplyEntityWrites(context.Background(), flaky, batch, "")
	var batchErr *contextbroker.WriteBatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("batchfel: %v", err)
	}
	if batchErr.Completed != 1 || batchErr.Operation != "merge" {
		t.Fatalf("slutfört bevaras ej: %+v", batchErr)
	}
}
