package projection

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"go.opentelemetry.io/otel/trace"
)

// Generella namn i tester: aldrig standardmodellnamn.

// genericRelationsRule är en D-kompatibel generisk relationsregel (inga
// modellnamn): regeln valideras med D:s Validate och dess identitet bär
// planeringen. E bygger PlanFunc-closuren från aktuell regelversion;
// F planerar deterministiskt efter samma avtal.
func genericRelationsRule() transform.Rule {
	return transform.Rule{
		ID:       "rule-generic-1",
		Revision: 9,
		Match: transform.Match{
			Kind:     transform.KindThing,
			Event:    transform.EventThingsV1Relations,
			Relation: "contains",
			Type:     "generic-template",
		},
		Entities: []transform.Entity{{
			ID:   "urn:ngsi-ld:Generic:{{id}}",
			Type: "Generic",
			Properties: []transform.Property{{
				Target: "label", Type: transform.TypeText,
				Const: "generic", ObservedAt: transform.ObservedAtNone,
			}},
		}},
	}
}

func TestRebuildFromExportWithDCompatibleRule(t *testing.T) {
	rule := genericRelationsRule()
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatalf("generic rule must be D-valid: %v", err)
	}
	ruleRef := relations.RuleRef{ID: rule.ID, Revision: rule.Revision}
	planner := relations.ReferencePlanner(ruleRef)

	s := pgTestStore(t)
	exported := []relations.Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 4, FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2", Revision: 4, FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-2", ToID: "to-3", Revision: 2, FromKind: "kind-a", ToKind: "kind-b"},
	}

	// Ren planering först (utan broker-I/O): ExportTenant-underlag +
	// regeluppsättning (via planner) → PendingWrite.
	planned, err := relations.PlanRebuildFromExport(exported, "tenant-a", "contains", planner)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned) != 3 {
		t.Fatalf("planned=%d, want 3", len(planned))
	}
	for _, w := range planned {
		if w.RuleID != rule.ID || w.RuleRevision != rule.Revision {
			t.Fatalf("rule not carried: %+v", w)
		}
		for _, banned := range []string{"Tank", "Greenhouse", "Sewer", "Room", "AirQuality"} {
			if strings.Contains(string(w.Payload), banned) || strings.Contains(w.IdempotencyKey, banned) {
				t.Fatalf("write %+v contains model name", w)
			}
		}
	}

	// Persist via lagret (fortfarande ingen brokerkontakt — endast pending-kö).
	res, err := s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, planner)
	if err != nil {
		t.Fatal(err)
	}
	if res.Groups != 2 || res.Pending != 3 {
		t.Fatalf("rebuild=%+v, want 2 groups 3 pending", res)
	}
	// Idempotent: samma regel ger samma nycklar, inget nytt arbete.
	res, err = s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, planner)
	if err != nil || res.Pending != 0 {
		t.Fatalf("second rebuild=%+v err=%v, want 0 new", res, err)
	}
	// Projektionen är orörd av rebuild (endast arbete planerades).
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 0 {
		t.Fatalf("rebuild must not mutate projection (DB empty here): %v", got)
	}
	// Tenantfrämmande länkar i exporten filtreras strikt.
	leaked := append(append([]relations.Link(nil), exported...),
		relations.Link{Tenant: "tenant-b", Relation: "contains", FromID: "from-1", ToID: "to-9"})
	res, err = s.RebuildFromExport(t.Context(), "tenant-a", "contains", leaked, nil, planner)
	if err != nil || res.Pending != 0 {
		t.Fatalf("cross-tenant export must not add work: %+v %v", res, err)
	}
	count, _ := s.PendingCount(t.Context(), "tenant-a")
	if count != 3 {
		t.Fatalf("pending=%d, want 3", count)
	}
	// Leverans sker först via DrainOnce (broker-I/O utanför planeringen).
	completed, failed, err := s.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, func(context.Context, PendingItem) error { return nil })
	if err != nil || completed != 3 || failed != 0 {
		t.Fatalf("drain=%d/%d err=%v", completed, failed, err)
	}
}

func TestRebuildFromExportMatchesLiveRebuild(t *testing.T) {
	s := pgTestStore(t)
	ruleRef := relations.RuleRef{ID: "rule-1", Revision: 9}
	planner := relations.ReferencePlanner(ruleRef)

	// Live-vägen: BootstrapUpsert + Rebuild (DB-läst snapshot).
	if _, _, err := s.BootstrapUpsert(t.Context(), []relations.Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 4, FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2", Revision: 4, FromKind: "kind-a", ToKind: "kind-b"},
	}); err != nil {
		t.Fatal(err)
	}
	live, err := s.Rebuild(t.Context(), "tenant-a", "contains", nil, planner, 0)
	if err != nil {
		t.Fatal(err)
	}
	exported, err := s.ExportTenant(t.Context(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	// Export-vägen över samma underlag ger samma mängd arbete.
	if _, err := s.BootstrapPrune(t.Context(), "tenant-a", relations.KeysForPrune(exported), time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	fromExport, err := s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, planner)
	if err != nil {
		t.Fatal(err)
	}
	// Live-Rebuild lade 2 (samma nycklar) — exportvägen är idempotent mot dem.
	if live.Pending != 2 || fromExport.Pending != 0 {
		t.Fatalf("live=%+v export=%+v, want 2 then 0 (same keys)", live, fromExport)
	}
}

func TestRebuildFromExportTracePreserved(t *testing.T) {
	s := pgTestStore(t)
	ruleRef := relations.RuleRef{ID: "rule-1", Revision: 9}

	// Deterministisk W3C-carrier utan global provider-swap (suite-oberoende):
	// en giltig fjärr-SpansContext injiceras till carrier, lagras via
	// RebuildFromExport och måste komma tillbaka med samma TraceID.
	// Span-länkningen vid DrainOnce ägs av TestProjectionTraceSurvivesRestart
	// (global SDK-provider kan endast ägas av ett test i taget).
	tid, err := trace.TraceIDFromHex("d6adf5e5f40bce18f3fc85f53e3ece85")
	if err != nil {
		t.Fatal(err)
	}
	sid, err := trace.SpanIDFromHex("2c04d263805fb0ac")
	if err != nil {
		t.Fatal(err)
	}
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), remote)
	storedCarrier := CaptureTrace(ctx)
	if len(storedCarrier) == 0 {
		t.Fatal("no carrier captured from valid span context")
	}

	exported := []relations.Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 1, FromKind: "kind-a", ToKind: "kind-b"},
	}
	if _, err := s.RebuildFromExport(ctx, "tenant-a", "contains", exported, storedCarrier, relations.ReferencePlanner(ruleRef)); err != nil {
		t.Fatal(err)
	}
	items, err := s.ClaimPending(t.Context(), "tenant-a", 10, time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("claim=%d err=%v", len(items), err)
	}
	if len(items[0].Trace) == 0 {
		t.Fatal("stored trace lost")
	}
	got := items[0].Trace["traceparent"]
	want := storedCarrier["traceparent"]
	if got == "" || got != want {
		t.Fatalf("traceparent=%q, want %q", got, want)
	}
	if !strings.Contains(got, tid.String()) {
		t.Fatalf("traceparent %q lacks trace id %s", got, tid)
	}
	for _, item := range items {
		if err := s.MarkDone(t.Context(), item.ID); err != nil {
			t.Fatal(err)
		}
	}
	// Ny regelrevision ger nytt arbete med samma bevarade trace; DrainOnce
	// levererar (länkning av lagrad context sker i linkSpan utan fel).
	if _, err := s.RebuildFromExport(ctx, "tenant-a", "contains", exported, storedCarrier, relations.ReferencePlanner(relations.RuleRef{ID: "rule-1", Revision: 10})); err != nil {
		t.Fatal(err)
	}
	completed, failed, err := s.DrainOnce(t.Context(), "tenant-a", 10, time.Minute, func(context.Context, PendingItem) error { return nil })
	if err != nil || completed != 1 || failed != 0 {
		t.Fatalf("drain=%d/%d err=%v", completed, failed, err)
	}
}

func TestRebuildFromExportValidation(t *testing.T) {
	s := pgTestStore(t)
	planner := relations.ReferencePlanner(relations.RuleRef{ID: "rule-1", Revision: 1})
	exported := []relations.Link{{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1"}}
	if _, err := s.RebuildFromExport(t.Context(), "", "contains", exported, nil, planner); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant: %v", err)
	}
	if _, err := s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil planner: %v", err)
	}
	broken := relations.PlanFunc(func(relations.Snapshot) ([]relations.PendingWrite, error) {
		return nil, errors.New("planner exploded")
	})
	if _, err := s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, broken); err == nil {
		t.Fatal("planner error must propagate without partial persist")
	}
	count, _ := s.PendingCount(t.Context(), "tenant-a")
	if count != 0 {
		t.Fatalf("failed rebuild persisted %d rows", count)
	}
	s.Close()
	if _, err := s.RebuildFromExport(t.Context(), "tenant-a", "contains", exported, nil, planner); !errors.Is(err, ErrNotFound) {
		t.Fatalf("closed store: %v", err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if _, err := s.RebuildFromExport(canceled, "tenant-a", "contains", exported, nil, planner); !errors.Is(err, ErrNotFound) && !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation/closed: %v", err)
	}
}

func TestBootstrapJSONWatermarkWithConcurrentLive(t *testing.T) {
	s := pgTestStore(t)

	// B-underlag som JSON (neutralt Link-format): export + per-mål-underlag
	// aggregerat av anroparen.
	exported := []relations.Link{
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-1", Revision: 1, FromKind: "kind-a", ToKind: "kind-b"},
		{Tenant: "tenant-a", Relation: "contains", FromID: "from-1", ToID: "to-2", Revision: 1, FromKind: "kind-a", ToKind: "kind-b"},
	}
	raw, err := relations.LinksToJSON(exported)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := relations.LinksFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC()
	if _, _, err := s.BootstrapUpsert(t.Context(), parsed); err != nil {
		t.Fatal(err)
	}
	// Samtidig live-skrivning under bootstrap (snapshot känner den inte).
	planner := relations.ReferencePlanner(relations.RuleRef{ID: "rule-1", Revision: 1})
	live := setChange("tenant-a", "from-9", "to-9", 1)
	if _, err := s.ApplyChange(t.Context(), live, nil, planner); err != nil {
		t.Fatal(err)
	}
	// Prune med snapshot utan live-länken: den överlever via watermark.
	pruned, err := s.BootstrapPrune(t.Context(), "tenant-a", relations.KeysForPrune(parsed), start)
	if err != nil {
		t.Fatal(err)
	}
	if pruned != 0 {
		t.Fatalf("pruned=%d, concurrent live link must survive", pruned)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-9"); len(got) != 1 {
		t.Fatalf("live link lost: %v", got)
	}
	if got := outgoingToIDs(t, s, "tenant-a", "contains", "from-1"); len(got) != 2 {
		t.Fatalf("bootstrap links lost: %v", got)
	}
}

func TestCaptureTraceNilWithoutSpan(t *testing.T) {
	if got := CaptureTrace(t.Context()); len(got) != 0 {
		t.Fatalf("traceless ctx must give nil, got %v", got)
	}
}
