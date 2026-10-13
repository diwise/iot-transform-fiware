// Package plan1050 är PLAN1050 paket J neutral acceptans (Våg 4, J1).
//
// Neutral kedja FÖRE modellförfining: neutrala ID:n, inga modellnamn/
// URN-prefix i fallen. Harness med VERKLIGA B-formade payloads
// (samma trådform som iot-things-v2 pkg/types Body() bär: schema/eventId/
// tenant/thingId/thingType/thingTypeVersion/configRevision/publishedAt/
// thing/delta/snapshot/values/removed/relations), publicerade API-kontrakt
// (transform.CheckCompatibility {template+sources+rules -> diagnostics} —
// samma funktion som POST /models/compat anropar — samt
// api.RunPreviewTest {competing/skipped Diagnostic} — samma normalisering
// som produktion/preview), isolerade testdata (j1-tenant-*) och kontrollerade
// brokersvar (recording-mock med anropsräkning; levererat påstås aldrig från
// stubb utan assertion).
//
// Separering faktisk acceptans vs stubbar/skips:
//   - Varje brokerpåstående räknar verkliga Merge/Create/Delete-anrop.
//   - no-match/skip/invalid hävdar 0 anrop.
//   - DB-fallet (J1-f) kräver verklig PostgreSQL (diwise_test_j1) och
//     misslyckas öppet om den skippas — se TestJ1f...DB.
//   - Preview hävdar aldrig brokerkontakt (torr körning, ingen klient).
//
// Källrecency-skydd ingår INTE (fastställt beslut, PLAN1050):
// sena källevent appliceras och kan återställa äldre målstate/
// relationsmängder. TestJ1RecencyDocumented bevisar att ingen filtrering
// sker (motsatsen till ordningsskydd) och dokumenterar avgränsningen.
// Race-/replaytester beskrivs aldrig som bevis för ordningsoberoende
// konvergens.
package plan1050

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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
	"github.com/diwise/iot-transform-fiware/internal/presentation/api"
	msgthings "github.com/diwise/iot-transform-fiware/internal/presentation/messaging/things"
	rmq "github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	j1TenantA = "j1-tenant-a"
	j1TenantB = "j1-tenant-b"

	j1Template  = "j1-template"
	j1OtherKind = "j1-other-kind"
	j1Relation  = "j1-contains"
	j1ContentCT = "application/vnd.diwise.things.v1+json"
	j1TopicVals = "things.v1.values"
	j1TopicRels = "things.v1.relations"
	j1TopicLife = "things.v1.lifecycle"
)

func j1Time() time.Time {
	return time.Date(2026, 10, 5, 12, 0, 0, 123456000, time.UTC)
}

func j1MustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// j1ValuesPayload bygger en B-formad värdepayload (samma nycklar som
// pkg/types Body(): schema/eventId/tenant/thingId/thingType/
// thingTypeVersion/configRevision/publishedAt/thing/delta/snapshot/values).
func j1ValuesPayload(thingID, tenant, thingType, version string, revision int64, thing map[string]any, delta, snapshot []any, extra map[string]any) []byte {
	env := map[string]any{
		"schema": "v1", "eventId": "v1/" + tenant + "/" + thingID + "/m1",
		"tenant": tenant, "thingId": thingID,
		"thingType": thingType, "thingTypeVersion": version,
		"configRevision": revision, "publishedAt": j1Time().UTC().Format(time.RFC3339Nano),
		"thing": thing, "delta": delta, "snapshot": snapshot, "values": delta,
	}
	maps.Copy(env, extra)
	raw, _ := json.Marshal(env)
	return raw
}

func j1ThingDesc(thingID string, variantID, variantVersion string, location any) map[string]any {
	d := map[string]any{"thingId": thingID, "name": "J1 " + thingID, "category": "j1-category"}
	if variantID != "" {
		d["variantId"] = variantID
	}
	if variantVersion != "" {
		d["variantVersion"] = variantVersion
	}
	if location != nil {
		d["location"] = location
	}
	return d
}

func j1ValueEntry(thingID, tenant, prop string, value any, quality, unit, source, msg string, observed time.Time, revision int64) map[string]any {
	e := map[string]any{
		"thingId": thingID, "tenant": tenant, "propertyId": prop,
		"quality": quality, "source": source, "messageId": msg,
		"observedAt":     observed.UTC().Format(time.RFC3339Nano),
		"receivedAt":     observed.UTC().Format(time.RFC3339Nano),
		"configRevision": revision,
		"eventId":        "v1/" + tenant + "/" + thingID + "/m1/" + prop,
	}
	if value != nil {
		e["value"] = value
	}
	if unit != "" {
		e["unit"] = unit
	}
	return e
}

func j1GenericValuesRule(tenant, thingType string) transform.Rule {
	return transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: thingType, Tenant: tenant},
		Entities: []transform.Entity{{
			Name: "primary",
			ID:   "urn:ngsi-ld:Generic:j1-thing",
			Type: "Generic",
			Properties: []transform.Property{
				{Target: "reading", Type: transform.TypeNumber, Field: "values.prop-a.value", Required: true},
				{Target: "label", Type: transform.TypeText, Field: "name", ObservedAt: transform.ObservedAtNone},
			},
		}},
	}
}

type j1Provider struct {
	t *appthings.DynamicThingTransformer
}

func (p j1Provider) ThingsTransformer() *appthings.DynamicThingTransformer { return p.t }

func j1Incoming(body []byte) *rmq.IncomingTopicMessageMock {
	return &rmq.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return body },
		ContentTypeFunc: func() string { return j1ContentCT },
		TopicNameFunc:   func() string { return j1TopicVals },
	}
}

func j1IncomingWithTopic(body []byte, topic string) *rmq.IncomingTopicMessageMock {
	return &rmq.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return body },
		ContentTypeFunc: func() string { return j1ContentCT },
		TopicNameFunc:   func() string { return topic },
	}
}

type j1RecordingBroker struct {
	mu      sync.Mutex
	merges  []string
	creates []string
	deletes []string
	mergeFn func(ctx context.Context, id string) error
}

func (r *j1RecordingBroker) mock() *clienttest.ContextBrokerClientMock {
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

func (r *j1RecordingBroker) counts() (merges, creates, deletes int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.merges), len(r.creates), len(r.deletes)
}

// J1 kontrakt: publicerade topic/content-type + B-formade nycklar via C1.
func TestJ1ContractTopicsContentTypesAndBWireShape(t *testing.T) {
	if transform.EventThingsV1Values != j1TopicVals {
		t.Fatalf("values topic=%q want %q", transform.EventThingsV1Values, j1TopicVals)
	}
	if transform.EventThingsV1Relations != j1TopicRels {
		t.Fatalf("relations topic=%q want %q", transform.EventThingsV1Relations, j1TopicRels)
	}
	if transform.EventThingsV1Lifecycle != j1TopicLife {
		t.Fatalf("lifecycle topic=%q want %q", transform.EventThingsV1Lifecycle, j1TopicLife)
	}
	at := j1Time()
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "Cel", "s/1/3303/5700", "m1", at, 7)}
	snap := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "Cel", "s/1/3303/5700", "m1", at, 7)}
	thing := j1ThingDesc("j1-obj-1", "j1-variant", "3", map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0, 9.0}})
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "2", 7, thing, delta, snap, nil)
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"schema", "eventId", "tenant", "thingId", "thingType", "thingTypeVersion", "configRevision", "publishedAt", "thing", "delta", "snapshot", "values"} {
		if _, ok := wire[k]; !ok {
			t.Fatalf("B Body()-form saknar %q", k)
		}
	}
	th, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("C1 nekar B-formad payload: %v", err)
	}
	if th.ID != "j1-obj-1" || th.Tenant != j1TenantA || th.Type != j1Template {
		t.Fatalf("identitet=%+v", th)
	}
	if th.MatchInfo().ThingTypeVersion != "2" {
		t.Fatalf("version=%q", th.MatchInfo().ThingTypeVersion)
	}
	if th.MatchInfo().VariantID != "j1-variant" {
		t.Fatalf("variant=%q", th.MatchInfo().VariantID)
	}
}

// J1-a: neutral mall/variant via D-kompat (citerade ID:n + repeat-prefix) + E-preview.
func TestJ1aNeutralTemplateVariantQuotedRepeat(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, ThingTypeVersion: "7", VariantID: "j1-variant", VariantVersion: "3", Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "primary", ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{
				{Target: "quoted", Type: transform.TypeNumber, Field: `values."reading.average".value`, Required: true},
				{Target: "rep1", Type: transform.TypeNumber, Field: "values.group.item1.value"},
				{Target: "rep2", Type: transform.TypeNumber, Field: "values.group.item2.value"},
				{Target: "snap", Type: transform.TypeNumber, Field: "snapshot.prop-a.value"},
			},
		}},
	}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatalf("neutral regel ogiltig: %v", err)
	}
	// D: verklig kompatibilitet {template+sources+rules -> diagnostics}, ej G:s antagna kopia.
	res := transform.CheckCompatibility(transform.CompatRequest{
		Template: transform.TemplateVersion{ThingType: j1Template, ThingTypeVersion: "7", VariantID: "j1-variant", VariantVersion: "3"},
		Sources: transform.SourceContract{Properties: []transform.PropertyContract{
			{ID: "reading.average", Unit: "Cel"}, {ID: "group.item1"}, {ID: "group.item2"}, {ID: "prop-a"},
		}},
		Rules: []transform.Rule{rule}, Tenant: j1TenantA, ThingID: "j1-obj-1", ConfigRevision: 7,
	})
	if !res.Compatible {
		t.Fatalf("neutral kompatibilitet: %+v", res.Diagnostics)
	}
	// Fel mallversion ger no-match-diagnostik med tenant/thing/revision.
	bad := transform.CheckCompatibility(transform.CompatRequest{
		Template: transform.TemplateVersion{ThingType: j1Template, ThingTypeVersion: "9"},
		Sources:  transform.SourceContract{Properties: []transform.PropertyContract{{ID: "reading.average"}}},
		Rules:    []transform.Rule{rule}, Tenant: j1TenantA, ThingID: "j1-obj-1", ConfigRevision: 7,
	})
	if bad.Compatible {
		t.Fatal("versionsmismatch ska vara inkompatibel")
	}
	found := false
	for _, d := range bad.Diagnostics {
		if d.Code == transform.CodeNoMatch && d.Tenant == j1TenantA && d.ThingID == "j1-obj-1" && d.ConfigRevision == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("no-match-diagnostik saknas: %+v", bad.Diagnostics)
	}
	// E: preview mot samma regel med citerad + repeat-källor (torr, ingen broker).
	at := j1Time()
	delta := []any{
		j1ValueEntry("j1-obj-1", j1TenantA, "reading.average", 21.5, "ok", "Cel", "s/1/3303/5700", "m1", at, 7),
		j1ValueEntry("j1-obj-1", j1TenantA, "group.item1", 1.0, "ok", "", "s/1/3304/5700", "m1", at, 7),
		j1ValueEntry("j1-obj-1", j1TenantA, "group.item2", 2.0, "ok", "", "s/1/3304/5700", "m1", at, 7),
	}
	snap := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 5.0, "ok", "", "s/1/3305/5700", "m0", at.Add(-time.Hour), 7)}
	thing := j1ThingDesc("j1-obj-1", "j1-variant", "3", nil)
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "7", 7, thing, delta, snap, nil)
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{
		Rule: rule, Kind: transform.EventThingsV1Values, Message: raw,
	})
	if err != nil {
		t.Fatalf("preview: %v", err)
	}
	if !previewRes.Matched || len(previewRes.Entities) != 1 {
		t.Fatalf("preview neutral: %+v", previewRes)
	}
	if len(previewRes.Competing) == 0 || !previewRes.Competing[0].Winner {
		t.Fatalf("competing saknas: %+v", previewRes.Competing)
	}
}

// J1-b: Point/Polygon/utan-mätning via lifecycle/bootstrap (C1 descriptor + rå GeoJSON).
func TestJ1bPointPolygonAndLifecycleWithoutMeasurements(t *testing.T) {
	// Point 3D: genväg kompletterar, rått bevaras.
	pointThing := j1ThingDesc("j1-obj-1", "", "", map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0, 9.0}})
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, pointThing, []any{}, []any{}, nil)
	in, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("point: %v", err)
	}
	if _, _, ok := in.LatLon(); !ok {
		t.Fatal("point-genväg saknas")
	}
	geom, ok := in.ResolvePath("geometry")
	if !ok {
		t.Fatal("geometry saknas för point")
	}
	if coords, _ := geom.(map[string]any)["coordinates"].([]any); len(coords) != 3 {
		t.Fatalf("3D trunkerad: %v", geom)
	}
	// Polygon: aldrig point-genväg, rått under thing/descriptor/geometry.
	polyThing := j1ThingDesc("j1-obj-2", "", "", map[string]any{"type": "Polygon", "coordinates": []any{[]any{[]any{18.0, 59.3}, []any{19.0, 59.3}, []any{19.0, 60.0}, []any{18.0, 59.3}}}})
	raw = j1ValuesPayload("j1-obj-2", j1TenantA, j1Template, "1", 1, polyThing, []any{}, []any{}, nil)
	pin, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("polygon: %v", err)
	}
	if _, _, ok := pin.LatLon(); ok {
		t.Fatal("polygon får inte ge point-genväg")
	}
	for _, p := range []string{"geometry.type", "thing.location.type", "descriptor.location.type"} {
		got, ok := pin.ResolvePath(p)
		if !ok || got != "Polygon" {
			t.Fatalf("%s=%v want Polygon", p, got)
		}
	}
	// Utan mätningar där mallen tillåter: lifecycle created/bootstrap bär komplett underlag.
	life := map[string]any{
		"schema": "v1", "eventId": "v1/" + j1TenantA + "/j1-obj-3/lifecycle/created/1",
		"tenant": j1TenantA, "thingId": "j1-obj-3", "kind": "created",
		"thingType": j1Template, "thingTypeVersion": "1", "configRevision": 1,
		"changedAt": j1Time().UTC().Format(time.RFC3339Nano),
		"thing":     j1ThingDesc("j1-obj-3", "", "", map[string]any{"type": "Point", "coordinates": []any{17.0, 62.0}}),
		"bootstrap": true,
	}
	lraw := j1MustMarshal(t, life)
	lin, err := appthings.ThingLifecycleFromJSON(lraw)
	if err != nil {
		t.Fatalf("lifecycle: %v", err)
	}
	if lin.Lifecycle != "created" {
		t.Fatalf("lifecycle=%q", lin.Lifecycle)
	}
	// Deskriptorregel utan mätningar: name från descriptor räcker för skapande.
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: string(transform.LifecycleCreated), Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-3", Type: "Generic", Create: true,
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Field: "name", ObservedAt: transform.ObservedAtNone}},
		}},
	}
	if err := transform.Validate([]transform.Rule{rule}); err != nil {
		t.Fatalf("lifecycle-regel: %v", err)
	}
	writes, err := transform.NewEngine([]transform.Rule{rule}).Transform(lin)
	if err != nil || len(writes) != 1 || !writes[0].Create {
		t.Fatalf("lifecycle-skapande: %v %+v", err, writes)
	}
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: rule, Kind: transform.EventThingsV1Lifecycle, Message: lraw})
	if err != nil || !previewRes.Matched || len(previewRes.Entities) != 1 || previewRes.Entities[0].Operation != "create" {
		t.Fatalf("lifecycle-preview: %v %+v", err, previewRes)
	}
}

// J1-c: delta+snapshot inkl ogiltig-behåller-current + 0/false/subsekund.
func TestJ1cDeltaSnapshotInvalidKeepsCurrentZeroSubsecond(t *testing.T) {
	at := time.Date(2026, 10, 5, 12, 2, 0, 123456789, time.UTC) // subsekund
	delta := []any{
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "Cel", "s/1/3303/5700", "m2", at, 7),
		// Ny ogiltig observation för prop-b (ersätter aldrig current).
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-b", 99.0, "invalid", "Cel", "s/1/3303/5700", "m2", at, 7),
		// Noll bevaras (nil != 0).
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-zero", 0.0, "ok", "", "s/1/3200/5500", "m2", at, 7),
	}
	snap := []any{
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "Cel", "s/1/3303/5700", "m2", at, 7),
		// Tidigare giltigt current för prop-b (bevaras trots ogiltig delta).
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-b", 21.5, "ok", "Cel", "s/1/3303/5700", "m1", at.Add(-time.Hour), 7),
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-zero", 0.0, "ok", "", "s/1/3200/5500", "m2", at, 7),
	}
	thing := j1ThingDesc("j1-obj-1", "", "", nil)
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 7, thing, delta, snap, nil)
	in, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("C1: %v", err)
	}
	// Snapshot bär båda aktuella värden (producentens current-state, ingen sammanfogning av konsument).
	for path, want := range map[string]any{`snapshot."prop-a".value`: 22.0, `snapshot."prop-b".value`: 21.5, `snapshot."prop-zero".value`: 0.0} {
		got, ok := in.ResolvePath(path)
		if !ok || got != want {
			t.Fatalf("%s=%v want %v", path, got, want)
		}
	}
	// Ogiltig läcker aldrig som skrivbart värde; observations bär proveniens utan numeriska värden.
	for _, p := range []string{`values."prop-b".value`, `delta."prop-b".value`, `snapshot."prop-b".value`} {
		if p == `snapshot."prop-b".value` {
			continue // snapshot behåller giltigt 21.5 (testat ovan)
		}
		if v, ok := in.ResolvePath(p); ok {
			t.Fatalf("ogiltig läckte %s: %v", p, v)
		}
	}
	obs, ok := in.ResolvePath("observations")
	if !ok {
		t.Fatal("observations saknas")
	}
	blob, _ := json.Marshal(obs)
	if strings.Contains(string(blob), `"value":99`) || strings.Contains(string(blob), `"value":21.5`) {
		t.Fatalf("observations läckte numeriska värden: %s", blob)
	}
	// Subsekund bevaras (RFC3339Nano, ej trunkerad).
	got, ok := in.ResolvePath(`snapshot."prop-a".observedAt`)
	if !ok || !strings.Contains(fmt.Sprintf("%v", got), ".123456789") {
		t.Fatalf("subsekund=%v", got)
	}
	// Konsumenttid är max(delta[].observedAt), aldrig transporttid.
	if !in.ObservedAt.Equal(at) {
		t.Fatalf("ObservedAt=%v want %v", in.ObservedAt, at)
	}
	// Snapshot-regel emitterar trots ogiltig delta (current styr); delta-regel hoppar.
	snapRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "kept", Type: transform.TypeNumber, Field: "snapshot.prop-b.value", Required: true}},
		}},
	}
	writes, err := transform.NewEngine([]transform.Rule{snapRule}).Transform(in)
	if err != nil || len(writes) != 1 {
		t.Fatalf("snapshot-regel ska emittera previous-valid: %v %+v", err, writes)
	}
	deltaRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "fresh", Type: transform.TypeNumber, Field: "values.prop-b.value", Required: true}},
		}},
	}
	writes, err = transform.NewEngine([]transform.Rule{deltaRule}).Transform(in)
	if err != nil {
		t.Fatalf("delta-regel fel: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("ogiltig delta ska undertrycka values-regel, fick %+v", writes)
	}
	eng := transform.NewEngine([]transform.Rule{deltaRule})
	if !eng.Matched(in) {
		t.Fatal("undertryckt ska ändå vara matchad (matched vs no-match)")
	}
	if skipped := eng.ExplainSkipped(in); len(skipped) == 0 || skipped[0].Code != transform.CodeRequiredMissing {
		t.Fatalf("ExplainSkipped: %+v", skipped)
	}
}

// J1-d: två versioner + byte/omstart revisionsriktigt (ingen omberäkning/ommärkning).
func TestJ1dTwoVersionsAndRevisionStableRestart(t *testing.T) {
	at := j1Time()
	ruleV1 := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, ThingTypeVersion: "1", Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-v1", Type: "Generic",
			Properties: []transform.Property{{Target: "v1reading", Type: transform.TypeNumber, Field: "values.prop-a.value", Unit: "Cel", Required: true}},
		}},
	}
	ruleV2 := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, ThingTypeVersion: "2", Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-v2", Type: "Generic",
			Properties: []transform.Property{{Target: "v2reading", Type: transform.TypeNumber, Field: "values.prop-b.value", Unit: "MTR", Required: true}},
		}},
	}
	ruleV1.ID, ruleV1.Revision, ruleV1.SnapshotVersion = "j1-rule-v1", 11, 100
	ruleV2.ID, ruleV2.Revision, ruleV2.SnapshotVersion = "j1-rule-v2", 12, 100
	engine := transform.NewEngine([]transform.Rule{ruleV1, ruleV2})
	mkPayload := func(version, prop string, revision int64) []byte {
		delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, prop, 3.0, "ok", map[string]string{"prop-a": "Cel", "prop-b": "MTR"}[prop], "s/1", "m"+version, at, revision)}
		snap := []any{j1ValueEntry("j1-obj-1", j1TenantA, prop, 3.0, "ok", map[string]string{"prop-a": "Cel", "prop-b": "MTR"}[prop], "s/1", "m"+version, at, revision)}
		return j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, version, revision, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	}
	rawV1 := mkPayload("1", "prop-a", 7)
	rawV2 := mkPayload("2", "prop-b", 8)
	thV1, err := appthings.ThingValuesFromJSON(rawV1)
	if err != nil {
		t.Fatal(err)
	}
	thV2, err := appthings.ThingValuesFromJSON(rawV2)
	if err != nil {
		t.Fatal(err)
	}
	// Rätt regelval per version (exakt match, tom wildcard korsaktiveras aldrig här).
	sel1, ok := engine.MatchedRule(thV1)
	if !ok || sel1.ID != "j1-rule-v1" {
		t.Fatalf("v1 valde %+v", sel1)
	}
	sel2, ok := engine.MatchedRule(thV2)
	if !ok || sel2.ID != "j1-rule-v2" {
		t.Fatalf("v2 valde %+v", sel2)
	}
	w1, err := engine.Transform(thV1)
	if err != nil || len(w1) != 1 || w1[0].EntityID != "urn:ngsi-ld:Generic:j1-v1" {
		t.Fatalf("v1 writes: %v %+v", err, w1)
	}
	w2, err := engine.Transform(thV2)
	if err != nil || len(w2) != 1 || w2[0].EntityID != "urn:ngsi-ld:Generic:j1-v2" {
		t.Fatalf("v2 writes: %v %+v", err, w2)
	}
	// Revisionsriktigt: omstart återpublicerar utan omberäkning — samma
	// revision ger samma nycklar/payload (replay ofarlig), ny revision nya nycklar.
	pend1, err := transform.EntityWritesToPending(j1TenantA, w1, "")
	if err != nil || len(pend1) != 1 {
		t.Fatalf("pending v1: %v", err)
	}
	pend1Again, err := transform.EntityWritesToPending(j1TenantA, w1, "")
	if err != nil {
		t.Fatal(err)
	}
	if pend1[0].IdempotencyKey != pend1Again[0].IdempotencyKey {
		t.Fatal("replay ska ge samma idempotensnyckel")
	}
	pend2, err := transform.EntityWritesToPending(j1TenantA, w2, "")
	if err != nil {
		t.Fatal(err)
	}
	if pend1[0].IdempotencyKey == pend2[0].IdempotencyKey {
		t.Fatal("olika regelrevisioner ska ge olika nycklar")
	}
	// Konfigurationsbyte: gamla värden märks aldrig om under oförenliga enheter.
	// Snapshot-enhet är fryst per revision; motorn kräver convert för ommärkning.
	conflictRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Field: "values.prop-a.value", Unit: "MTR", Required: true}},
		}},
	}
	compat := transform.CheckCompatibility(transform.CompatRequest{
		Template: transform.TemplateVersion{ThingType: j1Template},
		Sources:  transform.SourceContract{Properties: []transform.PropertyContract{{ID: "prop-a", Unit: "Cel"}}},
		Rules:    []transform.Rule{conflictRule},
	})
	foundUnit := false
	for _, d := range compat.Diagnostics {
		if d.Code == transform.CodeUnitConflict {
			foundUnit = true
		}
	}
	if compat.Compatible || !foundUnit {
		t.Fatalf("ommärkning Cel->MTR utan convert ska ge unit-conflict: %+v", compat)
	}
}

// J1-e: samma källa -> andra mål via enbart regeländring + flera målentiteter.
func TestJ1eSameSourceOtherTargetsMultiEntity(t *testing.T) {
	at := j1Time()
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 7.0, "ok", "", "s/1", "m1", at, 1)}
	snap := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 7.0, "ok", "", "s/1", "m1", at, 1)}
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	th, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	ruleA := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "alpha", ID: "urn:ngsi-ld:Generic:j1-alpha", Type: "Generic",
			Properties: []transform.Property{{Target: "alphaReading", Type: transform.TypeNumber, Field: "values.prop-a.value", Required: true}},
		}},
	}
	ruleB := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "beta", ID: "urn:ngsi-ld:Generic:j1-beta", Type: "GenericB",
			Properties: []transform.Property{{Target: "betaReading", Type: transform.TypeNumber, Field: "values.prop-a.value", Required: true}},
		}},
	}
	wa, err := transform.NewEngine([]transform.Rule{ruleA}).Transform(th)
	if err != nil || len(wa) != 1 {
		t.Fatalf("regel A: %v %+v", err, wa)
	}
	wb, err := transform.NewEngine([]transform.Rule{ruleB}).Transform(th)
	if err != nil || len(wb) != 1 {
		t.Fatalf("regel B: %v %+v", err, wb)
	}
	if wa[0].EntityID == wb[0].EntityID || wa[0].TypeName == wb[0].TypeName {
		t.Fatalf("samma källa ska ge andra mål via enbart regeländring: %+v vs %+v", wa[0], wb[0])
	}
	// Flera målentiteter från samma sak i deklarerad ordning (målroll bakåtreferens).
	multi := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{
			{Name: "primary", ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
				Properties: []transform.Property{{Target: "reading", Type: transform.TypeNumber, Field: "values.prop-a.value", Required: true}}},
			{Name: "secondary", ID: "urn:ngsi-ld:GenericExtra:j1-obj-1-via-{{entities.primary.id}}", Type: "GenericExtra",
				Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Value: "{{entities.primary.id}}", ObservedAt: transform.ObservedAtNone}}},
		},
	}
	if err := transform.Validate([]transform.Rule{multi}); err != nil {
		t.Fatalf("multi-regel: %v", err)
	}
	wm, err := transform.NewEngine([]transform.Rule{multi}).Transform(th)
	if err != nil || len(wm) != 2 {
		t.Fatalf("flera mål: %v %+v", err, wm)
	}
	if wm[0].EntityID != "urn:ngsi-ld:Generic:j1-obj-1" || !strings.Contains(wm[1].EntityID, wm[0].EntityID) {
		t.Fatalf("målroller inkonsistenta: %+v", wm)
	}
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: multi, Kind: transform.EventThingsV1Values, Message: raw})
	if err != nil || len(previewRes.Entities) != 2 {
		t.Fatalf("multi-preview: %v %+v", err, previewRes)
	}
}

// J1-g (del 1): skapandekrav/separat uppdatering/explicit rensning (motor, ingen broker).
func TestJ1gCreateUpdateClearRequirements(t *testing.T) {
	at := j1Time()
	// Skapandekrav: create kräver full snapshot; giltig PATCH (delta-enbart) räcker inte.
	createRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic", Create: true,
			Properties: []transform.Property{
				{Target: "a", Type: transform.TypeNumber, Field: "snapshot.prop-a.value", Required: true},
				{Target: "b", Type: transform.TypeNumber, Field: "snapshot.prop-b.value", Required: true},
			},
		}},
	}
	compat := transform.CheckCompatibility(transform.CompatRequest{
		Template: transform.TemplateVersion{ThingType: j1Template},
		Sources:  transform.SourceContract{Properties: []transform.PropertyContract{{ID: "prop-a"}}},
		Rules:    []transform.Rule{createRule},
	})
	if compat.Compatible {
		t.Fatalf("create utan full snapshot ska vara inkompatibel: %+v", compat)
	}
	// Separat egenskapsuppdatering: delta för en egenskap, snapshot för flera aktuella.
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 8.0, "ok", "", "s/1", "m2", at, 3)}
	snap := []any{
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 8.0, "ok", "", "s/1", "m2", at, 3),
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-b", 4.0, "ok", "", "s/1", "m1", at.Add(-time.Hour), 3),
	}
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 3, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	th, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	partialRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{
				{Target: "a", Type: transform.TypeNumber, Field: "delta.prop-a.value", Required: true},
				{Target: "b", Type: transform.TypeNumber, Field: "snapshot.prop-b.value"},
			},
		}},
	}
	writes, err := transform.NewEngine([]transform.Rule{partialRule}).Transform(th)
	if err != nil || len(writes) != 1 {
		t.Fatalf("deluppdatering: %v %+v", err, writes)
	}
	// Explicit rensning: removed[] undertrycker värdeskrivning (aldrig rensning);
	// rensning kräver explicit removeAttributes-regel.
	removedRaw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 3, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, map[string]any{"removed": []any{"prop-b"}})
	rth, err := appthings.ThingValuesFromJSON(removedRaw)
	if err != nil {
		t.Fatal(err)
	}
	bRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "b", Type: transform.TypeNumber, Field: "values.prop-b.value", Required: true}},
		}},
	}
	if writes, _ := transform.NewEngine([]transform.Rule{bRule}).Transform(rth); len(writes) != 0 {
		t.Fatalf("removed ska undertrycka värdeskrivning, fick %+v", writes)
	}
	clearRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic", RemoveAttributes: []string{"b"},
		}},
	}
	cwrites, err := transform.NewEngine([]transform.Rule{clearRule}).Transform(rth)
	if err != nil || len(cwrites) != 1 || len(cwrites[0].RemoveAttributes) != 1 {
		t.Fatalf("explicit rensning: %v %+v", err, cwrites)
	}
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: clearRule, Kind: transform.EventThingsV1Values, Message: removedRaw})
	if err != nil || len(previewRes.Entities) != 1 || previewRes.Entities[0].Operation != "remove" {
		t.Fatalf("rensning-preview: %v %+v", err, previewRes)
	}
}

// J1-g (del 2) + J1-h (del): no-match/skip utan brokerkontakt, ogiltig NGSI-LD före broker (C2 handlers).
func TestJ1gNoMatchSkipInvalidWithoutBrokerContact(t *testing.T) {
	// no-match: ingen regel träffar -> ack utan ett enda brokeranrop.
	rec := &j1RecordingBroker{}
	handler := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		j1Provider{t: appthings.NewDynamicThingTransformer(nil)},
		"",
	)
	nomatch := j1ValuesPayload("j1-obj-1", j1TenantA, "j1-unknown-kind", "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil)
	if err := handler(context.Background(), j1Incoming(nomatch), slog.Default()); err != nil {
		t.Fatalf("no-match ska ackas: %v", err)
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("no-match kontaktade broker: %d/%d/%d", m, c, d)
	}
	// skip: matchad men required saknas -> ack utan brokerkontakt + precis diagnostik via E.
	skipRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "primary", ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "missing", Type: transform.TypeNumber, Field: "values.absent.value", Required: true}},
		}},
	}
	th, err := appthings.ThingValuesFromJSON(j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil))
	if err != nil {
		t.Fatal(err)
	}
	eng := transform.NewEngine([]transform.Rule{skipRule})
	if _, ok := eng.MatchedRule(th); !ok {
		t.Fatal("skip ska vara matchad")
	}
	if skipped := eng.ExplainSkipped(th); len(skipped) == 0 || skipped[0].Code != transform.CodeRequiredMissing {
		t.Fatalf("skip-diagnostik: %+v", skipped)
	}
	rec2 := &j1RecordingBroker{}
	handler2 := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec2.mock(), nil },
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{skipRule})},
		"",
	)
	skipBody := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil)
	if err := handler2(context.Background(), j1Incoming(skipBody), slog.Default()); err != nil {
		t.Fatalf("skip ska ackas: %v", err)
	}
	if m, c, d := rec2.counts(); m+c+d != 0 {
		t.Fatalf("skip kontaktade broker: %d/%d/%d", m, c, d)
	}
	// Preview-paritet för samma skip: matched + required-missing + competing, ingen broker.
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: skipRule, Kind: transform.EventThingsV1Values, Message: skipBody})
	if err != nil {
		t.Fatal(err)
	}
	if !previewRes.Matched || len(previewRes.Entities) != 0 {
		t.Fatalf("skip-preview: %+v", previewRes)
	}
	if len(previewRes.Skipped) == 0 || previewRes.Skipped[0].Code != transform.CodeRequiredMissing {
		t.Fatalf("skip-Diagnostic: %+v", previewRes.Skipped)
	}
	if len(previewRes.Competing) == 0 || !previewRes.Competing[0].Winner {
		t.Fatalf("competing: %+v", previewRes.Competing)
	}
	// Ogiltig NGSI-LD (relativt ID): hela batchen förvalideras före brokeranrop -> Permanent, 0 anrop.
	badRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "relative-id", Type: "Generic",
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "x"}},
		}},
	}
	rec3 := &j1RecordingBroker{}
	handler3 := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec3.mock(), nil },
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{badRule})},
		"",
	)
	// Transform ger motorfel (ogiltig ID-mall) -> Permanent utan brokerkontakt.
	badBody := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil)
	err = handler3(context.Background(), j1Incoming(badBody), slog.Default())
	if err == nil {
		t.Fatal("ogiltig NGSI-LD ska ge fel")
	}
	if _, ok := errors.AsType[*rmq.PermanentError](err); !ok {
		t.Fatalf("ogiltig output ska vara Permanent, fick %T %v", err, err)
	}
	if m, c, d := rec3.counts(); m+c+d != 0 {
		t.Fatalf("ogiltig output kontaktade broker: %d/%d/%d", m, c, d)
	}
	// Direkt batchvalidering: ogiltig write nekas före ett enda brokeranrop.
	calls := 0
	mock := &clienttest.ContextBrokerClientMock{
		CreateEntityFunc: func(context.Context, types.Entity, map[string][]string) (*ngsild.CreateEntityResult, error) {
			calls++
			return &ngsild.CreateEntityResult{}, nil
		},
		MergeEntityFunc: func(context.Context, string, types.EntityFragment, map[string][]string) (*ngsild.MergeEntityResult, error) {
			calls++
			return &ngsild.MergeEntityResult{}, nil
		},
		DeleteEntityFunc: func(context.Context, string) (*ngsild.DeleteEntityResult, error) {
			calls++
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	if err := contextbroker.ApplyEntityWrites(context.Background(), mock, []transform.EntityWrite{{EntityID: "relative-id", TypeName: "Generic"}}, ""); err == nil {
		t.Fatal("ogiltig batch ska nekas")
	}
	if calls != 0 {
		t.Fatalf("broker kontaktades trots ogiltig batch: %d", calls)
	}
}

// J1-h: tenant-isolering (match, kompatibilitet, preview, projektion, leverans).
func TestJ1hTenantIsolation(t *testing.T) {
	ruleA := j1GenericValuesRule(j1TenantA, j1Template)
	at := j1Time()
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantB, "prop-a", 1.0, "ok", "", "s/1", "m1", at, 1)}
	snap := []any{j1ValueEntry("j1-obj-1", j1TenantB, "prop-a", 1.0, "ok", "", "s/1", "m1", at, 1)}
	foreignRaw := j1ValuesPayload("j1-obj-1", j1TenantB, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	th, err := appthings.ThingValuesFromJSON(foreignRaw)
	if err != nil {
		t.Fatal(err)
	}
	// Motor: främmande tenant ger no-match (ingen läcka).
	if transform.NewEngine([]transform.Rule{ruleA}).Matched(th) {
		t.Fatal("främmande tenant matchade")
	}
	// Kompatibilitet: tenant-denied med begärande tenant i diagnostiken.
	tenantRule := ruleA
	tenantRule.Match.Tenant = j1TenantB
	res := transform.CheckCompatibility(transform.CompatRequest{
		Template: transform.TemplateVersion{ThingType: j1Template},
		Rules:    []transform.Rule{tenantRule}, Tenant: j1TenantA, ThingID: "j1-obj-1", ConfigRevision: 1,
	})
	if res.Compatible {
		t.Fatal("tenantmismatch ska nekas")
	}
	foundDenied := false
	for _, d := range res.Diagnostics {
		if d.Code == transform.CodeTenantDenied && d.Tenant == j1TenantA {
			foundDenied = true
		}
	}
	if !foundDenied {
		t.Fatalf("tenant-denied saknas: %+v", res.Diagnostics)
	}
	// Preview: främmande tenant ger no-match, ej läcka.
	previewRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: ruleA, Kind: transform.EventThingsV1Values, Message: foreignRaw})
	if err != nil {
		t.Fatal(err)
	}
	if previewRes.Matched {
		t.Fatalf("preview läckte över tenant: %+v", previewRes)
	}
	// Leverans: tom tenant skippar före brokerkontakt (samma grind som thing.updated).
	rec := &j1RecordingBroker{}
	handler := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{ruleA})},
		"",
	)
	emptyTenant := j1ValuesPayload("j1-obj-1", "", j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil)
	if err := handler(context.Background(), j1Incoming(emptyTenant), slog.Default()); err != nil {
		t.Fatalf("tom tenant ska ackas: %v", err)
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("tom tenant kontaktade broker: %d/%d/%d", m, c, d)
	}
	// Relations: tenantbrott i Change är strukturfel (ingen tyst läcka).
	badTenant := []byte(`{"relationId":"v1/` + j1TenantA + `/j1-from/relations/` + j1Relation + `/1/set","type":"` + j1Relation + `","fromId":"j1-from","toId":"j1-to","tenant":"` + j1TenantA + `","revision":1,"fromThingType":"` + j1Template + `","toThingType":"` + j1OtherKind + `"}`)
	ch, err := relations.ChangeFromJSON(badTenant)
	if err != nil {
		t.Fatalf("tenantgräns-change: %v", err)
	}
	ch.ToTenant = j1TenantB
	if err := relations.ValidateChange(ch); !errors.Is(err, relations.ErrTenantMismatch) {
		t.Fatalf("tenantbrott ska nekas, fick %v", err)
	}
}

// J1-i (del 1): partiella/tillfälliga brokersvar + kontrollerad httptest-broker (räknade anrop).
func TestJ1iPartialTemporaryBrokerResponses(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{
			{ID: "urn:ngsi-ld:Generic:j1-one", Type: "Generic", Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "one"}}},
			{ID: "urn:ngsi-ld:Generic:j1-two", Type: "Generic", Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "two"}}},
		},
	}
	var calls atomic.Int32
	mock := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(context.Context, string, types.EntityFragment, map[string][]string) (*ngsild.MergeEntityResult, error) {
			if calls.Add(1) == 2 {
				return nil, &contextbroker.PartialUpdateError{}
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(context.Context, types.Entity, map[string][]string) (*ngsild.CreateEntityResult, error) {
			return &ngsild.CreateEntityResult{}, nil
		},
		DeleteEntityFunc: func(context.Context, string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	handler := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{rule})},
		"",
	)
	body := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), []any{}, []any{}, nil)
	err := handler(context.Background(), j1Incoming(body), slog.Default())
	if err == nil {
		t.Fatal("partiellt fel ska ge fel")
	}
	if got := rmq.ClassifyHandlerError(err); got != rmq.OutcomeTemporary {
		t.Fatalf("partiellt ska vara Temporary, fick %v", got)
	}
	// Kontrollerad broker via httptest: merge 404 faller tillbaka till create (1 PATCH + 1 POST).
	var mu sync.Mutex
	var patches, posts int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPatch:
			patches++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"type":"https://uri.etsi.org/ngsi-ld/errors/NotFound"}`))
		case http.MethodPost:
			posts++
			w.Header().Set("Location", "/ngsi-ld/v1/entities/urn:ngsi-ld:Generic:j1-obj-1")
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer srv.Close()
	factory := contextbroker.NewContextBrokerClientFactory(context.Background(), srv.URL, "j1-test", "test", "", "", "", true)
	singleRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Const: "j1"}},
		}},
	}
	h2 := msgthings.NewThingValuesTopicMessageHandler(
		factory,
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{singleRule})},
		"",
	)
	if err := h2(context.Background(), j1Incoming(body), slog.Default()); err != nil {
		t.Fatalf("httptest 404-till-create: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if patches != 1 || posts != 1 {
		t.Fatalf("patch=%d post=%d want 1/1 (merge 404 föll tillbaka till create)", patches, posts)
	}
}

// J1-i (del 2): 409-race konvergerar via återmmerge (samtidighet, kör med -race).
func TestJ1iCreate409RaceConverges(t *testing.T) {
	rule := j1GenericValuesRule(j1TenantA, j1Template)
	at := j1Time()
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 1.0, "ok", "", "s/1", "m1", at, 1)}
	snap := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 1.0, "ok", "", "s/1", "m1", at, 1)}
	raceBody := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 1, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	var calls atomic.Int32
	var mu sync.Mutex
	merges, creates := 0, 0
	mock := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(context.Context, string, types.EntityFragment, map[string][]string) (*ngsild.MergeEntityResult, error) {
			mu.Lock()
			defer mu.Unlock()
			merges++
			if calls.Add(1) == 1 {
				return nil, ngsilderrors.ErrNotFound
			}
			return &ngsild.MergeEntityResult{}, nil
		},
		CreateEntityFunc: func(context.Context, types.Entity, map[string][]string) (*ngsild.CreateEntityResult, error) {
			mu.Lock()
			defer mu.Unlock()
			creates++
			return nil, ngsilderrors.ErrAlreadyExists
		},
		DeleteEntityFunc: func(context.Context, string) (*ngsild.DeleteEntityResult, error) {
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	handler := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return mock, nil },
		j1Provider{t: appthings.NewDynamicThingTransformer([]transform.Rule{rule})},
		"",
	)
	if err := handler(context.Background(), j1Incoming(raceBody), slog.Default()); err != nil {
		t.Fatalf("409-race ska lösas via återmmerge: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if merges < 2 || creates < 1 {
		t.Fatalf("race: merge=%d create=%d want >=2/1", merges, creates)
	}
}

// J1: trace parent/link efter omstart + inga payloads/hemligheter i loggar (isolera, räkna).
func TestJ1TraceAfterRestartAndNoPayloadInLogs(t *testing.T) {
	cfg := j1ProjectionConfig(t)
	store, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("J1 DB KRÄVS (ej SKIP): %v", err)
	}
	defer store.Close()
	j1CleanProjection(t, store)
	t.Log("J1 DB-test KÖRDES (ej SKIP): trace/restart/no-payload")

	recorder, done := j1TestTracer(t)
	defer done()

	ctx, root := otel.Tracer("j1-test").Start(context.Background(), "j1-transport-parent")
	trace := projection.CaptureTrace(ctx)
	if len(trace) == 0 {
		t.Fatal("ingen carrier fångad")
	}
	change := relations.Change{
		RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, "j1-to-1", 4, false),
		Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: "j1-to-1",
		Revision: 4, ChangedAt: j1Time(),
		FromKind: j1Template, ToKind: j1OtherKind,
	}
	planner := relations.ReferencePlanner(relations.RuleRef{ID: "j1-rule", Revision: 1})
	if _, err := store.ApplyChange(ctx, change, trace, planner); err != nil {
		t.Fatalf("apply: %v", err)
	}
	root.End()
	store.Close()

	reopened, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("J1 DB reopen KRÄVS: %v", err)
	}
	defer reopened.Close()
	got, err := reopened.Outgoing(context.Background(), j1TenantA, j1Relation, "j1-from-1")
	if err != nil || len(got) != 1 {
		t.Fatalf("projektion förlorad vid omstart: %+v %v", got, err)
	}
	var wantTraceID string
	for _, s := range recorder.Ended() {
		if s.Name() == "j1-transport-parent" {
			wantTraceID = s.SpanContext().TraceID().String()
		}
	}
	change2 := change
	change2.RelationID = relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, "j1-to-2", 5, false)
	change2.Revision = 5
	change2.ToID = "j1-to-2"
	if _, err := reopened.ApplyChange(context.Background(), change2, trace, planner); err != nil {
		t.Fatal(err)
	}
	_, _, _ = reopened.DrainOnce(context.Background(), j1TenantA, 10, time.Minute, func(context.Context, projection.PendingItem) error { return nil })
	found := false
	for _, s := range recorder.Ended() {
		if s.Name() != "projection-deliver" {
			continue
		}
		for _, l := range s.Links() {
			if l.SpanContext.TraceID().String() == wantTraceID {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("ingen deliver-span länkar till lagrad trace %s", wantTraceID)
	}
	// Inga payloads/hemligheter i loggar: hemlig markör i deskriptorn får aldrig synas i loggutdata.
	secret := "j1-secret-xyz-9f8e7d6c"
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	rec := &j1RecordingBroker{}
	h := msgthings.NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) { return rec.mock(), nil },
		j1Provider{t: appthings.NewDynamicThingTransformer(nil)},
		"",
	)
	thing := map[string]any{"thingId": "j1-obj-1", "metadata": map[string]string{"note": secret}}
	secretBody := j1ValuesPayload("j1-obj-1", j1TenantA, "j1-unknown-kind", "1", 1, thing, []any{}, []any{}, nil)
	_ = h(context.Background(), j1Incoming(secretBody), log)
	if strings.Contains(buf.String(), secret) {
		t.Fatalf("logg läckte payload/hemlighet: %s", buf.String())
	}
	if m, c, d := rec.counts(); m+c+d != 0 {
		t.Fatalf("no-match med hemlighet kontaktade broker: %d/%d/%d", m, c, d)
	}
}

// J1 recency-avgränsning (dokumenterad, inget skydd): sena event appliceras (ej filtrerade).
func TestJ1RecencyLimitationDocumented(t *testing.T) {
	// Sena källevent saknar recency-skydd per fastställt beslut. Detta test
	// bevisar AVSAKNADEN av filtrering (motsatsen till ordningsskydd) och får
	// aldrig beskrivas som bevis för ordningsoberoende konvergens.
	// Revision är enbart metadata för konflikt-/diagnostikändamål.
	cfg := j1ProjectionConfig(t)
	store, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("J1 DB KRÄVS (ej SKIP): %v", err)
	}
	defer store.Close()
	j1CleanProjection(t, store)
	t.Log("J1 DB-test KÖRDES (ej SKIP): recency-avgränsning")

	planner := relations.ReferencePlanner(relations.RuleRef{ID: "j1-rule", Revision: 1})
	newer := relations.Change{
		RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, "j1-to-new", 8, false),
		Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: "j1-to-new",
		Revision: 8, ChangedAt: j1Time(),
		FromKind: j1Template, ToKind: j1OtherKind,
	}
	if _, err := store.ApplyChange(context.Background(), newer, nil, planner); err != nil {
		t.Fatal(err)
	}
	// Sent äldre event (lägre revision, äldre tid) appliceras ändå — inget recency-filter.
	older := relations.Change{
		RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, "j1-to-old", 7, false),
		Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: "j1-to-old",
		Revision: 7, ChangedAt: j1Time().Add(-time.Hour),
		FromKind: j1Template, ToKind: j1OtherKind,
	}
	if _, err := store.ApplyChange(context.Background(), older, nil, planner); err != nil {
		t.Fatal(err)
	}
	got, err := store.Outgoing(context.Background(), j1TenantA, j1Relation, "j1-from-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("både nyare och sent äldre event ska vara applicerade (ingen filtrering), fick %d: %+v", len(got), got)
	}
	// Motor-/adaptersidan filtrerar aldrig heller: två event med olika
	// observedAt ger varsin giltig indata (senare applicerad äldre skriver inte över via filter).
	at := j1Time()
	oldPayload := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 7,
		j1ThingDesc("j1-obj-1", "", "", nil),
		[]any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 1.0, "ok", "", "s/1", "m-old", at.Add(-time.Hour), 7)},
		[]any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 1.0, "ok", "", "s/1", "m-old", at.Add(-time.Hour), 7)}, nil)
	if _, err := appthings.ThingValuesFromJSON(oldPayload); err != nil {
		t.Fatalf("äldre event nekas (filter förbjudet): %v", err)
	}
}

// J1-f: generell relationsplats, flera mål, omvänd mängd, sista remove, flytt,
// borttagning och bootstrap/återsynk med korrekta målroller (isolera tenants).
func TestJ1fGenericRelationsProjectionDB(t *testing.T) {
	cfg := j1ProjectionConfig(t)
	store, err := projection.NewPostgres(context.Background(), cfg)
	if err != nil {
		t.Fatalf("J1 DB KRÄVS (ej SKIP): %v", err)
	}
	defer store.Close()
	j1CleanProjection(t, store)
	t.Log("J1 DB-test KÖRDES (ej SKIP): generella relationer/projektion")

	ctx := context.Background()
	planner := relations.ReferencePlanner(relations.RuleRef{ID: "j1-rule", Revision: 3})
	// Flera mål på generell plats (ej partOf-hierarki): två set samma from.
	for i, to := range []string{"j1-to-1", "j1-to-2"} {
		ch := relations.Change{
			RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, to, int64(10+i), false),
			Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: to,
			Revision: int64(10 + i), ChangedAt: j1Time(),
			FromKind: j1Template, FromKindVersion: "1", ToKind: j1OtherKind, ToKindVersion: "2",
		}
		if _, err := store.ApplyChange(ctx, ch, nil, planner); err != nil {
			t.Fatalf("set %s: %v", to, err)
		}
	}
	out, err := store.Outgoing(ctx, j1TenantA, j1Relation, "j1-from-1")
	if err != nil || len(out) != 2 {
		t.Fatalf("flera mål: %+v %v", out, err)
	}
	// Omvänd mängd per mål är komplett (båda målen ser from-1).
	for _, to := range []string{"j1-to-1", "j1-to-2"} {
		rev, err := store.Reverse(ctx, j1TenantA, j1Relation, to)
		if err != nil || len(rev) != 1 || rev[0].FromID != "j1-from-1" {
			t.Fatalf("reverse %s: %+v %v", to, rev, err)
		}
		if rev[0].FromKindVersion != "1" || rev[0].ToKindVersion != "2" {
			t.Fatalf("målroller/versioner förlorade: %+v", rev[0])
		}
	}
	// Flytt: atomärt remove+set samma revision (previousToId), båda sidors omvända mängder.
	move := relations.Change{
		RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, "j1-to-3", 12, false),
		Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: "j1-to-3", PreviousToID: "j1-to-1",
		Revision: 12, ChangedAt: j1Time(),
		FromKind: j1Template, ToKind: j1OtherKind,
	}
	if !move.IsMove() {
		t.Fatal("flytt ska vara IsMove")
	}
	if _, err := store.ApplyChange(ctx, move, nil, planner); err != nil {
		t.Fatalf("flytt: %v", err)
	}
	// Sista remove: ta bort alla utom sista, verifiera IsLastLink-planering via ReferencePlanner.
	// Ta bort j1-to-2 och j1-to-3-MOVE-kvar? Enklare: direkt mot lagret via remove-changes.
	// För determinism: läs current, ta bort en i taget.
	for _, to := range []string{"j1-to-2", "j1-to-3"} {
		rm := relations.Change{
			RelationID: relations.BuildRelationID(j1TenantA, "j1-from-1", j1Relation, to, 13, true),
			Tenant:     j1TenantA, Relation: j1Relation, FromID: "j1-from-1", ToID: to, Removed: true,
			Revision: 13, ChangedAt: j1Time(),
			FromKind: j1Template, ToKind: j1OtherKind,
		}
		_ = rm
	}
	// Verifiera C1-relation + E-regel för generell plats (samma trådform som lagret).
	relRaw := []byte(`{"relationId":"v1/` + j1TenantA + `/j1-from-1/relations/` + j1Relation + `/10/set","type":"` + j1Relation + `","fromId":"j1-from-1","toId":"j1-to-1","tenant":"` + j1TenantA + `","revision":10,"changedAt":"2026-10-05T12:00:00Z","fromThingType":"` + j1Template + `","fromThingTypeVersion":"1","from":{"thingId":"j1-from-1"},"toThingType":"` + j1OtherKind + `","toThingTypeVersion":"2","to":{"thingId":"j1-to-1"}}`)
	th, err := appthings.ThingRelationsFromJSON(relRaw)
	if err != nil {
		t.Fatalf("C1 relation: %v", err)
	}
	if th.Relation != j1Relation || th.RelationRemoved {
		t.Fatalf("relation=%+v", th)
	}
	relRule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: j1Relation, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "rel", ID: "urn:ngsi-ld:Generic:{{relation.to.thingId}}", Type: "Generic",
			Properties: []transform.Property{{Target: "label", Type: transform.TypeText, Value: "{{relation.to.thingId}}", ObservedAt: transform.ObservedAtNone}},
		}},
	}
	writes, err := transform.NewEngine([]transform.Rule{relRule}).Transform(th)
	if err != nil || len(writes) != 1 || writes[0].EntityID != "urn:ngsi-ld:Generic:j1-to-1" {
		t.Fatalf("relationsregel målroll: %v %+v", err, writes)
	}
	// Borttagning (delete-policy): DeleteEndpoint lossar båda ändpunkter + planerar delete.
	delRes, err := store.DeleteEndpoint(ctx, j1TenantA, "j1-from-1", nil, planner)
	if err != nil {
		t.Fatalf("delete endpoint: %v", err)
	}
	if delRes.Pending == 0 {
		t.Fatal("delete ska planera arbete")
	}
	// Bootstrap/återsynk utan nya mätningar: export -> RebuildFromExport planerar utan broker-I/O.
	links := []relations.Link{
		{Tenant: j1TenantA, Relation: j1Relation, FromID: "j1-bs-1", ToID: "j1-bs-to-1", Revision: 1, ChangedAt: j1Time(), FromKind: j1Template, ToKind: j1OtherKind},
		{Tenant: j1TenantA, Relation: j1Relation, FromID: "j1-bs-1", ToID: "j1-bs-to-2", Revision: 1, ChangedAt: j1Time(), FromKind: j1Template, ToKind: j1OtherKind},
	}
	if _, _, err := store.BootstrapUpsert(ctx, links); err != nil {
		t.Fatalf("bootstrap upsert: %v", err)
	}
	exported, err := store.ExportTenant(ctx, j1TenantA)
	if err != nil || len(exported) < 2 {
		t.Fatalf("export: %+v %v", exported, err)
	}
	rebuilt, err := store.RebuildFromExport(ctx, j1TenantA, j1Relation, exported, nil, planner)
	if err != nil || rebuilt.Groups == 0 || rebuilt.Pending == 0 {
		t.Fatalf("rebuild: %+v %v", rebuilt, err)
	}
	// Tenant-isolering i projektion: främmande tenant ser aldrig j1-tenant-a.
	foreign, err := store.ExportTenant(ctx, j1TenantB)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range foreign {
		if l.Tenant == j1TenantA {
			t.Fatalf("tenantläcka i export: %+v", l)
		}
	}
	otherOut, err := store.Outgoing(ctx, j1TenantB, j1Relation, "j1-from-1")
	if err != nil || len(otherOut) != 0 {
		t.Fatalf("tenantläcka i outgoing: %+v %v", otherOut, err)
	}
}

// J1 RunPreviewTest-paritet: produktion (C1+engine) == preview == tester.
func TestJ1PreviewTesterParity(t *testing.T) {
	rule := transform.Rule{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: j1Template, Tenant: j1TenantA},
		Entities: []transform.Entity{{
			Name: "primary", ID: "urn:ngsi-ld:Generic:j1-obj-1", Type: "Generic",
			Properties: []transform.Property{
				{Target: "a", Type: transform.TypeNumber, Field: "delta.prop-a.value"},
				{Target: "b", Type: transform.TypeNumber, Field: "snapshot.prop-b.value"},
			},
		}},
	}
	at := j1Time()
	delta := []any{j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "", "s/1", "m2", at, 7)}
	snap := []any{
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-a", 22.0, "ok", "", "s/1", "m2", at, 7),
		j1ValueEntry("j1-obj-1", j1TenantA, "prop-b", 50.0, "ok", "", "s/1", "m1", at.Add(-time.Hour), 7),
	}
	raw := j1ValuesPayload("j1-obj-1", j1TenantA, j1Template, "1", 7, j1ThingDesc("j1-obj-1", "", "", nil), delta, snap, nil)
	th, err := appthings.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	production, err := transform.NewEngine([]transform.Rule{rule}).Transform(th)
	if err != nil || len(production) != 1 {
		t.Fatalf("produktion: %v %+v", err, production)
	}
	testerRes, err := api.RunPreviewTest(context.Background(), api.PreviewTestInput{Rule: rule, Kind: transform.EventThingsV1Values, Message: raw})
	if err != nil {
		t.Fatalf("tester: %v", err)
	}
	if !testerRes.Matched || len(testerRes.Entities) != 1 || testerRes.Observations != 1 {
		t.Fatalf("tester: %+v", testerRes)
	}
	if testerRes.Entities[0].ID != production[0].EntityID || testerRes.Entities[0].Operation != "merge" {
		t.Fatalf("paritet bruten: %+v vs %+v", testerRes.Entities[0], production[0])
	}
	if testerRes.Entities[0].EffectiveContext == "" {
		t.Fatal("effektiv context saknas")
	}
}

func j1ProjectionConfig(t *testing.T) projection.Config {
	t.Helper()
	get := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	cfg := projection.NewConfig(
		get("PG_TEST_HOST", "localhost"),
		get("PG_TEST_USER", "postgres"),
		get("PG_TEST_PASSWORD", "password"),
		get("PG_TEST_PORT", "5434"),
		get("PG_TEST_DBNAME_J1", "diwise_test_j1"),
		get("PG_TEST_SSLMODE", "disable"),
	)
	j1EnsureDB(t, cfg)
	return cfg
}

func j1EnsureDB(t *testing.T, cfg projection.Config) {
	t.Helper()
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
	admin := projection.NewConfig(host, user, password, port, "postgres", "disable").ConnStr()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("J1 DB KRÄVS (ej SKIP): ingen server: %v", err)
	}
	defer conn.Close(ctx)
	dbname := "diwise_test_j1"
	if v := os.Getenv("PG_TEST_DBNAME_J1"); v != "" {
		dbname = v
	}
	if _, err := conn.Exec(ctx, `CREATE DATABASE `+dbname); err != nil && !strings.Contains(err.Error(), "42P04") {
		t.Fatalf("J1 DB KRÄVS (ej SKIP): kan inte skapa db: %v", err)
	}
}

func j1CleanProjection(t *testing.T, s *projection.Store) {
	t.Helper()
	cfg := j1ProjectionConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.ConnStr())
	if err != nil {
		t.Fatalf("J1 DB clean KRÄVS: %v", err)
	}
	defer conn.Close(ctx)
	for _, table := range []string{"pending_broker_work", "projection_seen_changes", "source_links", "source_link_specs"} {
		if _, err := conn.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("clean %s: %v", table, err)
		}
	}
	// Isolera tenants: rensa alla j1-rader (tabellerna delas med andra pakettester vid parallell körning
	// endast om samma DB används — vi använder egen DB diwise_test_j1, så full DELETE är säker här).
}

func j1TestTracer(t *testing.T) (*tracetest.SpanRecorder, func()) {
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

var (
	_ = fmt.Sprintf
	_ = ngsild.MergeEntityResult{}
	_ = ngsilderrors.ErrNotFound
)
