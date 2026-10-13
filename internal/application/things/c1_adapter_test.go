package things

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

func loadC1Fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("fixture %s: %v", name, err)
	}
	return raw
}

func mustC1Resolve(t *testing.T, input transform.Thing, path string) any {
	t.Helper()
	got, ok := input.ResolvePath(path)
	if !ok {
		t.Fatalf("missing source view %q in %+v", path, input.Fields)
	}
	return got
}

// Verkliga B-payloads (Body()-goldens), inte bara handskrivna exempel:
// fixturerna är byte-identiska med iot-things-v2/pkg/types Body().
func TestC1RealBValuesBodyParses(t *testing.T) {
	raw := loadC1Fixture(t, "values-legacy-v1.json")
	// Kontraktsschema: B:s Body() bär schema/EventID/tenant/thingId/versioner.
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema", "eventId", "tenant", "thingId", "thingType", "thingTypeVersion", "configRevision", "publishedAt", "thing", "values"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("B Body() saknar %q: %s", key, raw)
		}
	}
	in, err := ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("adapter nekar verklig B-payload: %v", err)
	}
	if in.ID != "object-1" || in.Tenant != "tenant-a" || in.Type != "source-template" {
		t.Fatalf("identitet=%+v", in)
	}
	if got := mustC1Resolve(t, in, `values."reading.average".value`); got != float64(0) {
		t.Fatalf("noll bevaras ej: %#v", got)
	}
	if got := mustC1Resolve(t, in, `values."reading.average".quality`); got != "uncertain" {
		t.Fatalf("quality=%v", got)
	}
	// values==delta alias: legacy utan delta ger båda vyerna.
	if _, ok := in.ResolvePath(`delta."reading.average".value`); !ok {
		t.Fatal("delta-vy saknas för legacy-payload")
	}
	// Gamla payloads utan delta/snapshot/removed tolereras: snapshot/removed
	// utelämnas, fabriceras aldrig.
	if _, ok := in.ResolvePath("snapshot"); ok {
		t.Fatal("snapshot fabricerad för legacy-payload utan snapshot")
	}
	if _, ok := in.ResolvePath("removed"); ok {
		t.Fatal("removed fabricerad för legacy-payload utan removed")
	}
	// Descriptor-alias och rå GeoJSON (Point 3D bevaras).
	if got := mustC1Resolve(t, in, "descriptor.thingId"); got != "object-1" {
		t.Fatalf("descriptor=%v", got)
	}
	if got := mustC1Resolve(t, in, "thing.location.type"); got != "Point" {
		t.Fatalf("thing.location=%v", got)
	}
	geom, ok := in.ResolvePath("geometry")
	if !ok {
		t.Fatal("geometry saknas")
	}
	coords, _ := geom.(map[string]any)["coordinates"].([]any)
	if len(coords) != 3 {
		t.Fatalf("3D trunkerad: %v", geom)
	}
}

func TestC1RealBRelationsBodyParses(t *testing.T) {
	raw := loadC1Fixture(t, "relations-v1.json")
	in, err := ThingRelationsFromJSON(raw)
	if err != nil {
		t.Fatalf("adapter nekar verklig B-relationspayload: %v", err)
	}
	if in.ID != "object-1" || in.Relation != "source-relation" {
		t.Fatalf("relation=%+v", in)
	}
	for _, path := range []string{"relation.relationId", "relation.fromId", "relation.toId", "relations[0].relationId", "relation.from.thingId", "relation.to.thingId"} {
		if _, ok := in.ResolvePath(path); !ok {
			t.Fatalf("missing %s", path)
		}
	}
}

func TestC1RealBLifecycleBodyParses(t *testing.T) {
	raw := loadC1Fixture(t, "lifecycle-v1.json")
	in, err := ThingLifecycleFromJSON(raw)
	if err != nil {
		t.Fatalf("adapter nekar verklig B-lifecyclepayload: %v", err)
	}
	if in.Lifecycle != "updated" {
		t.Fatalf("lifecycle=%q", in.Lifecycle)
	}
	if got := mustC1Resolve(t, in, "event.thingTypeVersion"); got != "2" {
		t.Fatalf("event.thingTypeVersion=%v", got)
	}
}

// Delta/snapshot-separation: values==delta alias, snapshot separat med
// tidigare-giltigt current, ogiltig ersätter aldrig current, removed explicit.
func TestC1DeltaSnapshotSeparateViews(t *testing.T) {
	raw := loadC1Fixture(t, "values-delta-snapshot-v1.json")
	in, err := ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatalf("delta/snapshot-payload: %v", err)
	}
	// values==delta alias en release.
	deltaAvg := mustC1Resolve(t, in, `delta."reading.average".value`)
	valuesAvg := mustC1Resolve(t, in, `values."reading.average".value`)
	if !reflect.DeepEqual(deltaAvg, valuesAvg) || deltaAvg != float64(22) {
		t.Fatalf("values/delta alias mismatch: %v vs %v", valuesAvg, deltaAvg)
	}
	// Snapshot bär både uppdaterat average och tidigare-giltigt peak som
	// inte finns i delta (producentens current-state, ingen sammanfogning).
	if got := mustC1Resolve(t, in, `snapshot."reading.average".value`); got != float64(22) {
		t.Fatalf("snapshot.average=%v", got)
	}
	if got := mustC1Resolve(t, in, `snapshot."reading.peak".value`); got != float64(21.5) {
		t.Fatalf("snapshot.peak (tidigare-giltigt)=%v", got)
	}
	if _, ok := in.ResolvePath(`delta."reading.peak".value`); ok {
		t.Fatal("delta får inte berikas med snapshot-värden")
	}
	// Ogiltig ersätter aldrig current: stale finns i observations men aldrig
	// som skrivbart värde i delta/values/snapshot.
	for _, path := range []string{`values."reading.stale".value`, `delta."reading.stale".value`, `snapshot."reading.stale".value`} {
		if v, ok := in.ResolvePath(path); ok {
			t.Fatalf("ogiltig läckte som skrivbart värde %s: %v", path, v)
		}
	}
	if got := mustC1Resolve(t, in, "observations[1].quality"); got != "invalid" {
		t.Fatalf("observations quality=%v", got)
	}
	// Explicit-rensning exponeras separat.
	if got := mustC1Resolve(t, in, "removed[0]"); got != "reading.obsolete" {
		t.Fatalf("removed=%v", got)
	}
	// Inbäddade relationer i värdeseventet.
	if got := mustC1Resolve(t, in, "relations[0].relationId"); got != "relation-1" {
		t.Fatalf("relations=%v", got)
	}
	// Konsumenttid är max(delta[].observedAt), aldrig transporttid.
	if got := mustC1Resolve(t, in, "event.thingTypeVersion"); got != "2" {
		t.Fatalf("event version=%v", got)
	}
	if in.ObservedAt.UTC().Format("2006-01-02T15:04:05Z") != "2026-10-01T12:02:01Z" {
		t.Fatalf("ObservedAt=%v, want max delta observedAt", in.ObservedAt)
	}
}

func TestC1RelationsMovePreviousToId(t *testing.T) {
	raw := loadC1Fixture(t, "relations-move-v1.json")
	in, err := ThingRelationsFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]any{
		"relation.previousToId": "object-0", "relation.toId": "object-2",
		"relation.fromThingTypeVersion": "2", "relation.toThingTypeVersion": "1",
		"relations[0].previousToId": "object-0",
	} {
		if got := mustC1Resolve(t, in, path); got != want {
			t.Fatalf("%s=%v, want %v", path, got, want)
		}
	}
}

func TestC1DuplicatePropertyIsPermanent(t *testing.T) {
	body := `{"thingId":"x","tenant":"t","thingType":"room",
	  "delta":[{"propertyId":"p","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"},
	           {"propertyId":"p","value":2,"quality":"ok","observedAt":"2026-10-01T12:01:00Z"}]}`
	if _, err := ThingValuesFromJSON([]byte(body)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("dubblett ska vara Permanent strukturfel, fick %v", err)
	}
	snap := `{"thingId":"x","tenant":"t","thingType":"room",
	  "delta":[{"propertyId":"p","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}],
	  "snapshot":[{"propertyId":"q","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"},
	              {"propertyId":"q","value":2,"quality":"ok","observedAt":"2026-10-01T12:01:00Z"}]}`
	if _, err := ThingValuesFromJSON([]byte(snap)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("snapshot-dubblett ska vara Permanent, fick %v", err)
	}
	rem := `{"thingId":"x","tenant":"t","thingType":"room","delta":[],"removed":["p","p"]}`
	if _, err := ThingValuesFromJSON([]byte(rem)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("removed-dubblett ska vara Permanent, fick %v", err)
	}
}

func TestC1ConflictingIdentitiesArePermanent(t *testing.T) {
	for _, body := range []string{
		`{"thingId":"a","tenant":"t","thingType":"room","delta":[{"propertyId":"p","thingId":"b","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}]}`,
		`{"thingId":"a","tenant":"t1","thingType":"room","delta":[{"propertyId":"p","tenant":"t2","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}]}`,
		`{"thingId":"a","tenant":"t","thingType":"room","configRevision":7,"delta":[{"propertyId":"p","value":1,"quality":"ok","configRevision":8,"observedAt":"2026-10-01T12:00:00Z"}]}`,
		`{"thingId":"a","tenant":"t","thingType":"room","values":[{"propertyId":"p","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}],"delta":[{"propertyId":"q","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}]}`,
		`{"fromId":"a","toId":"b","tenant":"t","type":"rel","from":{"thingId":"other"},"to":{"thingId":"b"}}`,
		`{"fromId":"a","toId":"b","tenant":"t","type":"rel","from":{"thingId":"a"},"to":{"thingId":"other"}}`,
		`{"thingId":"a","tenant":"t","thingType":"room","values":[{"propertyId":"","value":1,"quality":"ok"}]}`,
	} {
		var err error
		if strings.Contains(body, "fromId") {
			_, err = ThingRelationsFromJSON([]byte(body))
		} else {
			_, err = ThingValuesFromJSON([]byte(body))
		}
		if err == nil {
			t.Fatalf("motstridig identitet ska vara Permanent: %s", body)
		}
	}
	// Saknade underidentiteter tolereras (gamla payloads): tom thingId/tenant
	// i värden ärver kuvertet utan fel.
	legacy := `{"thingId":"a","tenant":"t","thingType":"room","values":[{"propertyId":"p","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z"}]}`
	if _, err := ThingValuesFromJSON([]byte(legacy)); err != nil {
		t.Fatalf("legacy utan underidentiteter ska tolereras: %v", err)
	}
}

func TestC1NonFiniteIsPermanent(t *testing.T) {
	nan := math.NaN()
	inf := math.Inf(1)
	for _, v := range []thingValue{{PropertyID: "p", Quality: "ok", Value: &nan}, {PropertyID: "p", Quality: "ok", Value: &inf}} {
		if _, _, _, err := buildValueView([]thingValue{v}, "x", "t", 0, "delta"); err == nil {
			t.Fatalf("NaN/Inf ska vara Permanent: %+v", v)
		}
	}
	// Överflöd via tråd ger också Permanent (json-fel, aldrig tyst skip).
	if _, err := ThingValuesFromJSON([]byte(`{"thingId":"x","tenant":"t","delta":[{"propertyId":"p","value":1e999,"quality":"ok"}]}`)); err == nil {
		t.Fatal("1e999 ska nekas permanent")
	}
}

func TestC1ObservationsNeverLeakValues(t *testing.T) {
	raw := loadC1Fixture(t, "values-delta-snapshot-v1.json")
	in, err := ThingValuesFromJSON([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	obs, ok := in.ResolvePath("observations")
	if !ok {
		t.Fatal("observations saknas")
	}
	blob, _ := json.Marshal(obs)
	if strings.Contains(string(blob), `"value":22`) || strings.Contains(string(blob), `"value":21.5`) {
		t.Fatalf("observations läckte numeriska värden: %s", blob)
	}
	for _, path := range []string{"observations[0].valuePresent", "observations[1].valuePresent"} {
		if _, ok := in.ResolvePath(path); !ok {
			t.Fatalf("missing %s", path)
		}
	}
}

func TestC1DescriptorAliasAndPolygon(t *testing.T) {
	// Polygon får aldrig bli Geo-punkt; rått bevaras under både thing,
	// descriptor och geometry.
	body := `{"thingId":"p","tenant":"t","thingType":"room",
	  "thing":{"thingId":"p","location":{"type":"Polygon","coordinates":[[[18.0,59.3],[19.0,59.3],[19.0,60.0],[18.0,59.3]]]}},
	  "delta":[]}`
	in, err := ThingValuesFromJSON([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := in.ResolvePath("location"); ok {
		t.Fatal("polygon får inte ge point-genväg")
	}
	for _, path := range []string{"geometry.type", "thing.location.type", "descriptor.location.type", "descriptor.geometry.type"} {
		got, ok := in.ResolvePath(path)
		if !ok || got != "Polygon" {
			t.Fatalf("%s=%v, want Polygon", path, got)
		}
	}
	// Point 3D: genväg kompletterar, rått bevaras.
	point := `{"thingId":"p","tenant":"t","thingType":"room",
	  "thing":{"thingId":"p","location":{"type":"Point","coordinates":[17,62,9]}},
	  "delta":[]}`
	pin, err := ThingValuesFromJSON([]byte(point))
	if err != nil {
		t.Fatal(err)
	}
	geom, _ := pin.ResolvePath("geometry")
	coords := geom.(map[string]any)["coordinates"].([]any)
	if len(coords) != 3 {
		t.Fatalf("3D trunkerad: %v", geom)
	}
	if _, _, ok := pin.LatLon(); !ok {
		t.Fatal("point-genväg saknas")
	}
}

func TestC1RemovedObjectFormTolerated(t *testing.T) {
	for _, removed := range []string{`["a"]`, `[{"propertyId":"a"}]`, `[{"property":"a"}]`, `[]`} {
		body := `{"thingId":"x","tenant":"t","thingType":"room","delta":[],"removed":` + removed + `}`
		in, err := ThingValuesFromJSON([]byte(body))
		if err != nil {
			t.Fatalf("removed %s: %v", removed, err)
		}
		if _, ok := in.ResolvePath("removed"); !ok {
			t.Fatalf("removed-vy saknas för %s", removed)
		}
	}
}

func TestC1UnknownFieldsToleratedWithoutDisallow(t *testing.T) {
	// Additiva producentfält får aldrig ge fel (json utan DisallowUnknownFields).
	body := `{"thingId":"x","tenant":"t","thingType":"room","unknownTop":"keep",
	  "thing":{"thingId":"x","unknownDesc":1},
	  "delta":[{"propertyId":"p","value":1,"quality":"ok","observedAt":"2026-10-01T12:00:00Z","unknownValue":true}]}`
	if _, err := ThingValuesFromJSON([]byte(body)); err != nil {
		t.Fatalf("additiva fält ska tolereras: %v", err)
	}
	rel := `{"fromId":"a","toId":"b","tenant":"t","type":"rel","unknownTop":1,"from":{"thingId":"a"}}`
	if _, err := ThingRelationsFromJSON([]byte(rel)); err != nil {
		t.Fatalf("relations additiva fält ska tolereras: %v", err)
	}
}
