package config

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// J2-standardregler efter J1 (PLAN1050 J2): JSON-/modelltester för levererade
// mappningar + dokumenterade kvarvarande avsteg. Modellnamn förekommer endast
// här i tester/regler — aldrig i motor/adaptrar.

// J2(a): dateObserved (timestamp:true) är max-observedAt (mättid), aldrig
// publiceringstid. Fallback publiceringstid endast för deskriptor-bara event.
func TestJ2DateObservedIsMaxObservedAt(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	payload := map[string]any{
		"schema": "v1", "eventId": "j2-time", "configRevision": 1,
		"publishedAt": "2026-10-01T11:00:00Z",
		"thingId":     "j2-room-1", "thingType": "room", "tenant": "test",
		"delta": []any{
			map[string]any{"propertyId": "temperature.average", "value": 21.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
			map[string]any{"propertyId": "humidity", "value": 55.0, "quality": "ok", "observedAt": "2026-10-01T10:05:00Z"},
		},
		"snapshot": []any{
			map[string]any{"propertyId": "temperature.average", "value": 21.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
			map[string]any{"propertyId": "humidity", "value": 55.0, "quality": "ok", "observedAt": "2026-10-01T10:05:00Z"},
		},
		"thing": map[string]any{"thingId": "j2-room-1", "name": "J2 Rum",
			"location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}},
	}
	raw, _ := json.Marshal(payload)
	input, err := things.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	writes, err := engine.Transform(input)
	if err != nil || len(writes) != 1 {
		t.Fatalf("room values: %+v %v", writes, err)
	}
	entity := writeJSON(t, writes[0])
	observed, ok := entity["dateObserved"].(map[string]any)["value"].(map[string]any)["@value"].(string)
	if !ok {
		t.Fatalf("dateObserved saknas: %+v", entity)
	}
	if observed != "2026-10-01T10:05:00Z" {
		t.Fatalf("dateObserved=%s, want max-observedAt 10:05 (aldrig publiceringstid 11:00)", observed)
	}
}

// J2(a): wastebin har dateObserved (mättid) sedan J2.
func TestJ2WastebinDateObserved(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	payload := map[string]any{
		"schema": "v1", "eventId": "j2-bin", "configRevision": 1,
		"publishedAt": "2026-10-01T11:00:00Z",
		"thingId":     "j2-bin-1", "thingType": "wastebin", "tenant": "test",
		"delta": []any{
			map[string]any{"propertyId": "fillRate", "value": 0.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
		},
		"snapshot": []any{
			map[string]any{"propertyId": "fillRate", "value": 0.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
		},
		"thing": map[string]any{"thingId": "j2-bin-1", "name": "J2 Bin",
			"location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}},
	}
	raw, _ := json.Marshal(payload)
	input, err := things.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	writes, err := engine.Transform(input)
	if err != nil || len(writes) != 1 {
		t.Fatalf("wastebin values: %+v %v", writes, err)
	}
	entity := writeJSON(t, writes[0])
	if _, ok := entity["dateObserved"]; !ok {
		t.Fatalf("wastebin dateObserved saknas: %+v", entity)
	}
}

// J2(b): lifecycle created/updated skapar beskrivande skal utan mätningar,
// med samma mål-ID:n som värde-reglerna. Mätburna entiteter (Weather/WQO)
// utelämnas tills första värdeseventet.
func TestJ2LifecycleCreatesShellWithoutMeasurements(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	cases := []struct {
		sourceType string
		subType    string
		wantID     string
		wantType   string
	}{
		{"room", "", "urn:ngsi-ld:IndoorEnvironmentObserved:j2-shell-1", "IndoorEnvironmentObserved"},
		{"wastebin", "", "urn:ngsi-ld:WasteContainer:j2-shell-1", "WasteContainer"},
		{"lifebuoy", "", "urn:ngsi-ld:Lifebuoy:j2-shell-1", "Lifebuoy"},
		{"desk", "", "urn:ngsi-ld:Device:j2-shell-1", "Device"},
		{"pumpingstation", "", "urn:ngsi-ld:SewagePumpingStation:j2-shell-1", "SewagePumpingStation"},
		{"sewer", "", "urn:ngsi-ld:Sewer:j2-shell-1", "Sewer"},
		{"sewer", "CombinedSewerOverflow", "urn:ngsi-ld:CombinedSewerOverflow:j2-shell-1", "CombinedSewerOverflow"},
		{"pointofinterest", "", "urn:ngsi-ld:PointOfInterest:j2-shell-1", "PointOfInterest"},
		{"pointofinterest", "Beach", "urn:ngsi-ld:Beach:j2-shell-1", "Beach"},
	}
	for _, tc := range cases {
		t.Run(tc.sourceType+"/"+tc.subType, func(t *testing.T) {
			body := map[string]any{
				"schema": "v1", "eventId": "j2-created", "thingId": "j2-shell-1",
				"thingType": tc.sourceType, "tenant": "test", "configRevision": 1,
				"kind": "created", "changedAt": "2026-10-01T12:00:00Z",
				"thing": map[string]any{"thingId": "j2-shell-1", "name": "J2 Shell",
					"alternativeName": "Alias", "subType": tc.subType,
					"metadata": map[string]any{"description": "J2 desc"},
					"location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}},
			}
			if tc.subType != "" {
				body["thingTypeVersion"] = "1"
			}
			raw, _ := json.Marshal(body)
			input, err := things.ThingLifecycleFromJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			writes, err := engine.Transform(input)
			if err != nil || len(writes) != 1 {
				t.Fatalf("lifecycle created: %+v %v", writes, err)
			}
			if writes[0].EntityID != tc.wantID || writes[0].TypeName != tc.wantType {
				t.Fatalf("mål-ID inkonsistent: %s/%s, want %s/%s",
					writes[0].EntityID, writes[0].TypeName, tc.wantID, tc.wantType)
			}
			entity := writeJSON(t, writes[0])
			if entity["name"].(map[string]any)["value"] != "J2 Shell" {
				t.Fatalf("namn mappas inte utan mätningar: %+v", entity)
			}
			if _, ok := entity["location"]; !ok {
				t.Fatalf("plats mappas inte utan mätningar: %+v", entity)
			}
			// Updated byter namn med samma ID (ingen ny entitet).
			body["kind"] = "updated"
			body["eventId"] = "j2-updated"
			thing := body["thing"].(map[string]any)
			thing["name"] = "J2 Shell renamed"
			raw, _ = json.Marshal(body)
			updated, err := things.ThingLifecycleFromJSON(raw)
			if err != nil {
				t.Fatal(err)
			}
			uw, err := engine.Transform(updated)
			if err != nil || len(uw) != 1 || uw[0].EntityID != tc.wantID {
				t.Fatalf("lifecycle updated: %+v %v", uw, err)
			}
			if writeJSON(t, uw[0])["name"].(map[string]any)["value"] != "J2 Shell renamed" {
				t.Fatalf("namn uppdateras inte utan mätningar: %+v", uw[0])
			}
		})
	}
}

// J2(c): partOf→refParent kräver toType building; annat mål = no-match
// (skip utan brokerkontakt), aldrig tyst felskrivning mot Building-URN.
func TestJ2RelationToTypeFilter(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	link := func(toID, toType string, removed bool) []byte {
		body := map[string]any{
			"relationId": "j2-link", "type": "partOf", "fromId": "j2-room-1",
			"toId": toID, "tenant": "test", "revision": 3,
			"changedAt":     "2026-10-01T12:00:00Z",
			"fromThingType": "room", "toThingType": toType,
			"from": map[string]any{"thingId": "j2-room-1"},
			"to":   map[string]any{"thingId": toID},
		}
		if removed {
			body["removed"] = true
		}
		raw, _ := json.Marshal(body)
		return raw
	}
	// Byggnadsmål träffar.
	input, err := things.ThingRelationsFromJSON(link("j2-bld-1", "building", false))
	if err != nil {
		t.Fatal(err)
	}
	writes, err := engine.Transform(input)
	if err != nil || len(writes) != 1 {
		t.Fatalf("building target: %+v %v", writes, err)
	}
	if got := writeJSON(t, writes[0])["refParent"].(map[string]any)["object"]; got != "urn:ngsi-ld:Building:j2-bld-1" {
		t.Fatalf("refParent=%v", got)
	}
	// Annan måltyp träffar inte (generisk avvikelse dokumenterad, ej tyst).
	other, err := things.ThingRelationsFromJSON(link("j2-other-1", "room", false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transform(other); err == nil {
		t.Fatal("icke-building mål ska vara no-match (toType-filter)")
	} else if !strings.Contains(err.Error(), "no matching rule") {
		t.Fatalf("want no matching rule, got %v", err)
	}
	// Bortkoppling mot byggnad rensar.
	removed, err := things.ThingRelationsFromJSON(link("j2-bld-1", "building", true))
	if err != nil {
		t.Fatal(err)
	}
	cleared, err := engine.Transform(removed)
	if err != nil || len(cleared) != 1 || len(cleared[0].RemoveAttributes) != 1 {
		t.Fatalf("removed: %+v %v", cleared, err)
	}
}

// J2(d): sewer kräver plats/tid (skärpt); overflowTime/refDevice är
// dokumenterade avsteg (mappas ej, skrivs ej tyst).
func TestJ2SewerRequirementsAndDeviations(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	base := func() map[string]any {
		return map[string]any{
			"schema": "v1", "eventId": "j2-sewer", "configRevision": 1,
			"publishedAt": "2026-10-01T11:00:00Z",
			"thingId":     "j2-sewer-1", "thingType": "sewer", "tenant": "test",
			"delta": []any{
				map[string]any{"propertyId": "level", "value": 1.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
			},
			"snapshot": []any{
				map[string]any{"propertyId": "level", "value": 1.5, "quality": "ok", "observedAt": "2026-10-01T10:00:00Z"},
			},
			"thing": map[string]any{"thingId": "j2-sewer-1", "name": "J2 Sewer",
				"location": map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}}},
		}
	}
	// Med plats/tid: skrivs (utan overflowTime — dokumenterat avsteg).
	payload := base()
	raw, _ := json.Marshal(payload)
	input, err := things.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	writes, err := engine.Transform(input)
	if err != nil || len(writes) != 1 {
		t.Fatalf("sewer values: %+v %v", writes, err)
	}
	entity := writeJSON(t, writes[0])
	for _, missing := range []string{"overflowTime", "refDevice"} {
		if _, ok := entity[missing]; ok {
			t.Fatalf("dokumenterat avsteg %s ska inte skrivas: %+v", missing, entity)
		}
	}
	if _, ok := entity["location"]; !ok {
		t.Fatalf("location ska skrivas: %+v", entity)
	}
	if _, ok := entity["dateObserved"]; !ok {
		t.Fatalf("dateObserved ska skrivas: %+v", entity)
	}
	// Utan plats: undertrycks (skärpt krav, ej tyst ofullständig skrivning).
	payload = base()
	thing := payload["thing"].(map[string]any)
	delete(thing, "location")
	raw, _ = json.Marshal(payload)
	input, err = things.ThingValuesFromJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	writes, err = engine.Transform(input)
	if err != nil {
		t.Fatalf("sewer utan plats fel: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("saknad plats ska undertrycka (required), fick %+v", writes)
	}
	probe := transform.NewEngine(rules)
	if !probe.Matched(input) {
		t.Fatal("undertryckt ska ändå vara matchad")
	}
	skipped := probe.ExplainSkipped(input)
	if len(skipped) == 0 || skipped[0].Attribute != "location" {
		t.Fatalf("ExplainSkipped: %+v", skipped)
	}
}

// J2(e): greenhouse/tank-regler förblir borttagna; mallar utan regler är
// avsiktligt (ingen återinföring av vana).
func TestJ2GreenhouseTankRulesAbsent(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rules {
		for _, blocked := range []string{"greenhouse", "tank", "tankwater"} {
			if r.Match.Type == blocked {
				t.Fatalf("borttagen regeltyp %q återinförd: %+v", blocked, r.Match)
			}
		}
	}
}

// J2(f): geometri (Point + Polygon) och enheter bevaras utan förvanskning.
func TestJ2GeometryAndUnits(t *testing.T) {
	rules, err := LoadModelsDir("../../../deployments/models")
	if err != nil {
		t.Fatal(err)
	}
	engine := things.NewDynamicThingTransformer(rules)
	for name, location := range map[string]any{
		"point":   map[string]any{"type": "Point", "coordinates": []any{17.5, 62.3}},
		"polygon": map[string]any{"type": "Polygon", "coordinates": []any{[]any{[]any{18.0, 59.3}, []any{19.0, 59.3}, []any{19.0, 60.0}, []any{18.0, 59.3}}}},
	} {
		payload := map[string]any{
			"schema": "v1", "eventId": "j2-geo", "configRevision": 1,
			"publishedAt": "2026-10-01T11:00:00Z",
			"thingId":     "j2-room-1", "thingType": "room", "tenant": "test",
			"delta": []any{
				map[string]any{"propertyId": "temperature.average", "value": 21.5, "quality": "ok", "unit": "Cel", "observedAt": "2026-10-01T10:00:00Z"},
			},
			"snapshot": []any{
				map[string]any{"propertyId": "temperature.average", "value": 21.5, "quality": "ok", "unit": "Cel", "observedAt": "2026-10-01T10:00:00Z"},
			},
			"thing": map[string]any{"thingId": "j2-room-1", "name": "J2 Geo", "location": location},
		}
		raw, _ := json.Marshal(payload)
		input, err := things.ThingValuesFromJSON(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		writes, err := engine.Transform(input)
		if err != nil || len(writes) != 1 {
			t.Fatalf("%s: %+v %v", name, writes, err)
		}
		entity := writeJSON(t, writes[0])
		loc, ok := entity["location"].(map[string]any)["value"].(map[string]any)
		if !ok {
			t.Fatalf("%s location saknas: %+v", name, entity)
		}
		if loc["type"] != location.(map[string]any)["type"] {
			t.Fatalf("%s geometrityp förvanskad: %+v", name, loc)
		}
		// Enhet bevaras genom grinden utan förvanskning: Cel-källa accepteras
		// (kanoniskt alias, ingen ommärkning eftersom regeln inte deklarerar
		// unit) och värdet skrivs exakt.
		temp, ok := entity["temperature"].(map[string]any)
		if !ok || temp["value"] != 21.5 {
			t.Fatalf("%s temperatur förlorad: %+v", name, entity)
		}
	}
}
