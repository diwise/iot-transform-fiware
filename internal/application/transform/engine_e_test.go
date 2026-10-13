package transform

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"
)

func jsonMarshal(v any) (string, error) {
	raw, err := json.Marshal(v)
	return string(raw), err
}

func containsStr(haystack, needle string) bool { return strings.Contains(haystack, needle) }

// PLAN1050 E regressioner (skrivna före fix): separata egenskapsuppdateringar,
// flera required-källor, saknad målentitet (skip, ej rensning), geometri och
// fel målroll. Generiska typer/ID:n, inga modellnamn i motorn.

func eValuesThing(delta, snapshot map[string]any, removed []string) Thing {
	fields := map[string]any{
		"thingId": "thing-1", "tenant": "tenant-a",
		"thing":      map[string]any{"thingId": "thing-1"},
		"descriptor": map[string]any{"thingId": "thing-1"},
		"event":      map[string]any{"thingTypeVersion": "2"},
	}
	if delta == nil {
		delta = map[string]any{}
	}
	alias := map[string]any{}
	maps.Copy(alias, delta)
	fields["values"] = alias
	fields["delta"] = delta
	if snapshot != nil {
		fields["snapshot"] = snapshot
	}
	if removed != nil {
		list := make([]any, len(removed))
		for i, id := range removed {
			list[i] = id
		}
		fields["removed"] = list
	}
	fields["observations"] = []any{}
	return Thing{
		ID: "thing-1", Type: "generic-template", Tenant: "tenant-a",
		Event: EventThingsV1Values, Fields: fields,
	}
}

func eProp(target, field string, required bool) Property {
	return Property{Target: target, Type: TypeNumber, Field: field, Required: required}
}

// Separat egenskapsuppdatering: delta bär endast temp, snapshot bär både
// temp och hum. values.temp-regel skriver temp (delta), values.hum-regel
// skriver inget (hum saknas i delta trots snapshot) — snapshot för fulla
// mappningar, delta för deluppdateringar.
func TestERegressionSeparatePropertyUpdates(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	delta := map[string]any{"temperature": mk(22)}
	snapshot := map[string]any{"temperature": mk(22), "humidity": mk(55)}
	th := eValuesThing(delta, snapshot, nil)

	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{
				eProp("temp", "values.temperature.value", false),
				eProp("hum", "values.humidity.value", false),
			},
		}},
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("writes=%d, want 1 (hum skip, temp skrivs)", len(writes))
	}
	ent, err := BuildEntity(writes[0], "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	raw, _ := jsonMarshal(ent)
	if !containsStr(raw, `"temp"`) || containsStr(raw, `"hum"`) {
		t.Fatalf("partial update fel: %s", raw)
	}

	// Full mappning via snapshot.humidity skriver hum när snapshot bär den.
	rule.Entities[0].Properties[1] = eProp("hum", "snapshot.humidity.value", false)
	writes, err = NewEngine([]Rule{rule}).Transform(th)
	if err != nil || len(writes) != 1 {
		t.Fatalf("snapshot full: %v %v", writes, err)
	}
	ent, _ = BuildEntity(writes[0], "")
	raw, _ = jsonMarshal(ent)
	if !containsStr(raw, `"hum"`) {
		t.Fatalf("snapshot.hum saknas: %s", raw)
	}
}

// Flera required-källor: en saknad required hoppar hela entiteten (skip,
// ej fel), även när andra required finns.
func TestERegressionMultipleRequired(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	th := eValuesThing(map[string]any{"a": mk(1)}, map[string]any{"a": mk(1)}, nil)
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{
				eProp("a", "values.a.value", true),
				eProp("b", "values.b.value", true),
			},
		}},
	}
	if !NewEngine([]Rule{rule}).Matched(th) {
		t.Fatal("matched=false, want true (matchad men undertryckt)")
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("writes=%d, want 0 (required b saknas hoppar entitet)", len(writes))
	}
}

// Saknad målentitet vid körning: ogiltig/saknad källa ger skip, aldrig
// rensning. Rensning kräver explicit removeAttributes-regel.
func TestERegressionMissingNeverClears(t *testing.T) {
	th := eValuesThing(map[string]any{}, map[string]any{}, nil)
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{eProp("temp", "values.temperature.value", false)},
		}},
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil || len(writes) != 0 {
		t.Fatalf("saknad ska ge skip: %v %v", writes, err)
	}
	for _, w := range writes {
		if len(w.RemoveAttributes) > 0 || w.Delete {
			t.Fatal("saknad källa får aldrig rensa")
		}
	}
}

// Geometri: Polygon bevaras rått, ogiltig geometri ger fel (ej tyst skip),
// Point-genväg ger GeoProperty.
func TestERegressionGeometry(t *testing.T) {
	th := eValuesThing(map[string]any{}, map[string]any{}, nil)
	th.Fields["geometry"] = map[string]any{
		"type": "Polygon",
		"coordinates": []any{[]any{
			[]any{18.0, 59.3}, []any{19.0, 59.3}, []any{19.0, 60.0}, []any{18.0, 59.3},
		}},
	}
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{{Target: "area", Type: TypeGeo, Field: "geometry"}},
		}},
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil || len(writes) != 1 {
		t.Fatalf("polygon: %v %v", writes, err)
	}
	if err := ValidateEntityWrites(writes, ""); err != nil {
		t.Fatalf("polygon serialiserad NGSI-LD ogiltig: %v", err)
	}

	bad := eValuesThing(map[string]any{}, map[string]any{}, nil)
	bad.Fields["geometry"] = map[string]any{"type": "Polygon", "coordinates": []any{}}
	ruleBad := rule
	ruleBad.Entities[0].Properties[0] = Property{Target: "area", Type: TypeGeo, Field: "geometry"}
	if _, err := NewEngine([]Rule{ruleBad}).Transform(bad); err == nil {
		t.Fatal("ogiltig polygon ska ge fel, ej tyst skip")
	}
}

// Fel målroll: okänd entities-referens ger fel (target-notfound), ej tyst skip.
func TestERegressionWrongTargetRole(t *testing.T) {
	th := eValuesThing(map[string]any{}, map[string]any{}, nil)
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			Name: "primary", ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{{Target: "label", Type: TypeText, Value: "{{entities.missing.id}}", ObservedAt: ObservedAtNone}},
		}},
	}
	if _, err := NewEngine([]Rule{rule}).Transform(th); err == nil {
		t.Fatal("okänd målroll ska ge fel")
	}
}

// Mallversion/variant styr regelval (D-specificitet i E-match): exakt
// version/variant matchar, avvikande version ger no-match, mer specifik
// regel vinner över generell vid samma prioritet.
func TestEVersionVariantMatch(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	base := eValuesThing(map[string]any{"temperature": mk(1)}, map[string]any{"temperature": mk(1)}, nil)
	// event.thingTypeVersion=2 via eValuesThing; descriptor saknar variant.
	versioned := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", ThingTypeVersion: "2"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:versioned", Type: "Generic",
			Properties: []Property{{Target: "label", Type: TypeText, Const: "versioned"}},
		}},
	}
	plain := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:plain", Type: "Generic",
			Properties: []Property{{Target: "label", Type: TypeText, Const: "plain"}},
		}},
	}
	writes, err := NewEngine([]Rule{plain, versioned}).Transform(base)
	if err != nil || len(writes) != 1 || writes[0].EntityID != "urn:ngsi-ld:Generic:versioned" {
		t.Fatalf("versionsspecifik ska vinna: %v %v", writes, err)
	}

	other := eValuesThing(map[string]any{"temperature": mk(1)}, map[string]any{"temperature": mk(1)}, nil)
	other.Fields["event"] = map[string]any{"thingTypeVersion": "9"}
	if _, err := NewEngine([]Rule{versioned}).Transform(other); err == nil {
		t.Fatal("avvikande version ska ge no-match")
	}
	if _, err := NewEngine([]Rule{plain}).Transform(other); err != nil {
		t.Fatalf("wildcard-version ska matcha alla: %v", err)
	}

	// Variant: descriptor.variantId styr.
	variantRule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", VariantID: "variant-a"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:variant", Type: "Generic",
			Properties: []Property{{Target: "label", Type: TypeText, Const: "variant"}},
		}},
	}
	withVariant := eValuesThing(map[string]any{}, map[string]any{}, nil)
	withVariant.Fields["descriptor"] = map[string]any{"thingId": "thing-1", "variantId": "variant-a"}
	withVariant.Fields["thing"] = map[string]any{"thingId": "thing-1", "variantId": "variant-a"}
	if _, err := NewEngine([]Rule{variantRule}).Transform(withVariant); err != nil {
		t.Fatalf("variant ska matcha: %v", err)
	}
	withoutVariant := eValuesThing(map[string]any{}, map[string]any{}, nil)
	withoutVariant.Fields["descriptor"] = map[string]any{"thingId": "thing-1", "variantId": "other"}
	withoutVariant.Fields["thing"] = map[string]any{"thingId": "thing-1", "variantId": "other"}
	if _, err := NewEngine([]Rule{variantRule}).Transform(withoutVariant); err == nil {
		t.Fatal("avvikande variant ska ge no-match")
	}
}

// Explicit borttag (removed[]) undertrycker snapshot-värde för samma
// egenskap: snapshot.humidity + removed=[humidity] skriver aldrig hum.
// Ogiltig/saknad/borttagen rensar aldrig; rensning kräver explicit
// removeAttributes-regel.
func TestERemovedSuppressesSnapshot(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	th := eValuesThing(
		map[string]any{},
		map[string]any{"humidity": mk(55)},
		[]string{"humidity"},
	)
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{eProp("hum", "snapshot.humidity.value", false)},
		}},
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	if len(writes) != 0 {
		t.Fatalf("removed ska undertrycka snapshot-skrivning: %+v", writes)
	}
}

// Härledd aktivering: ändrad delta-ingång kräver andra snapshotvärden utan
// att allt skrivs. sum(delta.temp, snapshot.hum) beräknas från båda vyerna
// och skriver endast derived, ej orelaterade snapshot-attribut.
func TestEDeriveActivationExplicit(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	th := eValuesThing(
		map[string]any{"temperature": mk(22)},
		map[string]any{"temperature": mk(22), "humidity": mk(50)},
		nil,
	)
	rule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{{
				Target: "comfort", Type: TypeNumber,
				Derive: &Derive{Fn: FnSum, Of: []Source{{Field: "delta.temperature.value"}, {Field: "snapshot.humidity.value"}}},
			}},
		}},
	}
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil || len(writes) != 1 {
		t.Fatalf("derive: %v %v", writes, err)
	}
	ent, err := BuildEntity(writes[0], "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	raw, _ := jsonMarshal(ent)
	if !containsStr(raw, `"comfort"`) || !containsStr(raw, "72") {
		t.Fatalf("derive sum(22,50)=72 saknas: %s", raw)
	}
}

// Create-fallback: giltig PATCH (delta-temp) är inte giltig full create när
// required snapshot-hum saknas. Samma indata ger merge-skrivning men ingen
// create-skrivning.
func TestECreateFallbackNeedsSnapshot(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	th := eValuesThing(
		map[string]any{"temperature": mk(22)},
		map[string]any{"temperature": mk(22)},
		nil,
	)
	mergeRule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{eProp("temp", "delta.temperature.value", false)},
		}},
	}
	if writes, err := NewEngine([]Rule{mergeRule}).Transform(th); err != nil || len(writes) != 1 {
		t.Fatalf("delta-PATCH ska ge merge: %v %v", writes, err)
	}
	createRule := Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic", Create: true,
			Properties: []Property{
				eProp("temp", "snapshot.temperature.value", true),
				eProp("hum", "snapshot.humidity.value", true),
			},
		}},
	}
	if writes, err := NewEngine([]Rule{createRule}).Transform(th); err != nil || len(writes) != 0 {
		t.Fatalf("ofullständig snapshot ska ge tom create (PATCH≠create): %v %v", writes, err)
	}
}

// Vald regel exponeras även vid undertryck (preview behöver identitet trots
// tomma writes); ExplainSkipped ger precis målroll/attribut/kod.
func TestEMatchedRuleAndSkipped(t *testing.T) {
	mk := func(v float64) map[string]any {
		return map[string]any{"value": v, "quality": "ok"}
	}
	th := eValuesThing(map[string]any{"a": mk(1)}, map[string]any{"a": mk(1)}, nil)
	rule := Rule{
		ID: "rule-1", Revision: 9, SnapshotVersion: 14,
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			Name: "primary", ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
			Properties: []Property{eProp("b", "values.b.value", true)},
		}},
	}
	engine := NewEngine([]Rule{rule})
	got, ok := engine.MatchedRule(th)
	if !ok || got.ID != "rule-1" || got.Revision != 9 || got.SnapshotVersion != 14 {
		t.Fatalf("MatchedRule: %+v %v", got, ok)
	}
	skipped := engine.ExplainSkipped(th)
	if len(skipped) == 0 || skipped[0].Code != CodeRequiredMissing {
		t.Fatalf("skipped: %+v", skipped)
	}
	if skipped[0].Entity != "primary" || skipped[0].Attribute != "b" || skipped[0].RuleID != "rule-1" {
		t.Fatalf("skip identitet: %+v", skipped[0])
	}
}

// Specificitet inkluderar version/variant (D kopplas in i E-match).
func TestESpecificityIncludesVersions(t *testing.T) {
	plain := Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"}
	versioned := plain
	versioned.ThingTypeVersion = "2"
	if versioned.Specificity() <= plain.Specificity() {
		t.Fatal("version ger ingen specificitet")
	}
	variant := plain
	variant.VariantID = "v"
	if variant.Specificity() <= plain.Specificity() {
		t.Fatal("variant ger ingen specificitet")
	}
}

// Motor → PendingWrite (F-PlanFunc-kontrakt): stabila idempotensnycklar,
// validerad serialiserad NGSI-LD före bygge, deterministisk replay.
func TestEPendingWritesStableKeys(t *testing.T) {
	rule := Rule{
		ID: "rule-1", Revision: 3, SnapshotVersion: 8,
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{
			{
				Name: "primary", ID: "urn:ngsi-ld:Generic:thing-1", Type: "Generic",
				Properties: []Property{{Target: "temp", Type: TypeNumber, Const: 1.0}},
			},
			{
				Name: "secondary", ID: "urn:ngsi-ld:Generic:thing-2", Type: "Generic",
				RemoveAttributes: []string{"stale"},
			},
		},
	}
	th := eValuesThing(map[string]any{}, map[string]any{}, nil)
	writes, err := NewEngine([]Rule{rule}).Transform(th)
	if err != nil || len(writes) != 2 {
		t.Fatalf("writes: %v %v", writes, err)
	}
	first, err := EntityWritesToPending("tenant-a", writes, "")
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	second, err := EntityWritesToPending("tenant-a", writes, "")
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(first) != 2 || first[0].IdempotencyKey != second[0].IdempotencyKey {
		t.Fatalf("instabila nycklar: %+v vs %+v", first, second)
	}
	if first[0].Operation != "merge" || first[1].Operation != "removeAttributes" {
		t.Fatalf("operationer: %+v", first)
	}
	if first[0].RuleID != "rule-1" || first[0].RuleRevision != 3 {
		t.Fatalf("regelidentitet: %+v", first[0])
	}
	if len(first[0].Payload) == 0 || len(first[1].Payload) == 0 {
		t.Fatal("tom payload")
	}
	// Ogiltig serialiserad output nekas före bygge (ingen nyckel/payload).
	bad := []EntityWrite{{EntityID: "relative-id", TypeName: "Generic"}}
	if _, err := EntityWritesToPending("tenant-a", bad, ""); err == nil {
		t.Fatal("ogiltig NGSI-LD ska nekas före I/O")
	}
}
