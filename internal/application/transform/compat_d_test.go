package transform

import (
	"strings"
	"testing"
)

// PLAN1050 D: generella positiva fall och verkliga fel med i övrigt giltiga
// regler. Inga standardmodellnamn i motorn: generiska typer/URN-prefix
// lever endast i regler/testdata.

// Generisk giltig things-regel (neutral mall, neutrala ID:n).
func genericThingRule() Rule {
	return Rule{
		Match: Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"},
		Entities: []Entity{{
			Name: "primary",
			ID:   "urn:ngsi-ld:Generic:{{id}}",
			Type: "Generic",
			Properties: []Property{
				{Target: "reading", Type: TypeNumber, Field: "values.temperature.value", Required: true},
				{Target: "label", Type: TypeText, Field: "name", ObservedAt: ObservedAtNone},
			},
		}},
	}
}

func TestDVersionVariantAdditive(t *testing.T) {
	base := genericThingRule()
	if err := Validate([]Rule{base}); err != nil {
		t.Fatalf("additive baseline must stay valid: %v", err)
	}

	// Exakta version/variant-filter accepteras.
	withVersion := genericThingRule()
	withVersion.Match.ThingTypeVersion = "2"
	withVersion.Match.VariantID = "variant-a"
	withVersion.Match.VariantVersion = "3"
	if err := Validate([]Rule{withVersion}); err != nil {
		t.Fatalf("version/variant rule: %v", err)
	}

	// Version utan typ avvisas (tom version matchar alla men kräver typ).
	bad := genericThingRule()
	bad.Match.Type = ""
	bad.Match.ThingTypeVersion = "2"
	if err := Validate([]Rule{bad}); err == nil {
		t.Fatal("thingTypeVersion without type accepted")
	}

	// Version/variant endast för things.
	meas := validRule()
	meas.Match.ThingTypeVersion = "1"
	if err := Validate([]Rule{meas}); err == nil {
		t.Fatal("measurement version accepted")
	}
	meas = validRule()
	meas.Match.VariantID = "v"
	if err := Validate([]Rule{meas}); err == nil {
		t.Fatal("measurement variant accepted")
	}

	// Tom variant = wildcard (giltig); tom version = wildcard (kräver typ, som finns).
	wild := genericThingRule()
	wild.Match.ThingTypeVersion = ""
	wild.Match.VariantID = ""
	wild.Match.VariantVersion = ""
	if err := Validate([]Rule{wild}); err != nil {
		t.Fatalf("wildcards: %v", err)
	}
}

func TestDOverlapIncludesVersions(t *testing.T) {
	a := Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", ThingTypeVersion: "1"}
	b := Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", ThingTypeVersion: "2"}
	if a.overlaps(b) {
		t.Fatal("different versions overlap")
	}
	b.ThingTypeVersion = ""
	if !a.overlaps(b) {
		t.Fatal("wildcard version must overlap")
	}
	a = Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", VariantID: "a"}
	b = Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template", VariantID: "b"}
	if a.overlaps(b) {
		t.Fatal("different variants overlap")
	}
	// Specificitet: version/variant ger +1 vardera.
	plain := Match{Kind: KindThing, Event: EventThingsV1Values, Type: "generic-template"}
	specific := plain
	specific.ThingTypeVersion = "1"
	specific.VariantID = "v"
	if specific.specificityD() <= plain.specificityD() {
		t.Fatal("version/variant adds no specificity")
	}
}

func TestDDeltaSnapshotSourceSyntax(t *testing.T) {
	for _, path := range []string{
		"values.temperature.value",
		`values."reading.average".value`,
		`values."reading.average".observedAt`,
		"snapshot.temperature.value",
		`snapshot."a.b".unit`,
		"delta.humidity.value",
		"descriptor.name",
		"thing.variantId",
		"event.publishedAt",
		"event.thingTypeVersion",
		"observations[0].quality",
		"relations[0].toId",
		"relation.to.thingId",
		"lifecycle.changedAt",
		"geometry",
		"location",
		"metadata.description",
		"name",
	} {
		r := genericThingRule()
		r.Entities[0].Properties = []Property{{Target: "sample", Type: TypeText, Field: path, ObservedAt: ObservedAtNone}}
		if err := Validate([]Rule{r}); err != nil {
			t.Fatalf("source %q: %v", path, err)
		}
	}
	// Statiskt omöjliga: bar values/snapshot/delta utan egenskaps-ID,
	// samt listindex på mapp.
	for _, path := range []string{"values", "snapshot", "delta", "values[0].value"} {
		r := genericThingRule()
		r.Entities[0].Properties = []Property{{Target: "sample", Type: TypeText, Field: path, ObservedAt: ObservedAtNone}}
		if err := Validate([]Rule{r}); err == nil {
			t.Fatalf("impossible source %q accepted", path)
		}
	}
	// {{}}-mallar med safeURI/firstPresent och citerade/indexerade vägar.
	r := genericThingRule()
	r.Entities[0].Properties = []Property{{Target: "label", Type: TypeText,
		Value: `{{safeURI(firstPresent(field:values."a.b".value, name))}}`, ObservedAt: ObservedAtNone}}
	if err := Validate([]Rule{r}); err != nil {
		t.Fatalf("template source: %v", err)
	}
}

func TestDDynamicVsImpossible(t *testing.T) {
	// Dynamisk okänd egenskap under values: giltig statiskt (valfri skip),
	// required ger undertryck vid körning — inte statiskt fel.
	r := genericThingRule()
	r.Entities[0].Properties[0] = Property{Target: "reading", Type: TypeNumber, Field: "values.unknown-sensor.value"}
	if err := Validate([]Rule{r}); err != nil {
		t.Fatalf("dynamic unknown property must be static-valid: %v", err)
	}
	// Fria metadata/repeat ger aldrig falsk fullständig garanti: tom
	// observations-metadata är giltig regel men ger inga värden.
	r.Entities[0].Properties[0] = Property{Target: "reading", Type: TypeNumber, Field: "observations[0].quality"}
	if err := Validate([]Rule{r}); err != nil {
		t.Fatalf("free metadata must be static-valid: %v", err)
	}
	writes, err := NewEngine([]Rule{r}).Transform(Thing{ID: "thing-1", Type: "generic-template", Event: EventThingsV1Values, Fields: map[string]any{}})
	if err != nil || len(writes) != 0 {
		t.Fatalf("free metadata must skip, not fail: %v %v", writes, err)
	}
	// Omöjligt: array på icke-Relationship, unit på Text, struktur utan delar.
	for _, mutate := range []func(*Rule){
		func(r *Rule) {
			r.Entities[0].Properties[0] = Property{Target: "x", Type: TypeText, Array: true, Const: "a"}
		},
		func(r *Rule) {
			r.Entities[0].Properties[0] = Property{Target: "x", Type: TypeText, Unit: "CEL", Const: "a"}
		},
		func(r *Rule) { r.Entities[0].Properties[0] = Property{Target: "x", Type: TypeStructured} },
	} {
		bad := genericThingRule()
		mutate(&bad)
		if err := Validate([]Rule{bad}); err == nil {
			t.Fatal("impossible construction accepted")
		}
	}
}

func TestDTargetRoles(t *testing.T) {
	// Positivt: bakåtreferens till tidigare roll + eget renderat ID i övriga uttryck.
	r := genericThingRule()
	second := Entity{Name: "secondary", ID: "urn:ngsi-ld:GenericSecondary:{{entities.primary.id}}", Type: "GenericSecondary",
		Properties: []Property{{Target: "label", Type: TypeText, Value: "{{entities.primary.id}}", ObservedAt: ObservedAtNone}}}
	r.Entities = append(r.Entities, second)
	if err := Validate([]Rule{r}); err != nil {
		t.Fatalf("backward role reference: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Rule)
		want   string
	}{
		{"unknown role", func(r *Rule) { r.Entities[0].ID = "urn:ngsi-ld:Generic:{{entities.missing.id}}" }, "unknown entity"},
		{"forward ref", func(r *Rule) {
			first := r.Entities[0]
			first.Name = "first"
			first.ID = "urn:ngsi-ld:Generic:{{entities.second.id}}"
			second := Entity{Name: "second", ID: "urn:ngsi-ld:Generic:{{id}}", Type: "Generic",
				Properties: []Property{{Target: "label", Type: TypeText, Const: "x"}}}
			r.Entities = []Entity{first, second}
		}, "declared after"},
		{"self ref in id", func(r *Rule) {
			r.Entities[0].Name = "self"
			r.Entities[0].ID = "urn:ngsi-ld:Generic:{{entities.self.id}}"
		}, "references itself"},
		{"duplicate name", func(r *Rule) {
			r.Entities[0].Name = "same"
			dup := r.Entities[0]
			dup.ID = "urn:ngsi-ld:GenericOther:{{id}}"
			r.Entities = append(r.Entities, dup)
		}, "duplicate entity name"},
		{"duplicate target", func(r *Rule) {
			dup := r.Entities[0]
			dup.Name = "other"
			r.Entities = append(r.Entities, dup)
		}, "duplicate entity id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bad := genericThingRule()
			// Håll regeln i övrigt giltig.
			tc.mutate(&bad)
			if err := Validate([]Rule{bad}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q, got %v", tc.want, err)
			}
		})
	}
}

func TestDCompatibilityPositive(t *testing.T) {
	rules := []Rule{genericThingRule()}
	req := CompatRequest{
		Template: TemplateVersion{ThingType: "generic-template", ThingTypeVersion: "2", VariantID: "variant-a", VariantVersion: "3"},
		Sources: SourceContract{
			Properties: []PropertyContract{{ID: "temperature", Unit: "CEL"}},
			Geometry:   true, Location: true,
			Metadata: []string{"description"},
		},
		Rules: rules, Tenant: "tenant-a", ThingID: "thing-1", ConfigRevision: 7,
	}
	res := CheckCompatibility(req)
	if !res.Compatible || len(res.Diagnostics) != 0 {
		t.Fatalf("positive compat: %+v", res)
	}

	// Generisk positiv med snapshot/delta/citerade vägar och flera mål.
	multi := genericThingRule()
	multi.Match.ThingTypeVersion = "2"
	multi.Entities[0].Properties = []Property{
		{Target: "snapshotReading", Type: TypeNumber, Field: "snapshot.temperature.value"},
		{Target: "deltaReading", Type: TypeNumber, Field: "delta.humidity.value"},
		{Target: "quoted", Type: TypeNumber, Field: `values."reading.average".value`},
		{Target: "derived", Type: TypeNumber, Derive: &Derive{Fn: FnSum, Of: []Source{{Field: "values.temperature.value"}, {Const: 1.0}}}},
	}
	multi.Entities = append(multi.Entities, Entity{
		Name: "extra", ID: "urn:ngsi-ld:GenericExtra:{{id}}", Type: "GenericExtra",
		Properties: []Property{{Target: "label", Type: TypeText, Value: "{{safeURI(firstPresent(name, id))}}", ObservedAt: ObservedAtNone}},
	})
	req.Rules = []Rule{multi}
	req.Sources.Properties = []PropertyContract{{ID: "temperature"}, {ID: "humidity"}, {ID: "reading.average"}}
	res = CheckCompatibility(req)
	if !res.Compatible {
		t.Fatalf("multi-target compat: %+v", res.Diagnostics)
	}
}

func TestDCompatibilityErrors(t *testing.T) {
	baseSources := SourceContract{Properties: []PropertyContract{{ID: "temperature", Unit: "CEL"}}}
	cases := []struct {
		name     string
		template TemplateVersion
		mutate   func(*Rule)
		sources  SourceContract
		code     DiagnosticCode
	}{
		{"no-match version", TemplateVersion{ThingType: "generic-template", ThingTypeVersion: "9"}, func(r *Rule) {
			r.Match.ThingTypeVersion = "1"
		}, baseSources, CodeNoMatch},
		{"no-match variant", TemplateVersion{ThingType: "generic-template", VariantID: "other"}, func(r *Rule) {
			r.Match.VariantID = "variant-a"
		}, baseSources, CodeNoMatch},
		{"required-missing", TemplateVersion{ThingType: "generic-template"}, func(r *Rule) {
			r.Entities[0].Properties[0] = Property{Target: "reading", Type: TypeNumber, Field: "values.missing.value", Required: true}
		}, baseSources, CodeRequiredMissing},
		{"unit-conflict relabel", TemplateVersion{ThingType: "generic-template"}, func(r *Rule) {
			r.Entities[0].Properties[0] = Property{Target: "reading", Type: TypeNumber, Field: "values.temperature.value", Unit: "MTR"}
		}, baseSources, CodeUnitConflict},
		{"target-notfound relation", TemplateVersion{ThingType: "generic-template"}, func(r *Rule) {
			r.Match = Match{Kind: KindThing, Event: EventThingsV1Relations, Type: "generic-template", Relation: "missing-link"}
			r.Entities[0].Properties = []Property{{Target: "ref", Type: TypeRelationship, Value: "urn:ngsi-ld:Generic:{{relation.to.thingId}}"}}
		}, SourceContract{Relations: []RelationSlot{{Name: "known-link"}}}, CodeTargetNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := genericThingRule()
			if tc.mutate != nil {
				tc.mutate(&r)
			}
			// Håll i övrigt giltig: statisk Validate måste passera så att
			// felet kommer från kompatibilitet, inte från syntax.
			if err := Validate([]Rule{r}); err != nil {
				t.Fatalf("fixture must be static-valid: %v", err)
			}
			res := CheckCompatibility(CompatRequest{Template: tc.template, Sources: tc.sources, Rules: []Rule{r}, Tenant: "tenant-a", ThingID: "thing-1", ConfigRevision: 7})
			if res.Compatible {
				t.Fatalf("expected incompatible: %+v", res)
			}
			found := false
			for _, d := range res.Diagnostics {
				if d.Code == tc.code && d.Tenant == "tenant-a" && d.ThingID == "thing-1" && d.ConfigRevision == 7 {
					found = true
				}
			}
			if !found {
				t.Fatalf("expected code %q with tenant/thing/revision: %+v", tc.code, res.Diagnostics)
			}
		})
	}
}

func TestDCompatibilityTenantAndAmbiguous(t *testing.T) {
	a := genericThingRule()
	a.ID = "rule-a"
	a.Match.Tenant = "tenant-a"
	b := genericThingRule()
	b.ID = "rule-b"
	b.Match.Tenant = "tenant-a"
	res := CheckCompatibility(CompatRequest{
		Template: TemplateVersion{ThingType: "generic-template"},
		Rules:    []Rule{a, b}, Tenant: "tenant-a",
	})
	if res.Compatible {
		t.Fatalf("equal priority/specificity overlap must be ambiguous: %+v", res)
	}
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == CodeAmbiguousMatch {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing ambiguous-match: %+v", res.Diagnostics)
	}

	// Åtskilda tenants ger ingen konflikt (Validate) men tenant-denied vid
	// främmande efterfrågan syns i kompatibilitet.
	c := genericThingRule()
	c.Match.Tenant = "tenant-b"
	res = CheckCompatibility(CompatRequest{
		Template: TemplateVersion{ThingType: "generic-template"},
		Rules:    []Rule{c}, Tenant: "tenant-a",
	})
	if res.Compatible {
		t.Fatalf("tenant mismatch must deny: %+v", res)
	}
}

func TestDCreateRequiresSnapshot(t *testing.T) {
	r := genericThingRule()
	r.Entities[0].Create = true
	r.Entities[0].Properties[0].Required = true
	req := CompatRequest{
		Template: TemplateVersion{ThingType: "generic-template"},
		Sources:  SourceContract{Properties: []PropertyContract{{ID: "other"}}},
		Rules:    []Rule{r},
	}
	res := CheckCompatibility(req)
	found := false
	for _, d := range res.Diagnostics {
		if d.Code == CodeRequiredMissing {
			found = true
		}
	}
	if res.Compatible || !found {
		t.Fatalf("create without snapshot required source must fail: %+v", res)
	}
}

func TestDNGSILDProfileValidatesSerializedOutput(t *testing.T) {
	// Faktisk serialiserad output valideras; full context-expansion,
	// SDM-schema och brokerexistens verifieras INTE (dokumenterat).
	r := genericThingRule()
	r.Entities[0].Properties = []Property{{Target: "reading", Type: TypeNumber, Const: 1.5}}
	writes, err := NewEngine([]Rule{r}).Transform(Thing{ID: "thing-1", Type: "generic-template", Event: EventThingsV1Values, Fields: map[string]any{}})
	if err != nil || len(writes) != 1 {
		t.Fatalf("const output: %v %v", writes, err)
	}
	if err := ValidateEntityWrites(writes, ""); err != nil {
		t.Fatalf("serialized output invalid: %v", err)
	}
	// Ogiltig renderad NGSI-LD (relativ URI) avvisas före brokerkontakt.
	bad := genericThingRule()
	bad.Entities[0].ID = "relative-id"
	if err := Validate([]Rule{bad}); err == nil {
		t.Fatal("relative ID accepted statically")
	}
}
