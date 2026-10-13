package transform

import (
	"errors"
	"fmt"
	"strings"
)

var validPropertyTypes = map[PropertyType]bool{
	TypeNumber:       true,
	TypeText:         true,
	TypeBoolean:      true,
	TypeDateTime:     true,
	TypeGeo:          true,
	TypeRelationship: true,
	TypeNumberArray:  true,
	TypeTextArray:    true,
	TypeStructured:   true,
	TypeLanguageMap:  true,
}

// Validate kör samtliga valideringar på en ordnad regellista.
func Validate(rules []Rule) error {
	type definition struct{ rule, entity int }
	definitions := map[string][]definition{}

	for i, r := range rules {
		if err := r.validate(); err != nil {
			return fmt.Errorf("rule %d (%q): %w", i, r.ID, err)
		}

		for j, e := range r.Entities {
			for _, previous := range definitions[e.ID] {
				other := rules[previous.rule]
				known := other.Entities[previous.entity]
				if known.Type != e.Type && (previous.rule == i || r.Match.overlaps(other.Match)) {
					return fmt.Errorf("rule %d (%q), entity %d: ID template %q has conflicting types %q and %q with rule %d (%q), entity %d", i, r.ID, j, e.ID, e.Type, known.Type, previous.rule, other.ID, previous.entity)
				}
			}
			definitions[e.ID] = append(definitions[e.ID], definition{rule: i, entity: j})
		}
	}

	return nil
}

func (r Rule) validate() error {
	if r.Match.Kind != KindMeasurement && r.Match.Kind != KindThing {
		return fmt.Errorf("match.kind must be %q or %q", KindMeasurement, KindThing)
	}

	if r.Match.Object != "" && !strings.HasPrefix(r.Match.Object, "urn:") {
		return fmt.Errorf("match.object %q must be a URN", r.Match.Object)
	}

	if err := r.Match.validateForKind(); err != nil {
		return err
	}

	if err := r.Match.validateRelationsMatch(); err != nil {
		return err
	}

	if err := r.Match.validateLifecycleMatch(); err != nil {
		return err
	}

	if err := r.validateDeleteMatch(); err != nil {
		return err
	}

	if len(r.Entities) == 0 {
		return errors.New("rule must declare at least one entity")
	}

	for i, e := range r.Entities {
		if err := e.validate(); err != nil {
			return fmt.Errorf("entity %d: %w", i, err)
		}
		if err := e.validateForRule(r.Match.Kind); err != nil {
			return fmt.Errorf("entity %d: %w", i, err)
		}
		if err := e.validateSourcesForKind(r.Match.Kind); err != nil {
			return fmt.Errorf("entity %d: %w", i, err)
		}
	}

	if err := r.validateDuplicateTargets(); err != nil {
		return err
	}

	return r.validateReferences()
}

// validateDuplicateTargets förbjuder dubblett-target i samma regel: två
// entiteter får inte dela identisk ID-malltext, även med samma typmall.
// Samma ID-/typmall får återanvändas mellan regler (konfliktkontrollen i
// Validate avvisar endast oförenliga typmallar vid överlapp); inom en regel
// vore två skrivningar mot samma målidentitet tvetydiga. Jämför malltext,
// renderar inga uttryck.
func (r Rule) validateDuplicateTargets() error {
	seen := map[string]int{}
	for i, e := range r.Entities {
		if prev, dup := seen[e.ID]; dup {
			return fmt.Errorf("entity %d: duplicate entity id template %q (also entity %d)", i, e.ID, prev)
		}
		seen[e.ID] = i
	}
	return nil
}

// validateReferences kontrollerar att {{entities.<namn>.id}} pekar på en
// deklarerad entitet i samma regel, vars ID redan har renderats.
func (r Rule) validateReferences() error {
	index := map[string]int{}
	for i, e := range r.Entities {
		if e.Name == "" {
			continue
		}
		if _, dup := index[e.Name]; dup {
			return fmt.Errorf("duplicate entity name %q", e.Name)
		}
		index[e.Name] = i
	}

	for i, e := range r.Entities {
		type expression struct{ path, value string }
		expressions := []expression{{"id", e.ID}, {"type", e.Type}}
		var properties func([]Property, string)
		properties = func(props []Property, parent string) {
			for _, p := range props {
				path := parent + "." + p.Target
				expressions = append(expressions, expression{path + ".value", p.Value}, expression{path + ".observedBy", p.ObservedBy})
				if p.Derive != nil {
					for j, source := range p.Derive.Of {
						expressions = append(expressions, expression{fmt.Sprintf("%s.derive.of[%d].value", path, j), source.Value})
					}
				}
				properties(p.Subproperties, path)
			}
		}
		properties(e.Properties, "properties")
		for _, expression := range expressions {
			names, err := entityReferences(expression.value)
			if err != nil {
				return fmt.Errorf("entity %d %s expression %q: %w", i, expression.path, expression.value, err)
			}
			for _, name := range names {
				j, ok := index[name]
				if !ok {
					return fmt.Errorf("entity %d %s expression %q references unknown entity %q", i, expression.path, expression.value, name)
				}
				if j == i && expression.path == "id" {
					return fmt.Errorf("entity %d id expression %q references itself", i, expression.value)
				}
				if j > i {
					return fmt.Errorf("entity %q is declared after it is referenced by entity %d %s expression %q", name, i, expression.path, expression.value)
				}
			}
		}
	}

	return nil
}

// validateForKind avvisar matchningsfält som inte gäller för regelns kind, så
// att en felskrivning inte tyst gör att regeln aldrig matchar.
func (mm Match) validateForKind() error {
	// Kända händelsekällor för versionerade event. Tomt = ursprunglig källa.
	if mm.Event != "" && mm.Event != EventThingsV1Values && mm.Event != EventThingsV1Relations && mm.Event != EventThingsV1Lifecycle {
		return fmt.Errorf("unknown match.event %q", mm.Event)
	}
	switch mm.Kind {
	case KindMeasurement:
		if mm.ToType != "" {
			return errors.New("match.toType requires things.v1.relations")
		}
		if mm.Type != "" || mm.SubType != "" {
			return errors.New("match.type/subType is only valid for things")
		}
		if mm.ThingTypeVersion != "" || mm.VariantID != "" || mm.VariantVersion != "" {
			return errors.New("match.thingTypeVersion/variantId/variantVersion is only valid for things")
		}
		if mm.Event != "" {
			return errors.New("match.event is only valid for things")
		}
		if mm.Relation != "" || mm.RelationRemoved {
			return errors.New("match.relation/relationRemoved is only valid for things")
		}
		if mm.Lifecycle != "" {
			return errors.New("match.lifecycle is only valid for things")
		}
	case KindThing:
		if mm.Device != "" || mm.Port != "" || mm.SensorType != "" || mm.Object != "" || mm.Resource != "" || mm.Env != "" {
			return errors.New("match.device/port/sensorType/object/resource/env is only valid for measurements")
		}
		// Legacy thing-regler (tom event-tagg) är ersatta av things-v2-regler
		// (PLAN001 steg 7.7): tomma thing-regler matchar aldrig och blir
		// tyst dödvikt — fail fast i stället.
		if mm.Event == "" {
			return errors.New("match.event is required for things")
		}
		if (mm.Relation != "" || mm.RelationRemoved) && mm.Event != EventThingsV1Relations {
			return errors.New("match.relation/relationRemoved requires event things.v1.relations")
		}
		if mm.ToType != "" && mm.Event != EventThingsV1Relations {
			return errors.New("match.toType requires things.v1.relations")
		}
		if mm.RelationRemoved && mm.Relation == "" {
			return errors.New("match.relationRemoved requires match.relation")
		}
		if mm.Lifecycle != "" && mm.Event != EventThingsV1Lifecycle {
			return errors.New("match.lifecycle requires event things.v1.lifecycle")
		}
		// PLAN1050 D: version utan typ är meningslös. Tom version är
		// wildcard över alla versioner; angiven version kräver typ.
		// configRevision har inget matchfält (nekat som okänt fält).
		if mm.ThingTypeVersion != "" && mm.Type == "" {
			return errors.New("match.thingTypeVersion requires match.type")
		}
	}
	return nil
}

// validateRelationsMatch kräver typavgränsning på relationsregler: utan
// match.type/subType skulle regeln träffa alla sakers länkar (även från
// producenter utan typfält), och value-mallar med saknade fält skulle ge
// omsändningsloop i stället för skip.
func (mm Match) validateRelationsMatch() error {
	if mm.Kind == KindThing && mm.Event == EventThingsV1Relations {
		if mm.Type == "" && mm.SubType == "" {
			return errors.New("match.type/subType is required for relations rules")
		}
	}
	return nil
}

// validateLifecycleMatch avvisar okända livscykeltyper: endast
// created/updated/deleted finns i producentens kontrakt.
func (mm Match) validateLifecycleMatch() error {
	if mm.Kind == KindThing && mm.Event == EventThingsV1Lifecycle {
		switch LifecycleKind(mm.Lifecycle) {
		case LifecycleCreated, LifecycleUpdated, LifecycleDeleted:
		default:
			return fmt.Errorf("match.lifecycle %q must explicitly be created, updated or deleted", mm.Lifecycle)
		}
	}
	return nil
}

// validateDeleteMatch kräver match.lifecycle: deleted för delete-regler:
// radering är en deleted-mekanik och får aldrig aktiveras av
// created/updated/värdes/relations-händelser. Korskontrollen ligger här
// (inte i Entity.validate) eftersom den behöver både Match och Entities.
func (r Rule) validateDeleteMatch() error {
	for _, e := range r.Entities {
		if !e.Delete {
			continue
		}
		if r.Match.Kind == KindThing && r.Match.Event == EventThingsV1Lifecycle && r.Match.Lifecycle == string(LifecycleDeleted) {
			continue
		}
		return errors.New("delete requires match.lifecycle: deleted")
	}
	return nil
}

func (e Entity) validate() error {
	if e.ID == "" {
		return errors.New("entity.id is required")
	}
	if e.Type == "" {
		return errors.New("entity.type is required")
	}
	if err := validateTemplate(e.ID); err != nil {
		return fmt.Errorf("entity.id: %w", err)
	}
	if err := validateTemplate(e.Type); err != nil {
		return fmt.Errorf("entity.type: %w", err)
	}
	if err := e.validateStatic(); err != nil {
		return err
	}

	seen := map[string]bool{}
	for i, p := range e.Properties {
		if err := validateAttributeName(p.Target); err != nil {
			return fmt.Errorf("property %d: %w", i, err)
		}
		if seen[p.Target] {
			return fmt.Errorf("duplicate property target %q", p.Target)
		}
		seen[p.Target] = true
		if err := p.validate(); err != nil {
			return fmt.Errorf("property %d (%s): %w", i, p.Target, err)
		}
	}

	return nil
}

func validateAttributeName(name string) error {
	switch name {
	case "id", "type", "@context", "createdAt", "modifiedAt", "deletedAt":
		return fmt.Errorf("attribute %q collides with NGSI-LD structural field", name)
	}
	if err := validateTerm(name); err != nil {
		return fmt.Errorf("attribute name: %w", err)
	}
	return nil
}

// validateSourcesForKind avvisar kind-fel källor: mätvärdeskällor
// (resource/object/env/name) på thing-regler och sakällor (field/ref) på
// mätvärdesregler skippas i dag tyst i motorn (Resolve hittar inget) —
// fail fast i stället. Rekursiv över Derive.Of och subproperties, som alla
// går via in.Resolve.
func (e Entity) validateSourcesForKind(kind Kind) error {
	for i, p := range e.Properties {
		if err := p.validateSourcesForKind(kind); err != nil {
			return fmt.Errorf("property %d (%s): %w", i, p.Target, err)
		}
	}
	return nil
}

func (p Property) validateSourcesForKind(kind Kind) error {
	for _, expr := range []string{p.Value, p.ObservedBy} {
		if err := validateTemplate(expr); err != nil {
			return err
		}
	}
	if err := validateSourceKind(p.Source, kind); err != nil {
		return err
	}
	if p.Derive != nil {
		for i, s := range p.Derive.Of {
			if err := validateSourceKind(s, kind); err != nil {
				return fmt.Errorf("derive.of %d: %w", i, err)
			}
		}
	}
	for i, sp := range p.Subproperties {
		if err := sp.validateSourcesForKind(kind); err != nil {
			return fmt.Errorf("subproperty %d (%s): %w", i, sp.Target, err)
		}
	}
	return nil
}

func validateSourceKind(s Source, kind Kind) error {
	if err := validateTemplate(s.Value); err != nil {
		return err
	}
	for _, path := range []string{s.Field, s.Ref} {
		if path != "" {
			if _, err := parsePath(path); err != nil {
				return err
			}
			if err := validateFieldPathForKind(path, kind); err != nil {
				return err
			}
		}
	}
	switch kind {
	case KindThing:
		if s.Resource != "" {
			return errors.New("resource is only valid for measurements")
		}
		if s.Object != "" {
			return errors.New("object is only valid for measurements")
		}
		if s.Env != "" {
			return errors.New("env is only valid for measurements")
		}
		if s.Name != "" {
			return errors.New("name is only valid for measurements")
		}
	case KindMeasurement:
		if s.Field != "" {
			return errors.New("field is only valid for things")
		}
		if s.Ref != "" {
			return errors.New("ref is only valid for things")
		}
	}
	return nil
}

// validateFieldPathForKind skiljer statiskt omöjliga fältvägar från
// dynamiska/valfria källor (PLAN1050 D). Syntax fel avvisas redan av
// parsePath. Här avvisas endast strukturellt omöjliga konstruktioner:
//
//   - values/snapshot/delta kräver egenskaps-ID (minst två steg) och är
//     mappar, inte listor (values[0] avvisas). Bladnamn och index efter
//     egenskaps-ID är dynamiska: okända blad är saknad källa vid körning
//     (skip/required), inte ett statiskt fel. Citerade nycklar
//     (values."a.b".value) och [n]-index är giltiga generella former.
//   - observations[]/relations[] är listor med proveniens utan värden;
//     alla former accepteras statiskt (metadata-bypass ger skip, inte fel).
//   - Övriga rötter (descriptor/thing/event/relation/lifecycle/geometry/
//     location/metadata samt standardgenvägar) är dynamiska descriptor-/
//     kuvertfält; okända rötter förblir dynamiska för framåtkompatibilitet.
//     Fria metadata/repeat ger aldrig falsk fullständig garanti.
//
// Mätvärden använder inte field/ref (avvisas ovan); deras {{}}-mallar
// förblir dynamiska (record-namn är datadrivna).
func validateFieldPathForKind(path string, kind Kind) error {
	if kind != KindThing {
		return nil
	}
	steps, err := parsePath(path)
	if err != nil {
		return err
	}
	if len(steps) == 0 {
		return fmt.Errorf("invalid field path %q", path)
	}
	switch steps[0].key {
	case "values", "snapshot", "delta":
		if len(steps) < 2 {
			return fmt.Errorf("field %q requires a property id (e.g. %s.<id>.value)", path, steps[0].key)
		}
		if steps[1].indexed {
			return fmt.Errorf("field %q: %s is a map by property id, not a list", path, steps[0].key)
		}
	}
	return nil
}

// validateForRule kontrollerar entitet mot regelns kind: removeAttributes
// är en thing-mekanik (attributrensning) som saknar mening för mätvärden,
// kräver namngivna attribut och utesluter värdeskrivning i samma entitet.
// Delete är en deleted-mekanik med samma exklusivitet (extend i
// validateDeleteMatch kräver dessutom match.lifecycle: deleted).
func (e Entity) validateForRule(kind Kind) error {
	if e.Delete {
		if kind != KindThing {
			return errors.New("delete is only valid for things")
		}
		if e.Create {
			return errors.New("delete cannot be combined with create")
		}
		if len(e.RemoveAttributes) > 0 {
			return errors.New("delete cannot be combined with removeAttributes")
		}
		if len(e.Properties) > 0 {
			return errors.New("delete cannot be combined with properties")
		}
		return nil
	}
	if len(e.RemoveAttributes) == 0 {
		return nil
	}
	if kind != KindThing {
		return errors.New("removeAttributes is only valid for things")
	}
	seen := map[string]bool{}
	for _, name := range e.RemoveAttributes {
		if err := validateAttributeName(name); err != nil {
			return err
		}
		if seen[name] {
			return fmt.Errorf("duplicate removeAttributes target %q", name)
		}
		seen[name] = true
	}
	if e.Create {
		return errors.New("removeAttributes cannot be combined with create")
	}
	if len(e.Properties) > 0 {
		return errors.New("removeAttributes cannot be combined with properties")
	}
	return nil
}

func (p Property) validate() error {
	if p.Array && p.Type != TypeRelationship {
		return errors.New("array is only valid for Relationship")
	}
	if p.LatLon {
		if p.Type != TypeGeo {
			return errors.New("latlon requires Geo target type")
		}
		if len(p.Transform) != 0 || p.Prefix != "" {
			return errors.New("transform/prefix is not supported for latlon")
		}
	}
	if p.Timestamp {
		if p.Type != TypeDateTime && p.Type != TypeText {
			return errors.New("timestamp requires DateTime or Text target type")
		}
		if len(p.Transform) != 0 || p.Prefix != "" || p.ObservedBy != "" {
			return errors.New("modifiers/metadata are not supported for timestamp")
		}
	}
	if p.Target == "" {
		return errors.New("target is required")
	}

	if !validPropertyTypes[p.Type] {
		return fmt.Errorf("unknown property type %q", p.Type)
	}

	// LanguageMap tar endast råa språkmappor (field:/const:) — transform
	// och derive avvisas i v1 (ingen definierad semantik över mappar).
	if p.Type == TypeLanguageMap {
		if p.Prefix != "" || p.ObservedBy != "" {
			return errors.New("prefix/observedBy metadata is not supported for languageMap")
		}
		if len(p.Transform) > 0 || p.Derive != nil {
			return errors.New("transform/derive is not supported for languageMap")
		}
		if p.Field == "" && p.Const == nil {
			return errors.New("languageMap requires field or const")
		}
	}

	if p.ObservedAt != "" && !strings.HasPrefix(string(p.ObservedAt), "field:") && !validObservedAt[p.ObservedAt] {
		return fmt.Errorf("unknown observedAt %q", p.ObservedAt)
	}
	if after, ok := strings.CutPrefix(string(p.ObservedAt), "field:"); ok {
		if _, err := parsePath(after); err != nil {
			return err
		}
	}

	if p.When != "" && p.When != WhenPresent {
		return fmt.Errorf("unknown when %q", p.When)
	}

	if p.Unit != "" && p.Type != TypeNumber && p.Type != TypeNumberArray {
		return fmt.Errorf("unit is only valid for Number and NumberArray properties")
	}
	if p.Unit != "" {
		if err := validateUnitCode(p.Unit); err != nil {
			return err
		}
	}

	if p.ObservedBy != "" && p.Type != TypeNumber {
		return fmt.Errorf("observedBy is only valid for Number properties")
	}
	if err := p.validateConversion(); err != nil {
		return err
	}

	if p.Value != "" {
		if err := validateTemplate(p.Value); err != nil {
			return err
		}
	}

	if p.ObservedBy != "" {
		if err := validateTemplate(p.ObservedBy); err != nil {
			return err
		}
	}

	if p.Type == TypeStructured {
		if p.Source != (Source{}) || p.Derive != nil || len(p.Transform) != 0 || p.Convert != nil || p.ObservedBy != "" {
			return errors.New("structured uses subproperties only; source/modifiers/metadata are not supported")
		}
		if len(p.Subproperties) == 0 {
			return errors.New("a structured property must declare subproperties")
		}
		seen := map[string]bool{}
		for i, sp := range p.Subproperties {
			if sp.Unit != "" || sp.ObservedAt != "" || sp.ObservedBy != "" {
				return fmt.Errorf("subproperty %d (%s): unit/observedAt/observedBy metadata is not supported on Structured values", i, sp.Target)
			}
			if sp.Array {
				return fmt.Errorf("subproperty %d (%s): array is only supported on NGSI-LD Relationship attributes", i, sp.Target)
			}
			if seen[sp.Target] {
				return fmt.Errorf("duplicate subproperty target %q", sp.Target)
			}
			seen[sp.Target] = true
			if sp.Type == TypeStructured {
				return fmt.Errorf("subproperty %d (%s): structured may not be nested", i, sp.Target)
			}
			if err := sp.validate(); err != nil {
				return fmt.Errorf("subproperty %d (%s): %w", i, sp.Target, err)
			}
		}
		return nil
	}

	if len(p.Subproperties) > 0 {
		return errors.New("subproperties are only valid for structured properties")
	}

	if err := p.validateSource(); err != nil {
		return err
	}

	if err := p.validateFunctions(); err != nil {
		return err
	}
	return p.validateStatic()
}

var validObservedAt = map[ObservedAt]bool{
	ObservedAtHeader:  true,
	ObservedAtRecord:  true,
	ObservedAtMessage: true,
	ObservedAtNone:    true,
}

// validateSource säkerställer att exakt en primär värdkälla är angiven.
func (p Property) validateSource() error {
	if (p.Object != "" || p.Env != "") && p.Resource == "" && p.Name == "" {
		return errors.New("object/env selectors require resource or name")
	}
	primary := 0
	if p.Resource != "" {
		primary++
	}
	if p.Field != "" {
		primary++
	}
	if p.Ref != "" {
		primary++
	}
	if p.Name != "" {
		primary++
	}
	if p.Const != nil {
		primary++
	}
	if p.LatLon {
		primary++
	}
	if p.Timestamp {
		primary++
	}
	if p.Value != "" {
		primary++
	}
	if p.Derive != nil {
		primary++
	}

	if primary == 0 {
		return errors.New("a value source is required")
	}
	if primary > 1 {
		return errors.New("exactly one value source is allowed")
	}

	return nil
}
