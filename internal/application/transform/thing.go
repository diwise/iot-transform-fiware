package transform

import (
	"strings"
	"time"
)

// Thing är motorns normaliserade indata för en sak. Fields är den råa
// JSON-representationen och läses via field-vägar (t.ex. temperature.v).
//
// PLAN1050 E: ThingTypeVersion/VariantID/VariantVersion bär explicit
// matchningsunderlag när adaptern sätter dem direkt (tester, C2). Tomma
// fält faller tillbaka till C1-källvyerna i Fields (event.thingTypeVersion,
// descriptor/thing.variantId/variantVersion, relation.fromThingTypeVersion
// m.fl.), så produktion via C1-adaptrar matchar utan C-ändringar. Ingen
// recency-/revisionsfiltrering; versioner är exakta matchfilter med tom
// wildcard, aldrig ordningsskydd.
type Thing struct {
	ID              string
	Type            string
	SubType         string
	Name            string
	AlternativeName string
	Description     string
	Tenant          string

	// TypeName är en explicit typgenväg; identiteten är alltid det råa ID:t.
	TypeName string

	// Event taggar händelsekällan för matchning (tomt = ursprunglig
	// thing.updated-källa; t.ex. "things.v1.values" för värdesevent).
	Event string

	// Relation avgränsar relationsregler till en relationstyp;
	// RelationRemoved skiljer bortkoppling från koppling.
	Relation        string
	RelationRemoved bool

	// Lifecycle bär livscykeltypen (created/updated/deleted) för
	// things.v1.lifecycle-händelser; tomt för övriga källor.
	Lifecycle string

	// ThingTypeVersion/VariantID/VariantVersion avgränsar things-regler
	// (PLAN1050 D). Tomt betyder explicit satt tomt; MatchInfo faller då
	// tillbaka till Fields-vyerna.
	ThingTypeVersion string
	VariantID        string
	VariantVersion   string

	ObservedAt time.Time
	Fields     map[string]any
}

func (t Thing) Kind() Kind { return KindThing }

func (t Thing) TenantID() string { return t.Tenant }

func (t Thing) MessageTime() time.Time {
	if t.Event == EventThingsV1Values {
		// J2(a): dateObserved (timestamp:true) är max-observedAt (mättid),
		// aldrig transporttid. ObservedAt sätts av C1 till max(delta[].observedAt)
		// med fallback max(snapshot[].observedAt); publishedAt används endast
		// när inga värdetider finns (deskriptor-bara event) så att timestamp
		// aldrig saknas tyst. event.publishedAt förblir åtkomlig via
		// {{event.publishedAt}} för diagnostik.
		if !t.ObservedAt.IsZero() {
			return t.ObservedAt
		}
		if value, ok := t.ResolveTime("event.publishedAt"); ok {
			return value
		}
		return time.Time{}
	}
	return t.ObservedAt
}

func (t Thing) BaseTime() (time.Time, bool) { return t.ObservedAt, !t.ObservedAt.IsZero() }

func (t Thing) LatLon() (float64, float64, bool) {
	if v, ok := t.Fields["location"]; ok {
		return toLatLon(v)
	}
	return 0, 0, false
}

func (t Thing) HasResource(string) bool { return false }

func (t Thing) MatchInfo() MatchInfo {
	toType, _ := lookupPath(t.Fields, "relation.to.type")
	rawType, _ := toType.(string)
	return MatchInfo{
		Tenant:           t.Tenant,
		Type:             t.Type,
		SubType:          t.SubType,
		Event:            t.Event,
		Relation:         t.Relation,
		RelationRemoved:  t.RelationRemoved,
		Lifecycle:        t.Lifecycle,
		ToType:           rawType,
		ThingTypeVersion: t.thingTypeVersion(),
		VariantID:        t.variantID(),
		VariantVersion:   t.variantVersion(),
	}
}

// thingTypeVersion föredrar explicit fält, annars C1-vyerna
// event.thingTypeVersion (values/lifecycle) och
// relation.fromThingTypeVersion (relations). Tomt är wildcard.
func (t Thing) thingTypeVersion() string {
	if t.ThingTypeVersion != "" {
		return t.ThingTypeVersion
	}
	for _, path := range []string{"event.thingTypeVersion", "relation.fromThingTypeVersion"} {
		if v, ok := lookupPath(t.Fields, path); ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// variantID föredrar explicit fält, annars descriptor/thing/relation-from.
func (t Thing) variantID() string {
	if t.VariantID != "" {
		return t.VariantID
	}
	for _, path := range []string{
		"descriptor.variantId", "thing.variantId",
		"relation.from.variantId", "relation.from.variantID",
	} {
		if v, ok := lookupPath(t.Fields, path); ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// variantVersion föredrar explicit fält, annars descriptor/thing/relation-from.
func (t Thing) variantVersion() string {
	if t.VariantVersion != "" {
		return t.VariantVersion
	}
	for _, path := range []string{
		"descriptor.variantVersion", "thing.variantVersion",
		"relation.from.variantVersion",
	} {
		if v, ok := lookupPath(t.Fields, path); ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// removedIDs samlar explicita rensningar (removed[]) som strängmängd.
// Saknad removed-nyckel ger tom mängd; ogiltiga former ignoreras här
// (C1 validerar struktur strikt) så att motorn aldrig kraschar på dem.
func (t Thing) removedIDs() map[string]bool {
	out := map[string]bool{}
	raw, ok := t.Fields["removed"]
	if !ok || raw == nil {
		return out
	}
	items, ok := raw.([]any)
	if !ok {
		return out
	}
	for _, item := range items {
		if s, ok := item.(string); ok && s != "" {
			out[s] = true
		}
	}
	return out
}

// isRemoved rapporterar om en field/ref-väg pekar på en egenskap listad i
// removed[]. Endast values/snapshot/delta-vägar prövas; descriptor/event/
// observations/relations-vägar påverkas aldrig.
func (t Thing) isRemoved(path string) bool {
	if path == "" || len(t.Fields) == 0 {
		return false
	}
	removed := t.removedIDs()
	if len(removed) == 0 {
		return false
	}
	if id, ok := valuesPropertyID(path); ok {
		return removed[id]
	}
	return false
}

func (t Thing) Variables() map[string]string {
	return map[string]string{
		"id":              t.ID,
		"type":            t.Type,
		"typeName":        t.TypeName,
		"subType":         t.SubType,
		"name":            t.Name,
		"alternativeName": t.AlternativeName,
		"uuid":            trailingSegment(t.ID),
		"tenant":          t.Tenant,
		"timestamp":       sourceTime(t.ObservedAt),
	}
}

func (t Thing) Resolve(p Property) (resolvedValue, error) {
	path := p.Field
	if path == "" {
		path = p.Ref
	}
	if path == "" {
		return resolvedValue{}, nil
	}
	// Explicit borttag undertrycker snapshot/delta-värde för samma egenskap:
	// removed[] ger skip (aldrig rensning); rensning kräver explicit
	// removeAttributes-/lifecycle-regel. Ogiltig obs når aldrig hit (C1
	// filtrerar), så den rensar aldrig giltigt attribut.
	if t.isRemoved(path) {
		return resolvedValue{}, nil
	}

	return resolveField(t.Fields, path), nil
}

func (t Thing) ResolveTime(path string) (time.Time, bool) {
	v, ok := lookupPath(t.Fields, path)
	if !ok {
		return time.Time{}, false
	}

	return parseSourceTime(v)
}

func (t Thing) ResolvePath(path string) (any, bool) {
	// Removed-suppression gäller även {{field:...}}-mallar och annan
	// väguppslagning: borttagen egenskap är saknad källa (skip), aldrig
	// rensning via värdeskrivning.
	if t.isRemoved(path) {
		return nil, false
	}
	return lookupPath(t.Fields, path)
}

// lookupPath följer en dot-separerad väg med valfria [n]-index.
// Nycklar som själva innehåller punkt citeras med dubbelfnutt
// (t.ex. values."temperature.average".value); ociterade vägar beter sig
// exakt som före (bakåtkompatibelt).
func lookupPath(root map[string]any, path string) (any, bool) {
	steps, err := parsePath(path)
	if err != nil {
		return nil, false
	}
	return lookupSteps(root, steps)
}

func lookupSteps(root map[string]any, steps []pathStep) (any, bool) {
	var cur any = root
	for _, step := range steps {
		if !step.indexed {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			v, ok := m[step.key]
			if !ok {
				return nil, false
			}
			cur = v
		} else {
			arr, ok := cur.([]any)
			if !ok || step.index >= len(arr) {
				return nil, false
			}
			cur = arr[step.index]
		}
	}

	if cur == nil {
		return nil, false
	}

	return cur, true
}

func trailingSegment(s string) string {
	if i := strings.LastIndexAny(s, ":/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
