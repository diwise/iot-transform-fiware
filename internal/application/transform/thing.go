package transform

import (
	"strings"
	"time"
)

// Thing är motorns normaliserade indata för en sak. Fields är den råa
// JSON-representationen och läses via field-vägar (t.ex. temperature.v).
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

	ObservedAt time.Time
	Fields     map[string]any
}

func (t Thing) Kind() Kind { return KindThing }

func (t Thing) TenantID() string { return t.Tenant }

func (t Thing) MessageTime() time.Time {
	if t.Event == EventThingsV1Values {
		value, _ := t.ResolveTime("event.publishedAt")
		return value
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
		Tenant:          t.Tenant,
		Type:            t.Type,
		SubType:         t.SubType,
		Event:           t.Event,
		Relation:        t.Relation,
		RelationRemoved: t.RelationRemoved,
		Lifecycle:       t.Lifecycle,
		ToType:          rawType,
	}
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
