package transform

import (
	"strconv"
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

	// TypeName och NameOrID är förberäknade med saks-konventionerna
	// (subType vinner över type; alternativa namn vinner över id).
	TypeName string
	NameOrID string

	ObservedAt time.Time
	Fields     map[string]any
}

func (t Thing) Kind() Kind { return KindThing }

func (t Thing) TenantID() string { return t.Tenant }

func (t Thing) MessageTime() time.Time { return t.ObservedAt }

func (t Thing) BaseTime() (time.Time, bool) { return t.ObservedAt, !t.ObservedAt.IsZero() }

func (t Thing) LatLon() (float64, float64, bool) {
	if v, ok := t.Fields["location"]; ok {
		return toLatLon(v)
	}
	return 0, 0, false
}

func (t Thing) MatchInfo() MatchInfo {
	return MatchInfo{
		Tenant:  t.Tenant,
		Type:    t.Type,
		SubType: t.SubType,
	}
}

func (t Thing) Variables() map[string]string {
	return map[string]string{
		"id":              t.ID,
		"type":            t.TypeName,
		"subType":         t.SubType,
		"name":            t.Name,
		"alternativeName": t.AlternativeName,
		"nameOrID":        t.NameOrID,
		"uuid":            trailingSegment(t.ID),
		"tenant":          t.Tenant,
		"timestamp":       t.ObservedAt.UTC().Format(rfc3339),
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

	v, ok := lookupPath(t.Fields, path)
	if !ok {
		return resolvedValue{}, nil
	}

	rv := resolvedValue{value: v, found: true}

	switch tv := v.(type) {
	case time.Time:
		rv.time, rv.hasTime = tv, true
	case string:
		if parsed, err := time.Parse(time.RFC3339, tv); err == nil {
			rv.time, rv.hasTime = parsed, true
		}
	}

	return rv, nil
}

func (t Thing) ResolveTime(path string) (time.Time, bool) {
	v, ok := lookupPath(t.Fields, path)
	if !ok {
		return time.Time{}, false
	}

	switch tv := v.(type) {
	case time.Time:
		return tv, true
	case string:
		if parsed, err := time.Parse(time.RFC3339, tv); err == nil {
			return parsed, true
		}
	}

	return time.Time{}, false
}

func (t Thing) ResolvePath(path string) (any, bool) {
	return lookupPath(t.Fields, path)
}

// lookupPath följer en dot-separerad väg med valfria [n]-index.
func lookupPath(root map[string]any, path string) (any, bool) {
	var cur any = root

	for _, seg := range strings.Split(path, ".") {
		name, idx, hasIdx := parseSegment(seg)

		if name != "" {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			v, ok := m[name]
			if !ok {
				return nil, false
			}
			cur = v
		}

		if hasIdx {
			arr, ok := cur.([]any)
			if !ok || idx < 0 || idx >= len(arr) {
				return nil, false
			}
			cur = arr[idx]
		}
	}

	if cur == nil {
		return nil, false
	}

	return cur, true
}

func parseSegment(seg string) (name string, idx int, hasIdx bool) {
	if i := strings.IndexByte(seg, '['); i >= 0 && strings.HasSuffix(seg, "]") {
		n, err := strconv.Atoi(seg[i+1 : len(seg)-1])
		if err != nil {
			return seg, 0, false
		}
		return seg[:i], n, true
	}
	return seg, 0, false
}

func trailingSegment(s string) string {
	if i := strings.LastIndexAny(s, ":/"); i >= 0 {
		return s[i+1:]
	}
	return s
}
