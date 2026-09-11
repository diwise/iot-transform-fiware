package transform

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
	"github.com/diwise/context-broker/pkg/ngsild/types/relationships"
)

// buildProperty skapar dekoratören för en property. Saknad källa ger
// (nil, nil) för valfria properties och ErrRequiredSourceMissing för
// required.
func buildProperty(p Property, in Input, ids map[string]string) (entities.EntityDecoratorFunc, error) {
	if p.Derive != nil {
		return buildDerived(p, in)
	}

	if p.Type == TypeStructured {
		return buildStructured(p, in, ids)
	}

	if p.Const != nil || p.Value != "" {
		raw := p.Const
		if p.Value != "" {
			rendered, err := renderTemplate(p.Value, in, ids)
			if err != nil {
				return nil, err
			}
			raw = rendered
		}

		raw, err := applyTransforms(p, raw)
		if err != nil {
			return nil, err
		}

		observedBy, err := renderObservedBy(p, in)
		if err != nil {
			return nil, err
		}

		return decorate(p, applyPrefix(p, raw), observationTime(p, in, resolvedValue{}), observedBy)
	}

	if p.LatLon {
		lat, lon, ok := in.LatLon()
		if !ok {
			if p.Required {
				return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
			}
			return nil, nil
		}
		return decorators.Location(lat, lon), nil
	}

	if p.Timestamp {
		return decorate(p, in.MessageTime().UTC().Format(rfc3339), "", "")
	}

	rv, err := in.Resolve(p)
	if err != nil {
		return nil, err
	}
	if !rv.found {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	raw, err := applyTransforms(p, rv.value)
	if err != nil {
		return nil, err
	}

	observedBy, err := renderObservedBy(p, in)
	if err != nil {
		return nil, err
	}

	return decorate(p, applyPrefix(p, raw), observationTime(p, in, rv), observedBy)
}

// applyPrefix prependar en sträng till ett skalärt värde eller till varje
// element i en lista (t.ex. "urn:ngsi-ld:Device:").
func applyPrefix(p Property, raw any) any {
	if p.Prefix == "" {
		return raw
	}

	switch x := raw.(type) {
	case string:
		return p.Prefix + x
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			if s, ok := v.(string); ok {
				out[i] = p.Prefix + s
			} else {
				out[i] = v
			}
		}
		return out
	default:
		return raw
	}
}

func renderObservedBy(p Property, in Input) (string, error) {
	if p.ObservedBy == "" {
		return "", nil
	}
	return renderTemplate(p.ObservedBy, in, nil)
}

func decorate(p Property, raw any, observedAt, observedBy string) (entities.EntityDecoratorFunc, error) {
	switch p.Type {
	case TypeNumber:
		v, ok := toFloat(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected number", p.Target)
		}
		opts := make([]properties.NumberPropertyDecoratorFunc, 0, 3)
		if p.Unit != "" {
			opts = append(opts, properties.UnitCode(p.Unit))
		}
		if observedAt != "" {
			opts = append(opts, properties.ObservedAt(observedAt))
		}
		if observedBy != "" {
			opts = append(opts, properties.ObservedBy(observedBy))
		}
		return decorators.Number(p.Target, v, opts...), nil
	case TypeText:
		s, ok := toString(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected text", p.Target)
		}
		opts := make([]properties.TextPropertyDecoratorFunc, 0, 1)
		if observedAt != "" {
			opts = append(opts, properties.TxtObservedAt(observedAt))
		}
		return textDecorator(p.Target, s, opts...), nil
	case TypeBoolean:
		b, ok := toBool(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected boolean", p.Target)
		}
		return entities.P(p.Target, &genericProperty{Type_: "Property", Value_: b}), nil
	case TypeDateTime:
		s, ok := toString(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected date time", p.Target)
		}
		return decorators.DateTime(p.Target, s), nil
	case TypeGeo:
		lat, lon, ok := toLatLon(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected a location", p.Target)
		}
		return decorators.Location(lat, lon), nil
	case TypeRelationship:
		if arr, ok := raw.([]any); ok {
			objects := make([]string, 0, len(arr))
			for _, v := range arr {
				if s, ok := toString(v); ok {
					objects = append(objects, s)
				}
			}
			return entities.R(p.Target, relationships.NewMultiObjectRelationship(objects)), nil
		}
		s, ok := toString(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected a relationship value", p.Target)
		}
		return relationshipDecorator(p.Target, s), nil
	case TypeTextArray:
		ss, ok := toStringSlice(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected a list of text", p.Target)
		}
		tlp := properties.NewTextListProperty(ss)
		if observedAt != "" {
			tlp.ObservedAt_ = &observedAt
		}
		return entities.P(p.Target, tlp), nil
	case TypeNumberArray:
		fs, ok := toFloatSlice(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected a list of numbers", p.Target)
		}
		values := make([]any, len(fs))
		for i, f := range fs {
			values[i] = f
		}
		return entities.P(p.Target, &genericProperty{
			Type_: "Property", Value_: values, UnitCode: p.Unit, ObservedAt: observedAt,
		}), nil
	default:
		return nil, fmt.Errorf("property %s: type %q is not supported yet", p.Target, p.Type)
	}
}

func textDecorator(name, value string, opts ...properties.TextPropertyDecoratorFunc) entities.EntityDecoratorFunc {
	tp := properties.NewTextProperty(value)
	for _, o := range opts {
		o(tp)
	}
	return entities.P(name, tp)
}

func relationshipDecorator(name, value string) entities.EntityDecoratorFunc {
	return entities.R(name, relationships.NewSingleObjectRelationship(value))
}

// buildStructured bygger en NGSI-LD-property vars värde som ett objekt av
// delproperties.
func buildStructured(p Property, in Input, ids map[string]string) (entities.EntityDecoratorFunc, error) {
	obj := map[string]any{}

	for _, sp := range p.Subproperties {
		val, found, err := resolveSubValue(sp, in, ids)
		if err != nil {
			return nil, err
		}
		if !found {
			if sp.Required {
				return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
			}
			continue
		}

		val, err = applyTransforms(sp, val)
		if err != nil {
			return nil, err
		}

		obj[sp.Target] = applyPrefix(sp, val)
	}

	if len(obj) == 0 && p.Required {
		return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
	}

	return entities.P(p.Target, &genericProperty{Type_: "Property", Value_: obj}), nil
}

func resolveSubValue(p Property, in Input, ids map[string]string) (any, bool, error) {
	if p.Const != nil {
		return p.Const, true, nil
	}
	if p.Value != "" {
		rendered, err := renderTemplate(p.Value, in, ids)
		if err != nil {
			return nil, false, err
		}
		return rendered, true, nil
	}
	if p.LatLon {
		lat, lon, ok := in.LatLon()
		if !ok {
			return nil, false, nil
		}
		return map[string]any{"latitude": lat, "longitude": lon}, true, nil
	}
	if p.Timestamp {
		return in.MessageTime().UTC().Format(rfc3339), true, nil
	}

	rv, err := in.Resolve(p)
	if err != nil {
		return nil, false, err
	}
	return rv.value, rv.found, nil
}

// genericProperty täcker NGSI-LD-properties som biblioteket saknar
// färdiga konstruktorer för (bool, array av tal, strukturerade värden).
type genericProperty struct {
	Type_      string `json:"type"`
	Value_     any    `json:"value"`
	ObservedAt string `json:"observedAt,omitempty"`
	UnitCode   string `json:"unitCode,omitempty"`
}

func (g *genericProperty) Type() string { return g.Type_ }

func (g *genericProperty) Value() any { return g.Value_ }

func toStringSlice(v any) ([]string, bool) {
	switch x := v.(type) {
	case []string:
		return x, true
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			s, ok := toString(e)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	}
	return nil, false
}

func toFloatSlice(v any) ([]float64, bool) {
	switch x := v.(type) {
	case []float64:
		return x, true
	case []any:
		out := make([]float64, 0, len(x))
		for _, e := range x {
			f, ok := toFloat(e)
			if !ok {
				return nil, false
			}
			out = append(out, f)
		}
		return out, true
	}
	return nil, false
}

// applyTransforms kör transformstegen i ordning. Ett steg kan byta typ
// (t.ex. boolToOnOff: bool -> "on"/"off").
func applyTransforms(p Property, raw any) (any, error) {
	for _, t := range p.Transform {
		var err error
		raw, err = applyTransform(t, raw)
		if err != nil {
			return nil, fmt.Errorf("property %s: %w", p.Target, err)
		}
	}
	return raw, nil
}

func applyTransform(t Transform, raw any) (any, error) {
	switch t.Op {
	case OpMultiply, OpDivide, OpAdd:
		f, ok := toFloat(raw)
		if !ok {
			return nil, fmt.Errorf("%s expects a number", t.Op)
		}
		arg := *t.Value
		switch t.Op {
		case OpMultiply:
			f *= arg
		case OpDivide:
			if arg == 0 {
				return nil, errors.New("divide by zero")
			}
			f /= arg
		case OpAdd:
			f += arg
		}
		return f, nil
	case OpFloor:
		f, ok := toFloat(raw)
		if !ok {
			return nil, fmt.Errorf("floor expects a number")
		}
		return math.Floor(f), nil
	case OpRound:
		f, ok := toFloat(raw)
		if !ok {
			return nil, fmt.Errorf("round expects a number")
		}
		return math.Round(f), nil
	case OpBoolToOnOff:
		b, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("boolToOnOff expects a boolean")
		}
		if b {
			return "on", nil
		}
		return "off", nil
	case OpSafeURI:
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("safeURI expects text")
		}
		return url.PathEscape(strings.TrimSpace(s)), nil
	default:
		return nil, fmt.Errorf("unknown transform op %q", t.Op)
	}
}

// buildDerived kombinerar flera källor med en namngiven funktion.
func buildDerived(p Property, in Input) (entities.EntityDecoratorFunc, error) {
	values := make([]any, 0, len(p.Derive.Of))

	for _, s := range p.Derive.Of {
		rv, err := in.Resolve(Property{Source: s})
		if err != nil {
			return nil, err
		}
		if !rv.found {
			continue
		}
		values = append(values, rv.value)
	}

	// when: present innebär att en deriverad property bara skrivs om minst en
	// källa fanns. Utan when skrivs funktionens resultat även för tomma källor
	// (t.ex. anyTrue -> 0).
	if p.When == WhenPresent && len(values) == 0 {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	raw, err := deriveValue(p.Derive.Fn, values)
	if err != nil {
		return nil, fmt.Errorf("property %s: %w", p.Target, err)
	}

	if raw == nil {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	observedBy, err := renderObservedBy(p, in)
	if err != nil {
		return nil, err
	}

	return decorate(p, raw, observationTime(p, in, resolvedValue{}), observedBy)
}

func deriveValue(fn string, values []any) (any, error) {
	switch fn {
	case FnAnyTrue:
		for _, v := range values {
			if b, ok := toBool(v); ok && b {
				return true, nil
			}
		}
		return false, nil
	case FnAllTrue:
		if len(values) == 0 {
			return false, nil
		}
		for _, v := range values {
			if b, ok := toBool(v); ok && !b {
				return false, nil
			}
		}
		return true, nil
	case FnSum, FnMin, FnMax:
		nums := make([]float64, 0, len(values))
		for _, v := range values {
			if f, ok := toFloat(v); ok {
				nums = append(nums, f)
			}
		}
		if len(nums) == 0 {
			return nil, nil
		}
		result := nums[0]
		for _, f := range nums[1:] {
			switch fn {
			case FnSum:
				result += f
			case FnMin:
				if f < result {
					result = f
				}
			case FnMax:
				if f > result {
					result = f
				}
			}
		}
		return result, nil
	case FnCoalesce, FnFirstPresent:
		if len(values) == 0 {
			return nil, nil
		}
		return values[0], nil
	default:
		return nil, fmt.Errorf("unknown derive fn %q", fn)
	}
}

// observationTime bestämmer propertyns observationstid enligt
// observedAt: header (default), record, message, field:<väg> eller none.
func observationTime(p Property, in Input, rv resolvedValue) string {
	switch p.ObservedAt {
	case ObservedAtNone:
		return ""
	case ObservedAtRecord:
		if rv.hasTime {
			return rv.time.UTC().Format(rfc3339)
		}
		return ""
	case ObservedAtMessage:
		return in.MessageTime().UTC().Format(rfc3339)
	case ObservedAtHeader, "":
		if t, ok := in.BaseTime(); ok {
			return t.UTC().Format(rfc3339)
		}
		return in.MessageTime().UTC().Format(rfc3339)
	default:
		path := strings.TrimPrefix(string(p.ObservedAt), "field:")
		if t, ok := in.ResolveTime(path); ok {
			return t.UTC().Format(rfc3339)
		}
		return ""
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

func toString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(x), true
	}
	return "", false
}

func toBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case float64:
		return x != 0, true
	case string:
		b, err := strconv.ParseBool(x)
		return b, err == nil
	}
	return false, false
}

func toLatLon(v any) (float64, float64, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return 0, 0, false
	}
	lat, latOK := toFloat(m["latitude"])
	lon, lonOK := toFloat(m["longitude"])
	return lat, lon, latOK && lonOK
}

const rfc3339 = "2006-01-02T15:04:05Z07:00"
