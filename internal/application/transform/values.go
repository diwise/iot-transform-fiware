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
)

// findRecord slår upp en post för en property. Mätvärden använder
// resource (+ valfritt object) eller metadata via name.
func (m Measurement) findRecord(p Property) (Record, bool) {
	for _, r := range m.Records {
		switch {
		case p.Resource != "":
			if r.Name != p.Resource {
				continue
			}
		case p.Name != "":
			if r.Name != p.Name {
				continue
			}
		default:
			continue
		}

		if p.Object != "" && r.Object != p.Object {
			continue
		}

		return r, true
	}

	return Record{}, false
}

// buildProperty skapar dekoratören för en property. Saknad källa ger
// (nil, nil) för valfria properties och ErrRequiredSourceMissing för
// required.
func buildProperty(p Property, m Measurement) (entities.EntityDecoratorFunc, error) {
	if p.Derive != nil {
		return buildDerived(p, m)
	}

	if p.LatLon {
		if !m.HasLatLon {
			if p.Required {
				return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
			}
			return nil, nil
		}
		return decorators.Location(m.Lat, m.Lon), nil
	}

	if p.Timestamp {
		return decorate(p, m.Timestamp.UTC().Format(rfc3339), "", "")
	}

	r, ok := m.findRecord(p)
	if !ok {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	raw, err := rawValue(p, r)
	if err != nil {
		return nil, err
	}

	raw, err = applyTransforms(p, raw)
	if err != nil {
		return nil, err
	}

	observedBy, err := renderObservedBy(p, m)
	if err != nil {
		return nil, err
	}

	return decorate(p, raw, observationTime(p, m, r), observedBy)
}

func renderObservedBy(p Property, m Measurement) (string, error) {
	if p.ObservedBy == "" {
		return "", nil
	}
	return renderTemplate(p.ObservedBy, m.templateVars(), nil)
}

// rawValue returnerar postens nativa värde. Typomvandling till propertyns
// måltyp sker efter transformstegen, så att t.ex. boolToOnOff kan arbeta på
// en bool.
func rawValue(p Property, r Record) (any, error) {
	switch {
	case r.Value != nil:
		return *r.Value, nil
	case r.Bool != nil:
		return *r.Bool, nil
	case r.String != nil:
		return *r.String, nil
	case r.HasTime:
		return r.Time.UTC().Format(rfc3339), nil
	default:
		return nil, fmt.Errorf("property %s: value is missing", p.Target)
	}
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
		prop, err := properties.UnmarshalP(map[string]any{"type": "Property", "value": b})
		if err != nil {
			return nil, err
		}
		return entities.P(p.Target, prop), nil
	case TypeDateTime:
		s, ok := toString(raw)
		if !ok {
			return nil, fmt.Errorf("property %s: expected date time", p.Target)
		}
		return decorators.DateTime(p.Target, s), nil
	default:
		return nil, fmt.Errorf("property %s: type %q is not supported for measurements yet", p.Target, p.Type)
	}
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
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

func textDecorator(name, value string, opts ...properties.TextPropertyDecoratorFunc) entities.EntityDecoratorFunc {
	tp := properties.NewTextProperty(value)
	for _, o := range opts {
		o(tp)
	}
	return entities.P(name, tp)
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
		f, ok := raw.(float64)
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
		f, ok := raw.(float64)
		if !ok {
			return nil, fmt.Errorf("floor expects a number")
		}
		return math.Floor(f), nil
	case OpRound:
		f, ok := raw.(float64)
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

// observationTime bestämmer propertyns observationstid enligt
// observedAt: header (default), record, message eller field:<namn>.
func observationTime(p Property, m Measurement, r Record) string {
	switch p.ObservedAt {
	case ObservedAtNone:
		return ""
	case ObservedAtRecord:
		if r.HasTime {
			return r.Time.UTC().Format(rfc3339)
		}
		return ""
	case ObservedAtMessage:
		return m.Timestamp.UTC().Format(rfc3339)
	case ObservedAtHeader, "":
		if m.HasHeaderTime {
			return m.HeaderTime.UTC().Format(rfc3339)
		}
		return m.Timestamp.UTC().Format(rfc3339)
	default:
		name := strings.TrimPrefix(string(p.ObservedAt), "field:")
		if rec, ok := m.findByRecordName(name); ok && rec.HasTime {
			return rec.Time.UTC().Format(rfc3339)
		}
		return ""
	}
}

func (m Measurement) findByRecordName(name string) (Record, bool) {
	for _, r := range m.Records {
		if r.Name == name {
			return r, true
		}
	}
	return Record{}, false
}

// buildDerived kombinerar flera källor med en namngiven funktion.
func buildDerived(p Property, m Measurement) (entities.EntityDecoratorFunc, error) {
	values := make([]any, 0, len(p.Derive.Of))

	for _, s := range p.Derive.Of {
		r, ok := m.findRecord(Property{Source: s})
		if !ok {
			continue
		}
		v, err := rawValue(p, r)
		if err != nil {
			continue
		}
		values = append(values, v)
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

	observedBy, err := renderObservedBy(p, m)
	if err != nil {
		return nil, err
	}

	return decorate(p, raw, observationTime(p, m, Record{}), observedBy)
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

const rfc3339 = "2006-01-02T15:04:05Z07:00"
