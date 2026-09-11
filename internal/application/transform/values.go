package transform

import (
	"fmt"
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

func (r Record) asFloat() (float64, bool) {
	if r.Value != nil {
		return *r.Value, true
	}
	if r.String != nil {
		if f, err := strconv.ParseFloat(*r.String, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (r Record) asBool() (bool, bool) {
	if r.Bool != nil {
		return *r.Bool, true
	}
	if r.Value != nil {
		return *r.Value != 0, true
	}
	if r.String != nil {
		if b, err := strconv.ParseBool(*r.String); err == nil {
			return b, true
		}
	}
	return false, false
}

func (r Record) asString() (string, bool) {
	if r.String != nil {
		return *r.String, true
	}
	if r.Value != nil {
		return strconv.FormatFloat(*r.Value, 'f', -1, 64), true
	}
	if r.Bool != nil {
		return strconv.FormatBool(*r.Bool), true
	}
	return "", false
}

// buildProperty skapar dekoratören för en property. Saknad källa ger
// (nil, nil) för valfria properties och ErrRequiredSourceMissing för
// required.
func buildProperty(p Property, m Measurement) (entities.EntityDecoratorFunc, error) {
	if p.Derive != nil {
		return buildDerived(p, m)
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

	return decorate(p, raw, observationTime(p, m, r))
}

func rawValue(p Property, r Record) (any, error) {
	switch p.Type {
	case TypeNumber:
		if f, ok := r.asFloat(); ok {
			return f, nil
		}
	case TypeText:
		if s, ok := r.asString(); ok {
			return s, nil
		}
	case TypeBoolean:
		if b, ok := r.asBool(); ok {
			return b, nil
		}
	case TypeDateTime:
		if r.HasTime {
			return r.Time.UTC().Format(rfc3339), nil
		}
		if s, ok := r.asString(); ok {
			return s, nil
		}
	}
	return nil, fmt.Errorf("property %s: value is not a %s", p.Target, p.Type)
}

func decorate(p Property, raw any, observedAt string) (entities.EntityDecoratorFunc, error) {
	switch p.Type {
	case TypeNumber:
		v, ok := raw.(float64)
		if !ok {
			return nil, fmt.Errorf("property %s: expected number", p.Target)
		}
		opts := make([]properties.NumberPropertyDecoratorFunc, 0, 2)
		if p.Unit != "" {
			opts = append(opts, properties.UnitCode(p.Unit))
		}
		if observedAt != "" {
			opts = append(opts, properties.ObservedAt(observedAt))
		}
		return decorators.Number(p.Target, v, opts...), nil
	case TypeText:
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("property %s: expected text", p.Target)
		}
		opts := make([]properties.TextPropertyDecoratorFunc, 0, 1)
		if observedAt != "" {
			opts = append(opts, properties.TxtObservedAt(observedAt))
		}
		return textDecorator(p.Target, s, opts...), nil
	case TypeBoolean:
		b, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("property %s: expected boolean", p.Target)
		}
		prop, err := properties.UnmarshalP(map[string]any{"type": "Property", "value": b})
		if err != nil {
			return nil, err
		}
		return entities.P(p.Target, prop), nil
	case TypeDateTime:
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("property %s: expected date time", p.Target)
		}
		return decorators.DateTime(p.Target, s), nil
	default:
		return nil, fmt.Errorf("property %s: type %q is not supported for measurements yet", p.Target, p.Type)
	}
}

func textDecorator(name, value string, opts ...properties.TextPropertyDecoratorFunc) entities.EntityDecoratorFunc {
	tp := properties.NewTextProperty(value)
	for _, o := range opts {
		o(tp)
	}
	return entities.P(name, tp)
}

// applyTransforms är en platshållare som fylls i av transformstegen.
func applyTransforms(p Property, raw any) (any, error) {
	return raw, nil
}

// observationTime bestämmer propertyns observationstid enligt
// observedAt: header (default), record, message eller field:<namn>.
func observationTime(p Property, m Measurement, r Record) string {
	switch p.ObservedAt {
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

// buildDerived fylls i av deriveringsfunktionerna.
func buildDerived(p Property, m Measurement) (entities.EntityDecoratorFunc, error) {
	return nil, fmt.Errorf("property %s: derive is not implemented yet", p.Target)
}

const rfc3339 = "2006-01-02T15:04:05Z07:00"
