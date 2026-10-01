package transform

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// convertValue defines the same target-type conversion for attributes and
// Structured scalars. Structured contains values, not nested NGSI-LD attributes.
func convertValue(p Property, raw any) (any, error) {
	invalid := func(expected string) (any, error) {
		return nil, fmt.Errorf("property %s: expected %s", p.Target, expected)
	}
	switch p.Type {
	case TypeNumber:
		v, ok := toFloat(raw)
		if !ok {
			return invalid("number")
		}
		if !finite(v) {
			return invalid("finite number")
		}
		return v, nil
	case TypeText:
		v, ok := toString(raw)
		if !ok {
			return invalid("text")
		}
		return v, nil
	case TypeBoolean:
		v, ok := toBool(raw)
		if !ok {
			return invalid("boolean")
		}
		return v, nil
	case TypeDateTime:
		v, ok := toString(raw)
		if !ok {
			return invalid("date time")
		}
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			return nil, fmt.Errorf("property %s: invalid date time %q: %w", p.Target, v, err)
		}
		return v, nil
	case TypeGeo:
		if s, ok := raw.(string); ok {
			var geo map[string]any
			if json.Unmarshal([]byte(s), &geo) == nil {
				raw = geo
			}
		}
		if geo, ok := toGeoJSON(raw); ok {
			return geo, nil
		}
		if lat, lon, ok := toLatLon(raw); ok {
			return map[string]any{"type": "Point", "coordinates": []any{lon, lat}}, nil
		}
		return invalid("a location")
	case TypeRelationship:
		v, err := relationshipObjects(raw, p.Array)
		if err != nil {
			return nil, fmt.Errorf("property %s: %w", p.Target, err)
		}
		return v, nil
	case TypeTextArray:
		v, ok := toStringSlice(raw)
		if !ok {
			return invalid("a list of text")
		}
		return v, nil
	case TypeNumberArray:
		v, ok := toFloatSlice(raw)
		if !ok {
			return invalid("a list of numbers")
		}
		for i, f := range v {
			if !finite(f) {
				return nil, fmt.Errorf("property %s: element %d: expected finite number", p.Target, i)
			}
		}
		return v, nil
	case TypeLanguageMap:
		v, ok := toLanguageMap(raw)
		if !ok {
			return invalid("a language map")
		}
		return v, nil
	default:
		return nil, fmt.Errorf("property %s: type %q is not supported yet", p.Target, p.Type)
	}
}

func propertyError(p Property, err error) error {
	description := sourceDescription(p.Source)
	if p.Derive != nil {
		sources := make([]string, len(p.Derive.Of))
		for i, source := range p.Derive.Of {
			sources[i] = fmt.Sprintf("of[%d] %s", i, sourceDescription(source))
		}
		description = fmt.Sprintf("derive %s: %s", p.Derive.Fn, strings.Join(sources, "; "))
	}
	return fmt.Errorf("property %q (%s): %w", p.Target, description, err)
}

func sourceDescription(s Source) string {
	switch {
	case s.Value != "":
		return fmt.Sprintf("value expression %q", s.Value)
	case s.Field != "":
		return fmt.Sprintf("field %q", s.Field)
	case s.Ref != "":
		return fmt.Sprintf("ref %q", s.Ref)
	case s.Resource != "":
		return fmt.Sprintf("resource %q", s.Resource)
	case s.Name != "":
		return fmt.Sprintf("name %q", s.Name)
	case s.Const != nil:
		return "const"
	case s.LatLon:
		return "latlon"
	case s.Timestamp:
		return "timestamp"
	default:
		return "composite source"
	}
}
