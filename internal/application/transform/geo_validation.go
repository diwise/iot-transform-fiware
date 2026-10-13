package transform

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func coordinateNumber(value any) (float64, bool) {
	if n, ok := value.(json.Number); ok {
		f, err := n.Float64()
		return f, err == nil && finite(f)
	}
	if value == nil {
		return 0, false
	}
	v := reflect.ValueOf(value)
	var number float64
	switch v.Kind() {
	case reflect.Float32, reflect.Float64:
		number = v.Float()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number = float64(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number = float64(v.Uint())
	default:
		return 0, false
	}
	return number, finite(number)
}

func coordinateList(value any) ([]any, bool) {
	if value == nil {
		return nil, false
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, false
	}
	result := make([]any, v.Len())
	for i := range result {
		result[i] = v.Index(i).Interface()
	}
	return result, true
}

// The geometry profile is WGS84, two or three consistent dimensions. Ring
// orientation, self-intersections and hole containment are not checked here.
func validateGeoJSON(raw any) (map[string]any, error) {
	geo, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected a location: GeoJSON object required")
	}
	kind, ok := geo["type"].(string)
	if !ok {
		return nil, fmt.Errorf("expected a location: GeoJSON type required")
	}
	dimension := 0
	position := func(value any, path string) ([]any, error) {
		items, ok := coordinateList(value)
		if !ok || (len(items) != 2 && len(items) != 3) {
			return nil, fmt.Errorf("%s: expected a location with 2D or 3D position", path)
		}
		if dimension == 0 {
			dimension = len(items)
		}
		if dimension != len(items) {
			return nil, fmt.Errorf("%s: inconsistent coordinate dimensions", path)
		}
		result := make([]any, len(items))
		for i, item := range items {
			v, ok := coordinateNumber(item)
			if !ok {
				return nil, fmt.Errorf("%s[%d]: expected finite numeric coordinate", path, i)
			}
			if i == 0 && (v < -180 || v > 180) {
				return nil, fmt.Errorf("%s[%d]: longitude outside WGS84 range", path, i)
			}
			if i == 1 && (v < -90 || v > 90) {
				return nil, fmt.Errorf("%s[%d]: latitude outside WGS84 range", path, i)
			}
			result[i] = v
		}
		return result, nil
	}
	positions := func(value any, path string, min int, ring bool) ([]any, error) {
		items, ok := coordinateList(value)
		if !ok || len(items) < min {
			return nil, fmt.Errorf("%s: expected at least %d positions", path, min)
		}
		result := make([]any, len(items))
		for i, item := range items {
			p, err := position(item, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			result[i] = p
		}
		if ring && !reflect.DeepEqual(result[0], result[len(result)-1]) {
			return nil, fmt.Errorf("%s: polygon ring must be closed", path)
		}
		return result, nil
	}
	polygon := func(value any, path string) ([]any, error) {
		items, ok := coordinateList(value)
		if !ok || len(items) == 0 {
			return nil, fmt.Errorf("%s: expected polygon rings", path)
		}
		result := make([]any, len(items))
		for i, item := range items {
			ring, err := positions(item, fmt.Sprintf("%s[%d]", path, i), 4, true)
			if err != nil {
				return nil, err
			}
			result[i] = ring
		}
		return result, nil
	}
	var coordinates any
	var err error
	switch kind {
	case "Point":
		coordinates, err = position(geo["coordinates"], "coordinates")
	case "MultiPoint":
		coordinates, err = positions(geo["coordinates"], "coordinates", 1, false)
	case "LineString":
		coordinates, err = positions(geo["coordinates"], "coordinates", 2, false)
	case "Polygon":
		coordinates, err = polygon(geo["coordinates"], "coordinates")
	case "MultiLineString", "MultiPolygon":
		items, ok := coordinateList(geo["coordinates"])
		if !ok || len(items) == 0 {
			return nil, fmt.Errorf("coordinates: expected nonempty %s components", kind)
		}
		result := make([]any, len(items))
		for i, item := range items {
			path := fmt.Sprintf("coordinates[%d]", i)
			if kind == "MultiLineString" {
				result[i], err = positions(item, path, 2, false)
			} else {
				result[i], err = polygon(item, path)
			}
			if err != nil {
				return nil, err
			}
		}
		coordinates = result
	default:
		return nil, fmt.Errorf("expected a location: unsupported GeoJSON type %q", kind)
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"type": kind, "coordinates": coordinates}, nil
}
