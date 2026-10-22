package transform

import (
	"reflect"
	"time"
)

func present(value any) bool {
	if value == nil {
		return false
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String, reflect.Array, reflect.Slice, reflect.Map:
		return v.Len() > 0
	case reflect.Pointer, reflect.Interface:
		return !v.IsNil()
	default:
		return true
	}
}

func sourceTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	// Broker-kompatibel precision: trunkera till millisekunder. Orion-LD
	// avvisar nanosekunder (9 decimaler) med "expected end-of-string".
	return FormatBrokerTime(t)
}

func parseSourceTime(value any) (time.Time, bool) {
	if t, ok := value.(time.Time); ok {
		return t, !t.IsZero()
	}
	if text, ok := value.(string); ok {
		t, err := time.Parse(time.RFC3339Nano, text)
		return t, err == nil && !t.IsZero()
	}
	return time.Time{}, false
}

func resolveField(fields map[string]any, path string) resolvedValue {
	steps, err := parsePath(path)
	if err != nil {
		return resolvedValue{}
	}
	value, ok := lookupSteps(fields, steps)
	rv := resolvedValue{value: value, found: ok && present(value)}
	if !rv.found || len(steps) < 2 || steps[len(steps)-1].key != "value" {
		return rv
	}
	parent, ok := lookupSteps(fields, steps[:len(steps)-1])
	if entry, entryOK := parent.(map[string]any); ok && entryOK {
		rv.time, rv.hasTime = parseSourceTime(entry["observedAt"])
		rv.unit, _ = entry["unit"].(string)
	}
	return rv
}
