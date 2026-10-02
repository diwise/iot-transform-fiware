package transform

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

// buildProperty skapar dekoratören för en property. Saknad källa ger
// (nil, nil) för valfria properties och ErrRequiredSourceMissing för
// required.
func buildProperty(p Property, in Input, ids map[string]string) (entities.EntityDecoratorFunc, error) {
	if p.Type == TypeStructured {
		return buildStructured(p, in, ids)
	}

	if p.Type == TypeLanguageMap {
		return buildLanguageMap(p, in, ids)
	}

	rv, err := evaluateValue(p, in, ids)
	if err != nil {
		return nil, err
	}
	if !rv.found {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	observedBy, err := renderObservedBy(p, in, ids)
	if err != nil {
		return nil, err
	}

	if p.Convert != nil && p.Unit == "" {
		p.Unit = p.Convert.To
	}
	observedAt, err := observationTime(p, in, rv)
	if err != nil {
		return nil, err
	}
	return decorate(p, rv.value, observedAt, observedBy)
}

// resolveSource is shared by attributes, aggregate inputs and Structured values.
// Source selectors do not perform target-type conversion.
func resolveSource(s Source, in Input, ids map[string]string) (resolvedValue, error) {
	switch {
	case s.Const != nil:
		return resolvedValue{value: s.Const, found: present(s.Const)}, nil
	case s.Value != "":
		value, err := renderTemplate(s.Value, in, ids)
		if errors.Is(err, errExprNotFound) {
			return resolvedValue{}, nil
		}
		rv := resolvedValue{value: value, found: err == nil && present(value)}
		if err == nil {
			parts, _ := parseTemplate(s.Value)
			for _, part := range parts {
				if part.expression == "" {
					continue
				}
				metadata, metadataErr := templateMetadata(part.expression, in, ids)
				if metadataErr != nil {
					return rv, metadataErr
				}
				if metadata.hasTime && (!rv.hasTime || metadata.time.After(rv.time)) {
					rv.time, rv.hasTime = metadata.time, true
				}
				if rv.unit != "" && metadata.unit != "" && canonicalUnit(rv.unit) != canonicalUnit(metadata.unit) {
					rv.unitConflict = true
				}
				if metadata.unit != "" {
					rv.unit = metadata.unit
				}
				rv.unitConflict = rv.unitConflict || metadata.unitConflict
			}
		}
		return rv, err
	case s.LatLon:
		lat, lon, ok := in.LatLon()
		return resolvedValue{value: map[string]any{"latitude": lat, "longitude": lon}, found: ok}, nil
	case s.Timestamp:
		t := in.MessageTime()
		return resolvedValue{value: sourceTime(t), found: !t.IsZero()}, nil
	default:
		return in.Resolve(Property{Source: s})
	}
}

func evaluateValue(p Property, in Input, ids map[string]string) (resolvedValue, error) {
	var rv resolvedValue
	var err error
	if p.Derive == nil {
		rv, err = resolveSource(p.Source, in, ids)
	} else {
		values := make([]any, 0, len(p.Derive.Of))
		inputs := make([]resolvedValue, 0, len(p.Derive.Of))
		for i, source := range p.Derive.Of {
			input, sourceErr := resolveSource(source, in, ids)
			if sourceErr != nil {
				return rv, fmt.Errorf("derive.of %d (%s): %w", i, sourceDescription(source), sourceErr)
			}
			if input.found && present(input.value) {
				if source.Name != "" {
					input.hasTime = false
				}
				value, prefixErr := applyPrefix(Property{Source: source}, input.value)
				if prefixErr != nil {
					return rv, fmt.Errorf("derive.of %d (%s): %w", i, sourceDescription(source), prefixErr)
				}
				values = append(values, value)
				inputs = append(inputs, input)
			}
		}
		if len(values) == 0 {
			return rv, nil
		}
		rv.value, err = deriveValue(p.Derive.Fn, values)
		rv.found = present(rv.value)
		if p.Derive.Fn == FnFirstPresent || p.Derive.Fn == FnCoalesce {
			rv.time, rv.hasTime, rv.unit, rv.unitConflict = inputs[0].time, inputs[0].hasTime, inputs[0].unit, inputs[0].unitConflict
		} else {
			if p.Derive.Fn == FnAnyTrue || p.Derive.Fn == FnAllTrue {
				rv.unit = "C62"
			}
			for i, input := range inputs {
				if input.hasTime && (!rv.hasTime || input.time.After(rv.time)) {
					rv.time, rv.hasTime = input.time, true
				}
				if p.Derive.Fn == FnSum || p.Derive.Fn == FnMin || p.Derive.Fn == FnMax {
					if input.unitConflict {
						return rv, fmt.Errorf("derive.of %d: incompatible template source units", i)
					}
					if rv.unit != "" && input.unit != "" && canonicalUnit(rv.unit) != canonicalUnit(input.unit) {
						return rv, fmt.Errorf("derive.of %d: incompatible source units %q and %q", i, rv.unit, input.unit)
					}
					if input.unit != "" {
						rv.unit = input.unit
					}
				}
			}
		}
	}
	if err != nil || !rv.found || !present(rv.value) {
		rv.found = false
		return rv, err
	}
	if rv.unitConflict && (p.Unit != "" || p.Convert != nil) {
		return rv, errors.New("incompatible template source units cannot be relabeled or converted")
	}
	rv.value, rv.unit, err = applyConversion(p, rv.value, rv.unit)
	if err != nil {
		return rv, err
	}
	rv.value, err = applyTransforms(p, rv.value)
	if err == nil && present(rv.value) {
		rv.value, err = applyPrefix(p, rv.value)
	}
	rv.found = present(rv.value)
	return rv, err
}

func templateMetadata(expr string, in Input, ids map[string]string) (resolvedValue, error) {
	expr = strings.TrimSpace(expr)
	_, args, call, err := templateCall(expr)
	if err != nil {
		return resolvedValue{}, err
	}
	if call {
		name, _, _ := strings.Cut(expr, "(")
		if name == "safeURI" {
			return templateMetadata(args[0], in, ids)
		}
		if name == "firstPresent" {
			for _, arg := range args {
				value, err := resolveExpr(arg, in.Variables(), in, ids)
				if errors.Is(err, errExprNotFound) {
					continue
				}
				if err != nil {
					return resolvedValue{}, err
				}
				if value != "" {
					return templateMetadata(arg, in, ids)
				}
			}
		}
		return resolvedValue{}, nil
	}
	path := strings.TrimSpace(strings.TrimPrefix(expr, "field:"))
	if _, variable := in.Variables()[path]; variable || strings.HasPrefix(path, "entities.") {
		return resolvedValue{}, nil
	}
	return in.Resolve(Property{Field: path})
}

// applyPrefix prependar en sträng till ett skalärt värde eller till varje
// element i en lista (t.ex. "urn:ngsi-ld:Device:").
func applyPrefix(p Property, raw any) (any, error) {
	if p.Prefix == "" {
		return raw, nil
	}

	switch x := raw.(type) {
	case string:
		return p.Prefix + x, nil
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			if s, ok := v.(string); ok {
				out[i] = p.Prefix + s
			} else {
				return nil, fmt.Errorf("prefix element %d: expected text", i)
			}
		}
		return out, nil
	case []string:
		out := make([]string, len(x))
		for i, value := range x {
			out[i] = p.Prefix + value
		}
		return out, nil
	default:
		return nil, errors.New("prefix expects text or a list of text")
	}
}

func renderObservedBy(p Property, in Input, ids map[string]string) (string, error) {
	if p.ObservedBy == "" {
		return "", nil
	}
	return renderTemplate(p.ObservedBy, in, ids)
}

func decorate(p Property, raw any, observedAt, observedBy string) (entities.EntityDecoratorFunc, error) {
	var err error
	raw, err = convertValue(p, raw)
	if err != nil {
		return nil, err
	}
	switch p.Type {
	case TypeNumber:
		v := raw.(float64)
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
		s := raw.(string)
		opts := make([]properties.TextPropertyDecoratorFunc, 0, 1)
		if observedAt != "" {
			opts = append(opts, properties.TxtObservedAt(observedAt))
		}
		return textDecorator(p.Target, s, opts...), nil
	case TypeBoolean:
		b := raw.(bool)
		return entities.P(p.Target, &genericProperty{Type_: "Property", Value_: b, ObservedAt: observedAt}), nil
	case TypeDateTime:
		s := raw.(string)
		return entities.P(p.Target, &genericProperty{Type_: "Property", Value_: map[string]any{"@type": "DateTime", "@value": s}, ObservedAt: observedAt}), nil
	case TypeGeo:
		return entities.P(p.Target, &genericGeoProperty{Type_: "GeoProperty", Value_: raw, ObservedAt: observedAt}), nil
	case TypeRelationship:
		return entities.R(p.Target, &genericRelationship{Type_: "Relationship", Object_: raw, ObservedAt: observedAt}), nil
	case TypeTextArray:
		ss := raw.([]string)
		tlp := properties.NewTextListProperty(ss)
		if observedAt != "" {
			tlp.ObservedAt_ = &observedAt
		}
		return entities.P(p.Target, tlp), nil
	case TypeNumberArray:
		fs := raw.([]float64)
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

func relationshipObjects(raw any, array bool) (any, error) {
	validate := func(value any) (string, error) {
		reference, ok := value.(string)
		if !ok {
			return "", errors.New("relationship reference must be a URI string")
		}
		if err := validateURI(reference); err != nil {
			return "", fmt.Errorf("invalid relationship URI %q: %w", reference, err)
		}
		return reference, nil
	}
	var elements []any
	switch value := raw.(type) {
	case []any:
		elements = value
	case []string:
		elements = make([]any, len(value))
		for i, reference := range value {
			elements[i] = reference
		}
	default:
		reference, err := validate(value)
		if err != nil {
			return nil, err
		}
		if array {
			return []string{reference}, nil
		}
		return reference, nil
	}
	references := make([]string, len(elements))
	for i, element := range elements {
		reference, err := validate(element)
		if err != nil {
			return nil, fmt.Errorf("relationship element %d: %w", i, err)
		}
		references[i] = reference
	}
	return references, nil
}

// buildStructured bygger en NGSI-LD-property vars värde som ett objekt av
// delproperties.
func buildStructured(p Property, in Input, ids map[string]string) (entities.EntityDecoratorFunc, error) {
	obj := map[string]any{}
	latest := resolvedValue{}

	for _, sp := range p.Subproperties {
		rv, err := evaluateValue(sp, in, ids)
		if err != nil {
			return nil, propertyError(sp, err)
		}
		if !rv.found {
			if sp.Required {
				return nil, propertyError(sp, ErrRequiredSourceMissing)
			}
			continue
		}

		val, err := convertValue(sp, rv.value)
		if err != nil {
			return nil, propertyError(sp, err)
		}

		obj[sp.Target] = val
		if rv.hasTime && (!latest.hasTime || rv.time.After(latest.time)) {
			latest.time, latest.hasTime = rv.time, true
		}
	}

	if len(obj) == 0 {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	observedAt, err := observationTime(p, in, latest)
	if err != nil {
		return nil, err
	}
	return entities.P(p.Target, &genericProperty{Type_: "Property", Value_: obj, ObservedAt: observedAt}), nil
}

// genericProperty täcker NGSI-LD-properties som biblioteket saknar
// färdiga konstruktorer för (bool, array av tal, strukturerade värden).
// Medvetet utanför i v1 (PLAN001 B8): VocabProperty, struktur-arrayer,
// BooleanArray/DateTimeArray, datasetId, observedBy utanför Number.
type genericProperty struct {
	Type_      string `json:"type"`
	Value_     any    `json:"value"`
	ObservedAt string `json:"observedAt,omitempty"`
	UnitCode   string `json:"unitCode,omitempty"`
}

func (g *genericProperty) Type() string { return g.Type_ }

func (g *genericProperty) Value() any { return g.Value_ }

// languageMapProperty skriver flerspråkiga texter som NGSI-LD
// LanguageProperty. Biblioteket saknar konstruktor; interfacet kräver bara
// Type()/Value(), så structen marshallas korrekt.
type languageMapProperty struct {
	Type_       string            `json:"type"`
	LanguageMap map[string]string `json:"languageMap"`
	ObservedAt  string            `json:"observedAt,omitempty"`
}

func (l *languageMapProperty) Type() string { return l.Type_ }

func (l *languageMapProperty) Value() any { return l.LanguageMap }

// buildLanguageMap bygger en LanguageProperty från field: (strängmapp) eller
// const: (mapp). Tom mapp = saknad källa (skip/required-semantik som
// vanligt); fel typ ger fel, inte tyst skip.
func buildLanguageMap(p Property, in Input, ids map[string]string) (entities.EntityDecoratorFunc, error) {
	rv, err := evaluateValue(p, in, ids)
	if err != nil {
		return nil, err
	}
	if !rv.found {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}
	value, err := convertValue(p, rv.value)
	if err != nil {
		return nil, err
	}
	m := value.(map[string]string)
	if len(m) == 0 {
		if p.Required {
			return nil, fmt.Errorf("%w: %s", ErrRequiredSourceMissing, p.Target)
		}
		return nil, nil
	}

	observedAt, err := observationTime(p, in, rv)
	if err != nil {
		return nil, err
	}
	return entities.P(p.Target, &languageMapProperty{Type_: "LanguageProperty", LanguageMap: m, ObservedAt: observedAt}), nil
}

// toLanguageMap normaliserar en strängmapp (från JSON-avkodning eller YAML
// const:). Icke-strängvärden avvisas.
func toLanguageMap(v any) (map[string]string, bool) {
	switch x := v.(type) {
	case map[string]string:
		return x, true
	case map[string]any:
		out := make(map[string]string, len(x))
		for k, e := range x {
			s, ok := e.(string)
			if !ok {
				return nil, false
			}
			out[k] = s
		}
		return out, true
	}
	return nil, false
}

// genericGeoProperty täcker GeoProperty-geometrier biblioteket saknar
// konstruktorer för (allt utom Point via latlon-vägen): samma value-form
// som biblioteket ({type, coordinates}) plus observedAt. Interfacet kräver
// bara Type()/Value(), så structen marshallas korrekt.
type genericGeoProperty struct {
	Type_      string `json:"type"`
	Value_     any    `json:"value"`
	ObservedAt string `json:"observedAt,omitempty"`
}

func (g *genericGeoProperty) Type() string { return g.Type_ }

func (g *genericGeoProperty) Value() any { return g.Value_ }

// genericRelationship täcker skalära/listformade Relationship-referenser
// med valfri observedAt.
type genericRelationship struct {
	Type_      string `json:"type"`
	Object_    any    `json:"object"`
	ObservedAt string `json:"observedAt,omitempty"`
}

func (r *genericRelationship) Type() string { return r.Type_ }

func (r *genericRelationship) Object() any { return r.Object_ }

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
	for i, t := range p.Transform {
		var err error
		raw, err = applyTransform(t, raw)
		if err != nil {
			return nil, fmt.Errorf("property %s: transform %d (%s): %w", p.Target, i, t.Op, err)
		}
		if value, ok := raw.(float64); ok && !finite(value) {
			return nil, fmt.Errorf("property %s: transform %d (%s): expected finite result", p.Target, i, t.Op)
		}
	}
	return raw, nil
}

func applyTransform(t Transform, raw any) (any, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
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
	case OpNumToOnOff:
		// Numerisk närvaro: noll (inkl. 0.0) ger "off", allt annat "on".
		// NaN/Inf kan inte förekomma per producentkontraktet.
		f, ok := toFloat(raw)
		if !ok {
			return nil, fmt.Errorf("numToOnOff expects a number")
		}
		if f != 0 {
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

func deriveValue(fn string, values []any) (any, error) {
	presentValues := make([]any, 0, len(values))
	for i, v := range values {
		if !present(v) {
			continue
		}
		if err := validateAggregateInput(fn, v); err != nil {
			return nil, fmt.Errorf("derive.of %d: %w", i, err)
		}
		presentValues = append(presentValues, v)
	}
	values = presentValues
	if len(values) == 0 {
		return nil, nil
	}
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
func observationTime(p Property, in Input, rv resolvedValue) (string, error) {
	switch p.ObservedAt {
	case ObservedAtNone:
		return "", nil
	case ObservedAtRecord:
		if rv.hasTime {
			return sourceTime(rv.time), nil
		}
		return "", nil
	case ObservedAtMessage:
		return sourceTime(in.MessageTime()), nil
	case ObservedAtHeader:
		if t, ok := in.BaseTime(); ok {
			return sourceTime(t), nil
		}
		return "", nil
	case "":
		if p.Name != "" || p.Timestamp {
			return "", nil
		}
		if rv.hasTime {
			return sourceTime(rv.time), nil
		}
		return "", nil
	default:
		path := strings.TrimPrefix(string(p.ObservedAt), "field:")
		if t, ok := in.ResolveTime(path); ok {
			return sourceTime(t), nil
		}
		if value, ok := in.ResolvePath(path); ok && present(value) {
			if t, ok := value.(time.Time); ok && t.IsZero() {
				return "", nil
			}
			if text, ok := value.(string); ok {
				if t, err := time.Parse(time.RFC3339Nano, text); err == nil && t.IsZero() {
					return "", nil
				}
			}
			return "", fmt.Errorf("observedAt field %q: expected RFC3339 date time", path)
		}
		return "", nil
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
	case int:
		return strconv.Itoa(x), true
	case int64:
		return strconv.FormatInt(x, 10), true
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
		if !finite(x) {
			return false, false
		}
		return x != 0, true
	case int:
		return x != 0, true
	case int64:
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
	lat, latOK := coordinateNumber(m["latitude"])
	lon, lonOK := coordinateNumber(m["longitude"])
	return lat, lon, latOK && lonOK && finite(lat) && finite(lon)
}

const rfc3339 = time.RFC3339Nano

func validateAggregateInput(fn string, value any) error {
	switch fn {
	case FnSum, FnMin, FnMax:
		v, ok := toFloat(value)
		if !ok || !finite(v) {
			return errors.New("expected finite number")
		}
	case FnAnyTrue, FnAllTrue:
		if v, ok := value.(float64); ok && !finite(v) {
			return errors.New("expected finite boolean input")
		}
		if _, ok := toBool(value); !ok {
			return errors.New("expected boolean")
		}
	}
	return nil
}
