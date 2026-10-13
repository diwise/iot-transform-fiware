package transform

import (
	"fmt"
	"strings"
)

// Equivalent spelling/code mappings only; there is no inferred numeric conversion.
var unitCodes = map[string]string{
	"Cel": "CEL", "°C": "CEL", "K": "KEL", "ppm": "59", "PPM": "59",
	"%": "P1", "pct": "P1", "%RH": "P1", "Pa": "PAL", "kPa": "KPA",
	"m": "MTR", "mm": "MMT", "m3": "MTQ", "m³": "MTQ", "S": "MHO", "1": "C62",
}

func canonicalUnit(unit string) string {
	if code, ok := unitCodes[unit]; ok {
		return code
	}
	return unit
}

func (p Property) validateConversion() error {
	if p.Convert == nil {
		return nil
	}
	if p.Type != TypeNumber && p.Type != TypeNumberArray {
		return fmt.Errorf("convert requires Number or NumberArray")
	}
	c := p.Convert
	if err := validateUnitCode(c.From); err != nil {
		return fmt.Errorf("convert.from: %w", err)
	}
	if err := validateUnitCode(c.To); err != nil {
		return fmt.Errorf("convert.to: %w", err)
	}
	if strings.TrimSpace(c.From) == "" || strings.TrimSpace(c.To) == "" {
		return fmt.Errorf("convert.from/to must not be empty")
	}
	if (c.Factor != nil && !finite(*c.Factor)) || !finite(c.Offset) {
		return fmt.Errorf("convert factor/offset must be finite")
	}
	if canonicalUnit(c.From) != canonicalUnit(c.To) && (c.Factor == nil || *c.Factor == 1) && c.Offset == 0 {
		return fmt.Errorf("convert between different units requires a non-identity numeric conversion")
	}
	if p.Unit != "" && canonicalUnit(p.Unit) != canonicalUnit(c.To) {
		return fmt.Errorf("unit must agree with convert.to")
	}
	return nil
}

func applyConversion(p Property, raw any, sourceUnit string) (any, string, error) {
	if p.Convert == nil {
		if sourceUnit != "" && p.Unit != "" && canonicalUnit(sourceUnit) != canonicalUnit(p.Unit) {
			return nil, sourceUnit, fmt.Errorf("source unit %q cannot be relabeled %q without convert", sourceUnit, p.Unit)
		}
		return raw, sourceUnit, nil
	}
	if err := p.validateConversion(); err != nil {
		return nil, sourceUnit, err
	}
	c := p.Convert
	if sourceUnit != "" && canonicalUnit(sourceUnit) != canonicalUnit(c.From) {
		return nil, sourceUnit, fmt.Errorf("source unit %q does not match convert.from %q", sourceUnit, c.From)
	}
	factor := 1.0
	if c.Factor != nil {
		factor = *c.Factor
	}
	convert := func(value any) (float64, error) {
		v, ok := toFloat(value)
		if !ok || !finite(v) {
			return 0, fmt.Errorf("convert expects finite numeric input")
		}
		v = v*factor + c.Offset
		if !finite(v) {
			return 0, fmt.Errorf("convert produced non-finite result")
		}
		return v, nil
	}
	if p.Type == TypeNumberArray {
		values, ok := toFloatSlice(raw)
		if !ok {
			return nil, sourceUnit, fmt.Errorf("convert expects a numeric list")
		}
		result := make([]float64, len(values))
		for i, v := range values {
			converted, err := convert(v)
			if err != nil {
				return nil, sourceUnit, fmt.Errorf("convert element %d: %w", i, err)
			}
			result[i] = converted
		}
		return result, c.To, nil
	}
	value, err := convert(raw)
	return value, c.To, err
}
