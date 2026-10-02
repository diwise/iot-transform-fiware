package transform

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

func validateURI(value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("absolute URI must be valid UTF-8")
	}
	hex := func(b byte) bool { return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F') }
	for i := 0; i < len(value); i++ {
		if value[i] == '%' && (i+2 >= len(value) || !hex(value[i+1]) || !hex(value[i+2])) {
			return fmt.Errorf("invalid percent encoding in absolute URI")
		}
	}
	if strings.ContainsAny(value, "\\\"<>`{}|^") {
		return fmt.Errorf("invalid character in absolute URI")
	}
	uri, err := url.Parse(value)
	if err != nil || !uri.IsAbs() || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 || (uri.Opaque == "" && uri.Host == "" && uri.Path == "") {
		return fmt.Errorf("invalid absolute URI %q", value)
	}
	return nil
}

// staticTemplate deliberately does not substitute unknown source fields.
func staticTemplate(value string) bool {
	parts, err := parseTemplate(value)
	if err != nil {
		return false // syntax is checked by the caller
	}
	for _, part := range parts {
		if part.expression != "" {
			return false
		}
	}
	return true
}

func (e Entity) validateStatic() error {
	if staticTemplate(e.ID) {
		if err := validateURI(e.ID); err != nil {
			return fmt.Errorf("entity.id %q: %w", e.ID, err)
		}
	}
	if staticTemplate(e.Type) {
		if err := validateTerm(e.Type); err != nil {
			return fmt.Errorf("entity.type: %w", err)
		}
	}
	if e.Context != "" {
		if err := validateContextURL(e.Context); err != nil {
			return fmt.Errorf("entity.context: %w", err)
		}
	}
	return nil
}

func staticSource(s Source) (any, bool) {
	if s.Const != nil {
		return s.Const, true
	}
	if s.Value != "" && staticTemplate(s.Value) {
		return s.Value, true
	}
	return nil, false
}

func (p Property) validateStatic() error {
	if p.ObservedBy != "" && staticTemplate(p.ObservedBy) {
		if err := validateURI(p.ObservedBy); err != nil {
			return fmt.Errorf("observedBy %q: %w", p.ObservedBy, err)
		}
	}
	raw, known := staticSource(p.Source)
	if p.Derive != nil {
		values := make([]any, 0, len(p.Derive.Of))
		known = true
		for i, source := range p.Derive.Of {
			value, static := staticSource(source)
			if !static {
				known = false
				continue
			}
			if !present(value) {
				continue
			}
			var err error
			value, err = applyPrefix(Property{Source: source}, value)
			if err != nil {
				return fmt.Errorf("derive.of %d: %w", i, err)
			}
			if err := validateAggregateInput(p.Derive.Fn, value); err != nil {
				return fmt.Errorf("derive.of %d: %w", i, err)
			}
			values = append(values, value)
		}
		if known {
			var err error
			raw, err = deriveValue(p.Derive.Fn, values)
			if err != nil {
				return err
			}
		}
	}
	if !known || !present(raw) {
		return nil
	}
	var err error
	unit := ""
	if p.Derive != nil && (p.Derive.Fn == FnAnyTrue || p.Derive.Fn == FnAllTrue) {
		unit = "C62"
	}
	raw, _, err = applyConversion(p, raw, unit)
	if err != nil {
		return fmt.Errorf("static source: %w", err)
	}
	raw, err = applyTransforms(p, raw)
	if err != nil {
		return fmt.Errorf("static source: %w", err)
	}
	if !present(raw) {
		return nil
	}
	raw, err = applyPrefix(p, raw)
	if err != nil {
		return fmt.Errorf("static source: %w", err)
	}
	if p.Type == TypeLanguageMap {
		if _, err := convertValue(p, raw); err != nil {
			return fmt.Errorf("static source: %w", err)
		}
		return nil
	}
	// Reuse execution's conversion and protocol checks, without source times.
	observer := p.ObservedBy
	if !staticTemplate(observer) {
		observer = ""
	}
	decorator, err := decorate(p, raw, "", observer)
	if err != nil {
		return fmt.Errorf("static source (const=%v, value=%q): %w", p.Const, p.Value, err)
	}
	if _, err := BuildEntity(EntityWrite{EntityID: "urn:example:static-validation", TypeName: "StaticValidation", Props: []entities.EntityDecoratorFunc{decorator}}, ""); err != nil {
		return fmt.Errorf("static source: %w", err)
	}
	return nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
