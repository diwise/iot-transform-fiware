package transform

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"strings"
	"unicode"
)

func validateURI(value string) error {
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
		if strings.IndexFunc(e.Type, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
			return fmt.Errorf("entity.type %q: invalid type term", e.Type)
		}
		if strings.Contains(e.Type, ":") {
			if err := validateURI(e.Type); err != nil {
				return fmt.Errorf("entity.type %q: %w", e.Type, err)
			}
		}
	}
	if e.Context != "" {
		if err := validateURI(e.Context); err != nil {
			return fmt.Errorf("entity.context: %w", err)
		}
		u, _ := url.Parse(e.Context)
		if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("entity.context %q: expected HTTP(S) URL", e.Context)
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
			switch p.Derive.Fn {
			case FnSum, FnMin, FnMax:
				f, ok := toFloat(value)
				if !ok || !finite(f) {
					return fmt.Errorf("derive.of %d: expected finite number", i)
				}
			case FnAnyTrue, FnAllTrue:
				if _, ok := toBool(value); !ok {
					return fmt.Errorf("derive.of %d: expected boolean", i)
				}
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
	if !known || raw == nil {
		return nil
	}
	var err error
	raw, err = applyTransforms(p, raw)
	if err != nil {
		return fmt.Errorf("static source: %w", err)
	}
	if p.Derive == nil {
		raw = applyPrefix(p, raw)
	}
	if p.Type == TypeLanguageMap {
		if _, ok := toLanguageMap(raw); !ok {
			return errors.New("static source: expected a language map")
		}
		return nil
	}
	// Reuse execution's conversion and protocol checks, without source times.
	_, err = decorate(p, raw, "", "")
	if err != nil {
		return fmt.Errorf("static source (const=%v, value=%q): %w", p.Const, p.Value, err)
	}
	return nil
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
