package transform

import (
	"errors"
	"fmt"
	"strings"
)

var validPropertyTypes = map[PropertyType]bool{
	TypeNumber:       true,
	TypeText:         true,
	TypeBoolean:      true,
	TypeDateTime:     true,
	TypeGeo:          true,
	TypeRelationship: true,
	TypeNumberArray:  true,
	TypeTextArray:    true,
	TypeStructured:   true,
}

// Validate kör samtliga valideringar på en ordnad regellista.
func Validate(rules []Rule) error {
	for i, r := range rules {
		if err := r.validate(); err != nil {
			return fmt.Errorf("rule %d: %w", i, err)
		}
	}
	return nil
}

func (r Rule) validate() error {
	if r.Match.Kind != KindMeasurement && r.Match.Kind != KindThing {
		return fmt.Errorf("match.kind must be %q or %q", KindMeasurement, KindThing)
	}

	if r.Match.Object != "" && !strings.HasPrefix(r.Match.Object, "urn:") {
		return fmt.Errorf("match.object %q must be a URN", r.Match.Object)
	}

	if len(r.Entities) == 0 {
		return errors.New("rule must declare at least one entity")
	}

	for i, e := range r.Entities {
		if err := e.validate(); err != nil {
			return fmt.Errorf("entity %d: %w", i, err)
		}
	}

	return nil
}

func (e Entity) validate() error {
	if e.ID == "" {
		return errors.New("entity.id is required")
	}
	if e.Type == "" {
		return errors.New("entity.type is required")
	}

	for i, p := range e.Properties {
		if err := p.validate(); err != nil {
			return fmt.Errorf("property %d (%s): %w", i, p.Target, err)
		}
	}

	return nil
}

func (p Property) validate() error {
	if p.Target == "" {
		return errors.New("target is required")
	}

	if !validPropertyTypes[p.Type] {
		return fmt.Errorf("unknown property type %q", p.Type)
	}

	if p.ObservedAt != "" && !strings.HasPrefix(string(p.ObservedAt), "field:") && !validObservedAt[p.ObservedAt] {
		return fmt.Errorf("unknown observedAt %q", p.ObservedAt)
	}

	if p.When != "" && p.When != WhenPresent {
		return fmt.Errorf("unknown when %q", p.When)
	}

	if p.Type == TypeStructured {
		if len(p.Subproperties) == 0 {
			return errors.New("a structured property must declare subproperties")
		}
		for i, sp := range p.Subproperties {
			if err := sp.validate(); err != nil {
				return fmt.Errorf("subproperty %d (%s): %w", i, sp.Target, err)
			}
		}
		return nil
	}

	if len(p.Subproperties) > 0 {
		return errors.New("subproperties are only valid for structured properties")
	}

	return p.validateSource()
}

var validObservedAt = map[ObservedAt]bool{
	ObservedAtHeader:  true,
	ObservedAtRecord:  true,
	ObservedAtMessage: true,
}

// validateSource säkerställer att exakt en primär värdkälla är angiven.
func (p Property) validateSource() error {
	primary := 0
	if p.Resource != "" {
		primary++
	}
	if p.Field != "" {
		primary++
	}
	if p.Name != "" {
		primary++
	}
	if p.Const != nil {
		primary++
	}
	if p.LatLon {
		primary++
	}
	if p.Value != "" {
		primary++
	}
	if p.Derive != nil {
		primary++
	}

	if primary == 0 {
		return errors.New("a value source is required")
	}
	if primary > 1 {
		return errors.New("exactly one value source is allowed")
	}

	return nil
}
