package transform

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

// Record är en normaliserad resurs-/metadatapost i en mätning.
type Record struct {
	Object  string
	Name    string
	Value   *float64
	Bool    *bool
	String  *string
	Time    time.Time
	HasTime bool
}

// Measurement är motorns normaliserade indata för en enskild
// objektobservation (ett objekt ur ett SenML-pack).
type Measurement struct {
	DeviceID   string
	SensorType string
	Object     string
	Env        string
	Port       string
	Tenant     string
	Timestamp  time.Time
	Lat        float64
	Lon        float64
	HasLatLon  bool
	Records    []Record
}

// EntityWrite beskriver en entitet som ska skrivas, utan att utföra
// skrivningen. Presentationen exekverar skrivningarna i listordning.
type EntityWrite struct {
	EntityID string
	TypeName string
	Context  string
	Create   bool
	Props    []entities.EntityDecoratorFunc
}

// Engine kör den konfigurationsstyrda mappningen.
type Engine struct {
	rules []Rule
}

func NewEngine(rules []Rule) *Engine {
	return &Engine{rules: rules}
}

// Transform matchar mätningen mot den mest specifika regeln och producerar
// entitetsskrivningar. Ingen matchande regel ger ErrNoMatchingRule.
func (e *Engine) Transform(m Measurement) ([]EntityWrite, error) {
	rule, ok := e.match(m)
	if !ok {
		return nil, ErrNoMatchingRule
	}

	vars := m.templateVars()
	ids := map[string]string{}
	writes := make([]EntityWrite, 0, len(rule.Entities))

	for _, ent := range rule.Entities {
		id, err := renderTemplate(ent.ID, vars, ids)
		if err != nil {
			return nil, fmt.Errorf("entity %q: %w", ent.Name, err)
		}

		if ent.Name != "" {
			ids[ent.Name] = id
		}

		props, err := entityProps(ent, m)
		if err != nil {
			if errors.Is(err, ErrRequiredSourceMissing) {
				continue
			}
			return nil, fmt.Errorf("entity %q: %w", ent.Name, err)
		}

		writes = append(writes, EntityWrite{
			EntityID: id,
			TypeName: ent.Type,
			Context:  ent.Context,
			Create:   ent.Create,
			Props:    props,
		})
	}

	return writes, nil
}

func entityProps(ent Entity, m Measurement) ([]entities.EntityDecoratorFunc, error) {
	props := make([]entities.EntityDecoratorFunc, 0, len(ent.Properties))

	for _, p := range ent.Properties {
		dec, err := buildProperty(p, m)
		if err != nil {
			return nil, err
		}
		if dec == nil {
			continue
		}
		props = append(props, dec)
	}

	return props, nil
}

// match väljer den regel med högst specificitet (device > sensorType >
// object) och därefter lägst konfigordning.
func (e *Engine) match(m Measurement) (Rule, bool) {
	best := -1
	bestScore := -1

	for i, r := range e.rules {
		if r.Match.Kind != KindMeasurement || !r.Match.matches(m) {
			continue
		}

		if s := r.Match.score(); s > bestScore {
			bestScore, best = s, i
		}
	}

	if best < 0 {
		return Rule{}, false
	}

	return e.rules[best], true
}

func (mm Match) matches(m Measurement) bool {
	if mm.Device != "" && mm.Device != m.DeviceID {
		return false
	}
	if mm.SensorType != "" && !strings.EqualFold(mm.SensorType, m.SensorType) {
		return false
	}
	if mm.Object != "" && mm.Object != m.Object {
		return false
	}
	if mm.Env != "" && mm.Env != m.Env {
		return false
	}
	if mm.Port != "" && mm.Port != m.Port {
		return false
	}
	if mm.Tenant != "" && mm.Tenant != m.Tenant {
		return false
	}
	return true
}

func (mm Match) score() int {
	s := 0
	if mm.Device != "" {
		s += 4
	}
	if mm.SensorType != "" {
		s += 2
	}
	if mm.Object != "" {
		s++
	}
	return s
}

func (m Measurement) templateVars() map[string]string {
	return map[string]string{
		"deviceID":   m.DeviceID,
		"sensorType": m.SensorType,
		"object":     m.Object,
		"env":        m.Env,
		"port":       m.Port,
		"tenant":     m.Tenant,
		"timestamp":  m.Timestamp.UTC().Format(time.RFC3339),
	}
}

// renderTemplate ersätter {{variabel}}, {{safeURI(variabel)}} och
// {{entities.<namn>.id}} i en mall.
func renderTemplate(tpl string, vars, entities map[string]string) (string, error) {
	var firstErr error

	out := templateExprRe.ReplaceAllStringFunc(tpl, func(match string) string {
		if firstErr != nil {
			return match
		}

		expr := strings.TrimSpace(templateExprRe.FindStringSubmatch(match)[1])

		if mm := entityRefExpr.FindStringSubmatch(expr); mm != nil {
			id, ok := entities[mm[1]]
			if !ok {
				firstErr = fmt.Errorf("unknown entity %q", mm[1])
				return ""
			}
			return id
		}

		if mm := safeURIExpr.FindStringSubmatch(expr); mm != nil {
			v, ok := vars[mm[1]]
			if !ok {
				firstErr = fmt.Errorf("unknown template variable %q", mm[1])
				return ""
			}
			return url.PathEscape(strings.TrimSpace(v))
		}

		v, ok := vars[expr]
		if !ok {
			firstErr = fmt.Errorf("unknown template variable %q", expr)
			return ""
		}
		return v
	})

	if firstErr != nil {
		return "", firstErr
	}

	return out, nil
}
