package transform

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

// Input är den normaliserade indata motorn arbetar på. Implementeras av
// Measurement (mätvärden) och Thing (saker).
type Input interface {
	Kind() Kind
	TenantID() string
	MatchInfo() MatchInfo
	Variables() map[string]string
	LatLon() (float64, float64, bool)
	MessageTime() time.Time
	BaseTime() (time.Time, bool)

	// Resolve slår upp en egenskaps värdekälla.
	Resolve(p Property) (resolvedValue, error)
	// ResolveTime slår upp en tid via en field-väg (observedAt: field:<väg>).
	ResolveTime(path string) (time.Time, bool)
	// ResolvePath slår upp ett värde via en JSON-väg/namn (arrayer, Structured).
	ResolvePath(path string) (any, bool)
}

// MatchInfo är de fält en regel kan matcha mot, oavsett indatatyp.
type MatchInfo struct {
	Device     string
	SensorType string
	Object     string
	Env        string
	Port       string
	Tenant     string
	Type       string
	SubType    string
}

// resolvedValue är resultatet av en värdekälleuppslagning.
type resolvedValue struct {
	value   any
	time    time.Time
	hasTime bool
	found   bool
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

// Transform matchar indatat mot den mest specifika regeln och producerar
// entitetsskrivningar. Ingen matchande regel ger ErrNoMatchingRule.
func (e *Engine) Transform(in Input) ([]EntityWrite, error) {
	rule, ok := e.match(in)
	if !ok {
		return nil, ErrNoMatchingRule
	}

	vars := in.Variables()
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

		typeName, err := renderTemplate(ent.Type, vars, ids)
		if err != nil {
			return nil, fmt.Errorf("entity %q: %w", ent.Name, err)
		}

		props, err := entityProps(ent, in, ids)
		if err != nil {
			if errors.Is(err, ErrRequiredSourceMissing) {
				continue
			}
			return nil, fmt.Errorf("entity %q: %w", ent.Name, err)
		}

		writes = append(writes, EntityWrite{
			EntityID: id,
			TypeName: typeName,
			Context:  ent.Context,
			Create:   ent.Create,
			Props:    props,
		})
	}

	return writes, nil
}

func entityProps(ent Entity, in Input, ids map[string]string) ([]entities.EntityDecoratorFunc, error) {
	props := make([]entities.EntityDecoratorFunc, 0, len(ent.Properties))

	for _, p := range ent.Properties {
		dec, err := buildProperty(p, in, ids)
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

// match väljer den regel med högst specificitet och därefter lägst
// konfigordning.
func (e *Engine) match(in Input) (Rule, bool) {
	info := in.MatchInfo()
	kind := in.Kind()

	best := -1
	bestScore := -1

	for i, r := range e.rules {
		if r.Match.Kind != kind || !r.Match.matches(info) {
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

func (mm Match) matches(info MatchInfo) bool {
	if mm.Device != "" && mm.Device != info.Device {
		return false
	}
	if mm.SensorType != "" && !strings.EqualFold(mm.SensorType, info.SensorType) {
		return false
	}
	if mm.Object != "" && mm.Object != info.Object {
		return false
	}
	if mm.Env != "" && mm.Env != info.Env {
		return false
	}
	if mm.Port != "" && mm.Port != info.Port {
		return false
	}
	if mm.Type != "" && mm.Type != info.Type {
		return false
	}
	if mm.SubType != "" && mm.SubType != info.SubType {
		return false
	}
	if mm.Tenant != "" && mm.Tenant != info.Tenant {
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
	if mm.Type != "" {
		s += 4
	}
	if mm.SubType != "" {
		s += 2
	}
	if mm.Tenant != "" {
		s++
	}
	return s
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
