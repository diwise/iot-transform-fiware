package things

import (
	"encoding/json"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// DynamicThingTransformer mappar en sak till entitetsskrivningar med den
// konfigurationsstyrda motorn.
type DynamicThingTransformer struct {
	engine *transform.Engine
}

func NewDynamicThingTransformer(rules []transform.Rule) *DynamicThingTransformer {
	return &DynamicThingTransformer{engine: transform.NewEngine(rules)}
}

func (d *DynamicThingTransformer) Transform(t transform.Thing) ([]transform.EntityWrite, error) {
	return d.engine.Transform(t)
}

// ThingFromJSON tolkar ett thing.updated-envelope och bygger motorns indata.
func ThingFromJSON(body []byte) (transform.Thing, error) {
	env := struct {
		Timestamp time.Time       `json:"timestamp"`
		Thing     json.RawMessage `json:"thing"`
	}{}

	if err := json.Unmarshal(body, &env); err != nil {
		return transform.Thing{}, err
	}

	var fields map[string]any
	if err := json.Unmarshal(env.Thing, &fields); err != nil {
		return transform.Thing{}, err
	}

	var meta Thing
	if err := json.Unmarshal(env.Thing, &meta); err != nil {
		return transform.Thing{}, err
	}

	observedAt := meta.ObservedAt
	if observedAt.IsZero() {
		observedAt = env.Timestamp
	}

	return transform.Thing{
		ID:              meta.ID,
		Type:            meta.Type,
		SubType:         stringOrEmpty(meta.SubType),
		Name:            meta.Name,
		AlternativeName: meta.AlternativeName,
		Description:     stringOrEmpty(meta.Description),
		Tenant:          meta.Tenant,
		TypeName:        meta.TypeName(),
		NameOrID:        meta.AlternativeNameOrNameOrID(),
		ObservedAt:      observedAt,
		Fields:          fields,
	}, nil
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
