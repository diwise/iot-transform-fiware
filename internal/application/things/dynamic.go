package things

import (
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
)

// DynamicThingTransformer mappar en sak till entitetsskrivningar med den
// konfigurationsstyrda motorn. Oföränderlig efter konstruktion: omladdning
// sker genom att bygga en ny instans (se api.Snapshot, TF-05). Delas av
// values-, relations- och lifecycle-handlers (thing.updated-vägen är
// borttagen, PLAN001 steg 7.6).
type DynamicThingTransformer struct {
	engine *transform.Engine
}

func NewDynamicThingTransformer(rules []transform.Rule) *DynamicThingTransformer {
	return &DynamicThingTransformer{engine: transform.NewEngine(rules)}
}

func (d *DynamicThingTransformer) Transform(t transform.Thing) ([]transform.EntityWrite, error) {
	return d.engine.Transform(t)
}

func stringOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
