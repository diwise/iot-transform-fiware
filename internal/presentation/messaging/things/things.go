package things

import (
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
)

// TransformerProvider levererar aktuell snapshot-motor per meddelande så
// att omladdade regler slår igenom utan omregistrering (TF-05). Används av
// values-, relations- och lifecycle-handlers (thing.updated-vägen är
// borttagen, PLAN001 steg 7.6).
type TransformerProvider interface {
	ThingsTransformer() *appthings.DynamicThingTransformer
}
