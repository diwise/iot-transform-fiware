package things

import (
	"testing"

	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/matryer/is"
)

// HARM-003: locks the producer envelope shape this consumer decodes.
// iot-things publishes ThingUpdated with the JSON fields id, type, thing,
// tenant and timestamp; ThingFromJSON must keep decoding exactly that shape.
// Changes on either side require a compatibility/migration plan.
func TestProducerEnvelopeShape(t *testing.T) {
	is := is.New(t)

	th, err := appthings.ThingFromJSON([]byte(wastecontainerEnvelope))
	is.NoErr(err)

	is.Equal(th.Tenant, "default")
	is.Equal(th.Type, "Container")
	is.Equal(th.SubType, "WasteContainer")
	is.Equal(th.TypeName, "WasteContainer")
	is.Equal(th.AlternativeName, "Soptunnor.XY")
	is.Equal(th.NameOrID, "Soptunnor.XY")
	is.True(!th.ObservedAt.IsZero())
}

const wastecontainerEnvelope = `{
	"id": "2bf440f4",
	"type": "Container",
	"thing": {
		"id": "2bf440f4",
		"type": "Container",
		"subType": "WasteContainer",
		"name": "Soptunnor.X",
		"alternativeName": "Soptunnor.XY",
		"location": {"latitude": 62, "longitude": 17},
		"observedAt": "2024-11-19T10:49:59Z",
		"percent": 56,
		"tenant": "default"
	},
	"tenant": "default",
	"timestamp": "2024-11-19T10:49:59.748823813Z"
}`
