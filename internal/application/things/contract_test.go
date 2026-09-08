package things

import (
	"encoding/json"
	"testing"

	"github.com/matryer/is"
)

// HARM-003: locks the producer envelope shape this consumer decodes.
// iot-things publishes ThingUpdated with the JSON fields id, type, thing,
// tenant and timestamp; the local msg[T] envelope must keep decoding
// exactly that shape. Changes on either side require a
// compatibility/migration plan.
func TestProducerEnvelopeShape(t *testing.T) {
	is := is.New(t)

	m := msg[container]{}
	is.NoErr(json.Unmarshal([]byte(wastecontainerJson), &m))

	is.Equal(m.ID, "2bf440f4")
	is.Equal(m.Type, "Container")
	is.Equal(m.Tenant, "default")
	is.True(!m.Timestamp.IsZero())

	is.Equal(m.Thing.Tenant, "default")
	is.Equal(m.Thing.AlternativeName, "Soptunnor.XY")
	is.Equal(m.Thing.EntityID(), "urn:ngsi-ld:WasteContainer:Soptunnor.XY")
}
