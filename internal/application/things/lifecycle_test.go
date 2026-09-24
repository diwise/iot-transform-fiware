package things

import (
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/matryer/is"
)

const lifecycleDeletedBody = `{
	"eventId":"v1/t/room-a/lifecycle/deleted/4","tenant":"t",
	"thingId":"room-a","kind":"deleted",
	"thingType":"room","thingTypeVersion":"1",
	"configRevision":4,"changedAt":"2026-09-17T12:00:00Z",
	"thing":{"thingId":"room-a","name":"Room A","subType":"","category":"room"}
}`

func TestLifecycleAdapterMapsDeletedEvent(t *testing.T) {
	is := is.New(t)

	th, err := ThingLifecycleFromJSON([]byte(lifecycleDeletedBody))
	is.NoErr(err)
	is.Equal(th.ID, "room-a")
	is.Equal(th.Type, "room")
	is.Equal(th.Tenant, "t")
	is.Equal(th.Event, transform.EventThingsV1Lifecycle)
	is.Equal(th.Lifecycle, "deleted")
	// Samma namnkonvention som sakens egna regler.
	is.Equal(th.NameOrID, "Room:A")
	is.Equal(th.TypeName, "room")

	lc, ok := th.Fields["lifecycle"].(map[string]any)
	is.True(ok)
	is.Equal(lc["kind"], "deleted")
	is.Equal(lc["revision"], int64(4))
	is.Equal(lc["changedAt"], "2026-09-17T12:00:00Z")

	is.Equal(th.ObservedAt.UTC().Format("2006-01-02T15:04:05Z"), "2026-09-17T12:00:00Z")
}

func TestLifecycleAdapterMapsCreatedEvent(t *testing.T) {
	is := is.New(t)

	body := `{"eventId":"v1/t/tank-1/lifecycle/created/1","tenant":"t",
		"thingId":"tank-1","kind":"created","thingType":"tank",
		"thingTypeVersion":"1","configRevision":1,
		"changedAt":"2026-09-17T12:00:00Z",
		"thing":{"thingId":"tank-1","name":"Tank 1"}}`
	th, err := ThingLifecycleFromJSON([]byte(body))
	is.NoErr(err)
	is.Equal(th.Lifecycle, "created")
	is.Equal(th.Type, "tank")
}

func TestLifecycleAdapterRejectsMissingThingID(t *testing.T) {
	is := is.New(t)

	_, err := ThingLifecycleFromJSON([]byte(`{"kind":"deleted","tenant":"t"}`))
	is.True(err != nil)
}

func TestLifecycleAdapterRejectsInvalidKind(t *testing.T) {
	is := is.New(t)

	_, err := ThingLifecycleFromJSON([]byte(`{"thingId":"x","kind":"archived","tenant":"t"}`))
	is.True(err != nil)

	_, err = ThingLifecycleFromJSON([]byte(`{"thingId":"x","tenant":"t"}`))
	is.True(err != nil)

	_, err = ThingLifecycleFromJSON([]byte(`not json`))
	is.True(err != nil)
}

func TestLifecycleAdapterToleratesMissingThing(t *testing.T) {
	is := is.New(t)

	// Deleted-händelsen bär alltid deskriptor via producenten, men adaptern
	// ska inte krascha om den saknas: ID/typ från kuvertet gäller.
	th, err := ThingLifecycleFromJSON([]byte(
		`{"thingId":"x","kind":"deleted","thingType":"room","tenant":"t","configRevision":2}`))
	is.NoErr(err)
	is.Equal(th.ID, "x")
	is.Equal(th.Type, "room")
	is.Equal(th.Lifecycle, "deleted")
}
