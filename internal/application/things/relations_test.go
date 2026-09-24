package things

import (
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/matryer/is"
)

const relationSetBody = `{
	"relationId":"partof-tank-1","type":"partOf",
	"fromId":"tank-1","toId":"greenhouse-a","tenant":"t",
	"revision":2,"changedAt":"2026-09-17T12:00:00Z",
	"fromThingType":"tank",
	"from":{"thingId":"tank-1","name":"Tank 1","category":"tank"},
	"toThingType":"greenhouse",
	"to":{"thingId":"greenhouse-a","name":"Växthus A","category":"greenhouse"}
}`

func TestRelationsAdapterMapsSetEvent(t *testing.T) {
	is := is.New(t)

	th, err := ThingRelationsFromJSON([]byte(relationSetBody))
	is.NoErr(err)
	is.Equal(th.ID, "tank-1")
	is.Equal(th.Type, "tank")
	is.Equal(th.Tenant, "t")
	is.Equal(th.Event, transform.EventThingsV1Relations)
	is.Equal(th.Relation, "partOf")
	is.True(!th.RelationRemoved)
	// Samma namnkonvention som sakens egna regler: name vinner över id,
	// så {{nameOrID}}/{{type}} renderar identiskt med värde-regeln.
	is.Equal(th.NameOrID, "Tank:1")
	is.Equal(th.TypeName, "tank")

	rel, ok := th.Fields["relation"].(map[string]any)
	is.True(ok)
	is.Equal(rel["fromId"], "tank-1")
	is.Equal(rel["toId"], "greenhouse-a")
	is.Equal(rel["removed"], false)

	to, ok := rel["to"].(map[string]any)
	is.True(ok)
	is.Equal(to["typeName"], "greenhouse")
	// Samma sanering som sakens egna regler (åäö/mellanslag → ":").
	is.Equal(to["nameOrID"], "V:xthus:A")
	is.Equal(to["thingId"], "greenhouse-a")

	from, ok := rel["from"].(map[string]any)
	is.True(ok)
	wq, ok := from["waterQualityObserved"].([]any)
	is.True(ok)
	is.Equal(1, len(wq))
	is.Equal("urn:ngsi-ld:WaterQualityObserved:Tank:1", wq[0])
}

func TestRelationsAdapterMapsRemovedEvent(t *testing.T) {
	is := is.New(t)

	body := `{"relationId":"partof-tank-1","type":"partOf","fromId":"tank-1",
		"toId":"greenhouse-a","removed":true,"tenant":"t","revision":3,
		"changedAt":"2026-09-17T12:00:00Z","fromThingType":"tank",
		"from":{"thingId":"tank-1","name":"Tank 1"},
		"toThingType":"greenhouse",
		"to":{"thingId":"greenhouse-a","name":"Växthus A"}}`
	th, err := ThingRelationsFromJSON([]byte(body))
	is.NoErr(err)
	is.True(th.RelationRemoved)
	is.Equal(th.Relation, "partOf")
}

func TestRelationsAdapterRejectsMissingFromID(t *testing.T) {
	is := is.New(t)

	_, err := ThingRelationsFromJSON([]byte(`{"type":"partOf","tenant":"t"}`))
	is.True(err != nil)
}

func TestRelationsAdapterToleratesMissingTo(t *testing.T) {
	is := is.New(t)

	// Äldre producent utan to-sida: adaptern fyller tomma nycklar så att
	// firstPresent hoppar deterministiskt; match.type skyddar mot
	// typelösa händelser (se validering).
	th, err := ThingRelationsFromJSON([]byte(
		`{"relationId":"partof-x","type":"partOf","fromId":"x","tenant":"t","revision":1}`))
	is.NoErr(err)
	rel := th.Fields["relation"].(map[string]any)
	to := rel["to"].(map[string]any)
	is.Equal("", to["nameOrID"])
}
