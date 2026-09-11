package things

import (
	"strings"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const poiRules = `
rules:
  - match: { kind: thing, subType: Beach }
    entities:
      - name: beach
        id: "urn:ngsi-ld:Beach:{{nameOrID}}"
        type: Beach
        create: true
        properties:
          - { target: name, type: Text, field: name, observedAt: none, required: true }
          - { target: location, type: Geo, field: location, required: true }
      - id: "urn:ngsi-ld:WaterQualityObserved:{{firstPresent(field:current.ref, nameOrID)}}"
        type: WaterQualityObserved
        properties:
          - { target: refPointOfInterest, type: Relationship, value: "{{entities.beach.id}}" }
          - { target: refDevice, type: Relationship, field: "current.ref", prefix: "urn:ngsi-ld:Device:" }
          - { target: location, type: Geo, field: location, required: true }
          - { target: dateObserved, type: DateTime, field: observedAt, required: true }
          - { target: temperature, type: Number, field: "current.v" }
          - { target: description, type: Text, field: description, observedAt: none }
          - { target: source, type: Text, field: "current.source", observedAt: none }
  - match: { kind: thing, type: PointOfInterest }
    entities:
      - id: "urn:ngsi-ld:PointOfInterest:{{nameOrID}}"
        type: PointOfInterest
        properties:
          - { target: name, type: Text, field: name, observedAt: none, required: true }
          - { target: description, type: Text, field: description, observedAt: none }
          - { target: location, type: Geo, field: location }
`

const beachJSON = `{"thing":{"current":{"ref":"09089d61-8f40-5ac8-a631-c940dab1fc9b","timestamp":"2026-03-23T16:20:30Z","v":21.1},"id":"71ed07e4-52c0-417c-be15-3110b8e1f4e8","location":{"latitude":0,"longitude":0},"name":"Teststrand","observedAt":"2026-03-23T16:20:30Z","subType":"Beach","tenant":"default","type":"PointOfInterest"},"tenant":"default","timestamp":"2026-03-23T16:21:34.397588165Z"}`

func TestBeachPointOfInterestIsModelConformant(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(beachJSON))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(poiRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 2)

	beach := writes[0]
	is.Equal(beach.EntityID, "urn:ngsi-ld:Beach:Teststrand")
	is.Equal(beach.TypeName, "Beach")
	is.True(beach.Create)

	wqo := writes[1]
	is.Equal(wqo.EntityID, "urn:ngsi-ld:WaterQualityObserved:09089d61-8f40-5ac8-a631-c940dab1fc9b")
	is.Equal(wqo.TypeName, "WaterQualityObserved")

	beachJSONOut := marshalEntity(t, beach.EntityID, beach.TypeName, beach.Props)
	is.True(strings.Contains(beachJSONOut, `"name":{"type":"Property","value":"Teststrand"`))
	is.True(strings.Contains(beachJSONOut, `"location":{"type":"GeoProperty"`))

	wqoJSON := marshalEntity(t, wqo.EntityID, wqo.TypeName, wqo.Props)
	is.True(strings.Contains(wqoJSON, `"refPointOfInterest":{"type":"Relationship","object":"urn:ngsi-ld:Beach:Teststrand"`))
	is.True(strings.Contains(wqoJSON, `"refDevice":{"type":"Relationship","object":"urn:ngsi-ld:Device:09089d61-8f40-5ac8-a631-c940dab1fc9b"`))
	is.True(strings.Contains(wqoJSON, `"temperature":{"type":"Property","value":21.1`))
	is.True(strings.Contains(wqoJSON, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(wqoJSON, `"dateObserved":{"type":"Property"`))
}

const plainPoiJSON = `{"thing":{"id":"poi-1","location":{"latitude":1,"longitude":2},"name":"Torg","description":"Ett torg","tenant":"default","type":"PointOfInterest"},"tenant":"default","timestamp":"2026-03-23T16:21:34.397588165Z"}`

func TestPlainPointOfInterestIsModelConformant(t *testing.T) {
	is := is.New(t)

	th, err := ThingFromJSON([]byte(plainPoiJSON))
	is.NoErr(err)

	rules, err := config.ParseModel([]byte(poiRules))
	is.NoErr(err)

	writes, err := NewDynamicThingTransformer(rules).Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	is.Equal(writes[0].EntityID, "urn:ngsi-ld:PointOfInterest:Torg")
	is.Equal(writes[0].TypeName, "PointOfInterest")

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"name":{"type":"Property","value":"Torg"`))
	is.True(strings.Contains(b, `"description":{"type":"Property","value":"Ett torg"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
}
