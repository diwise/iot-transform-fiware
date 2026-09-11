package measurements

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

func marshalEntity(t *testing.T, id, typeName string, props []entities.EntityDecoratorFunc) string {
	t.Helper()

	e, err := entities.New(id, typeName, props...)
	if err != nil {
		t.Fatalf("entities.New: %v", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	return string(b)
}

func transformFirst(t *testing.T, rulesYAML string, msg events.MessageAccepted) string {
	t.Helper()

	rules, err := config.ParseModel([]byte(rulesYAML))
	if err != nil {
		t.Fatalf("ParseModel: %v", err)
	}

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if len(writes) != 1 {
		t.Fatalf("expected exactly one write, got %d", len(writes))
	}

	return marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
}

func withoutLocation(pack senml.Pack) senml.Pack {
	out := make(senml.Pack, 0, len(pack))
	for _, r := range pack {
		if r.Unit == senml.UnitLat || r.Unit == senml.UnitLon {
			continue
		}
		out = append(out, r)
	}
	return out
}

const weatherRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303", env: air }
    entities:
      - id: "urn:ngsi-ld:WeatherObserved:{{deviceID}}"
        type: WeatherObserved
        properties:
          - { target: temperature, type: Number, resource: "5700", observedAt: header }
          - { target: source, type: Text, name: source, observedAt: none }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
`

func TestWeatherObservedIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: weatherPack(ts), Timestamp: ts}

	b := transformFirst(t, weatherRules, msg)
	is.True(strings.Contains(b, `"temperature":{"type":"Property","value":22.5`))
	is.True(strings.Contains(b, `"source":{"type":"Property","value":"outdoor"`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(b, `"dateObserved":{"type":"Property"`))
}

func TestWeatherObservedWithoutRequiredLocationIsSkipped(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: withoutLocation(weatherPack(ts)), Timestamp: ts}

	rules, err := config.ParseModel([]byte(weatherRules))
	is.NoErr(err)

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 0)
}

const noiseRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3324" }
    entities:
      - id: "urn:ngsi-ld:NoiseLevelObserved:{{deviceID}}"
        type: NoiseLevelObserved
        properties:
          - { target: LAeq, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObservedFrom, type: DateTime, timestamp: true, required: true }
          - { target: dateObservedTo, type: DateTime, timestamp: true, required: true }
`

func TestNoiseLevelObservedIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: noisePack(ts), Timestamp: ts}

	b := transformFirst(t, noiseRules, msg)
	is.True(strings.Contains(b, `"LAeq":{"type":"Property","value":55`))
	is.True(strings.Contains(b, `"location":{"type":"GeoProperty"`))
	is.True(strings.Contains(b, `"dateObservedFrom":{"type":"Property"`))
	is.True(strings.Contains(b, `"dateObservedTo":{"type":"Property"`))
	is.True(!strings.Contains(b, `"noiseLevel"`))
	is.True(!strings.Contains(b, `"dateObserved":`))
}

const waterRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3424" }
    entities:
      - id: "urn:ngsi-ld:WaterConsumptionObserved:{{deviceID}}"
        type: WaterConsumptionObserved
        properties:
          - { target: location, type: Geo, latlon: true }
          - target: alarmStopsLeaks
            type: Number
            observedAt: none
            derive: { fn: anyTrue, of: [{ resource: "9" }] }
          - target: alarmWaterQuality
            type: Number
            observedAt: none
            derive: { fn: anyTrue, of: [{ resource: "11" }] }
          - target: alarmTamper
            type: Number
            observedAt: none
            derive: { fn: anyTrue, of: [{ resource: "64007" }] }
          - target: alarmInProgress
            type: Number
            observedAt: none
            derive: { fn: anyTrue, of: [{ resource: "9" }, { resource: "11" }, { resource: "64007" }] }
          - { target: alternateName, type: Text, resource: "3", observedAt: none }
          - target: waterConsumption
            type: Number
            resource: "1"
            unit: MTQ
            observedAt: record
            observedBy: "urn:ngsi-ld:Device:{{deviceID}}"
`

func TestWaterConsumptionObservedIsModelConformant(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: waterPack(ts), Timestamp: ts}

	b := transformFirst(t, waterRules, msg)
	is.True(strings.Contains(b, `"waterConsumption":{"type":"Property","value":12.3456`))
	is.True(strings.Contains(b, `"unitCode":"MTQ"`))
	is.True(strings.Contains(b, `"alarmInProgress":{"type":"Property","value":1`))
	is.True(strings.Contains(b, `"alarmStopsLeaks":{"type":"Property","value":1`))
	is.True(!strings.Contains(b, `"LTR"`))
}

func TestWaterConsumptionWithoutVolumeStillWritesAlarms(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pack := waterPack(ts)

	noVolume := make(senml.Pack, 0, len(pack))
	for _, r := range pack {
		if r.Name == "dev-1/1" {
			continue
		}
		noVolume = append(noVolume, r)
	}

	msg := events.MessageAccepted{Pack_: noVolume, Timestamp: ts}

	rules, err := config.ParseModel([]byte(waterRules))
	is.NoErr(err)

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	b := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)
	is.True(strings.Contains(b, `"alarmStopsLeaks"`))
	is.True(!strings.Contains(b, `"waterConsumption"`))
}

func boolptr(b bool) *bool { return &b }

func waterPack(ts time.Time) senml.Pack {
	recTime := ts.Add(30 * time.Second)
	return senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3424", Time: float64(ts.Unix())},
		{Name: "dev-1/1", Value: ptr(12.3456), Time: float64(recTime.Unix())},
		{Name: "dev-1/3", StringValue: "watermeter"},
		{Name: "dev-1/9", BoolValue: boolptr(true)},
		{Name: "dev-1/11", BoolValue: boolptr(false)},
		{Name: "dev-1/64007", BoolValue: boolptr(false)},
		{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func weatherPack(ts time.Time) senml.Pack {
	return senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3303", Time: float64(ts.Unix())},
		{Name: "dev-1/5700", Value: ptr(22.5)},
		{Name: "dev-1/source", StringValue: "outdoor"},
		{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/env", StringValue: "air"},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func noisePack(ts time.Time) senml.Pack {
	return senml.Pack{
		{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3324", Time: float64(ts.Unix())},
		{Name: "dev-1/5700", Value: ptr(55.0)},
		{Name: "dev-1/lat", Value: ptr(62.36), Unit: senml.UnitLat},
		{Name: "dev-1/lon", Value: ptr(17.51), Unit: senml.UnitLon},
		{Name: "dev-1/tenant", StringValue: "default"},
	}
}

func ptr(f float64) *float64 { return &f }
