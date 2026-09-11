package measurements

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

type captureWriter struct {
	id       string
	typeName string
	props    []entities.EntityDecoratorFunc
}

func (c *captureWriter) MergeOrCreate(_ context.Context, _ client.ContextBrokerClient, id, typeName string, props []entities.EntityDecoratorFunc) error {
	c.id, c.typeName, c.props = id, typeName, props
	return nil
}

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

const weatherRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303", env: air }
    entities:
      - id: "urn:ngsi-ld:WeatherObserved:{{deviceID}}"
        type: WeatherObserved
        properties:
          - { target: temperature, type: Number, resource: "5700", observedAt: header }
          - { target: source, type: Text, name: source, observedAt: none }
          - { target: location, type: Geo, latlon: true }
          - { target: dateObserved, type: DateTime, timestamp: true }
`

func TestWeatherObservedMatchesDynamicEngine(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: weatherPack(ts), Timestamp: ts}

	writer := &captureWriter{}
	is.NoErr(WeatherObserved(context.Background(), msg, nil, writer))
	staticJSON := marshalEntity(t, writer.id, writer.typeName, writer.props)

	rules, err := config.ParseModel([]byte(weatherRules))
	is.NoErr(err)
	is.NoErr(transform.Validate(rules))

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	dynamicJSON := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)

	is.Equal(dynamicJSON, staticJSON)
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

const noiseRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3324" }
    entities:
      - id: "urn:ngsi-ld:NoiseLevelObserved:{{deviceID}}"
        type: NoiseLevelObserved
        properties:
          - { target: noiseLevel, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true }
          - { target: dateObserved, type: DateTime, timestamp: true }
`

func TestNoiseLevelObservedMatchesDynamicEngine(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: noisePack(ts), Timestamp: ts}

	writer := &captureWriter{}
	is.NoErr(NoiseLevelObserved(context.Background(), msg, nil, writer))
	staticJSON := marshalEntity(t, writer.id, writer.typeName, writer.props)

	rules, err := config.ParseModel([]byte(noiseRules))
	is.NoErr(err)
	is.NoErr(transform.Validate(rules))

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	dynamicJSON := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)

	is.Equal(dynamicJSON, staticJSON)
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
            required: true
            unit: LTR
            observedAt: record
            observedBy: "urn:ngsi-ld:Device:{{deviceID}}"
            transform:
              - { op: add, value: 0.0005 }
              - { op: multiply, value: 1000 }
              - { op: floor }
`

func TestWaterConsumptionObservedMatchesDynamicEngine(t *testing.T) {
	is := is.New(t)

	ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: waterPack(ts), Timestamp: ts}

	writer := &captureWriter{}
	is.NoErr(WaterConsumptionObserved(context.Background(), msg, nil, writer))
	staticJSON := marshalEntity(t, writer.id, writer.typeName, writer.props)

	rules, err := config.ParseModel([]byte(waterRules))
	is.NoErr(err)
	is.NoErr(transform.Validate(rules))

	writes, err := NewDynamicTransformer(rules, nil).Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)
	dynamicJSON := marshalEntity(t, writes[0].EntityID, writes[0].TypeName, writes[0].Props)

	is.Equal(dynamicJSON, staticJSON)
}

func ptr(f float64) *float64 { return &f }
