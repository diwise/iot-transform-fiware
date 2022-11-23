package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/test"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
)

func base(baseName, deviceID string) iotcore.EventDecoratorFunc {
	return func(m *iotcore.MessageAccepted) {
		m.Pack = append(m.Pack, senml.Record{
			BaseName:    baseName,
			BaseTime:    float64(time.Now().UTC().Unix()),
			Name:        "0",
			StringValue: deviceID,
		})
	}
}

func TestThatIndoorEnvironmentObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303/indoors", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("Temperature", "", &temp, nil, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := IndoorEnvironmentObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), `"temperature":{"type":"Property","value":22.2},"type":"IndoorEnvironmentObserved"}`))
}

func TestThatWeatherObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("Temperature", "", &temp, nil, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WeatherObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), `"temperature":{"type":"Property","value":22.2},"type":"WeatherObserved"}`))
}

func TestThatWaterQualityObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303/water", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("Temperature", "", &temp, nil, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WaterQualityObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), `"temperature":{"type":"Property","value":22.2},"type":"WaterQualityObserved"}`))
}

func TestThatAirQualityObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3428", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("CO2", "", &temp, nil, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := AirQualityObserved(context.Background(), *msg, cbClient)

	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), `"co2":{"type":"Property","value":22.2}`))
}

func TestThatAirQualityIsNotCreatedOnNoValidProperties(t *testing.T) {
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804))

	cbClient := &test.ContextBrokerClientMock{}
	err := AirQualityObserved(context.Background(), *msg, cbClient)

	is.True(err != nil)
	is.Equal(len(cbClient.CreateEntityCalls()), 0) // should not have been called
}

func TestThatDeviceCanBeCreated(t *testing.T) {
	p := true
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3302", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("Presence", "", nil, &p, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{Updated: []string{entityID}}, nil
		},
	}

	err := Device(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue))
}

func TestThatLifebuoyCanBeCreated(t *testing.T) {
	p := true
	is := is.New(t)
	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3302/lifebuoy", "deviceID"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("Presence", "", nil, &p, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{Updated: []string{entityID}}, nil
		},
	}

	err := Lifebuoy(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue))
}

func TestThatWaterConsumptionObservedIsPatchedIfAlreadyExisting(t *testing.T) {
	v := 1.009
	is := is.New(t)

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.869475538Z")

	msg := iotcore.NewMessageAccepted("watermeter-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3424", "watermeter-01"),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Rec("CurrentVolume", "", &v, nil, 0, nil),
		iotcore.Rec("CurrentDateTime", "2006-01-02T15:04:05.869475538Z", nil, nil, float64(ct.Unix()), nil))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{}, nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	is.Equal(len(cbClient.UpdateEntityAttributesCalls()), 1) // update entity attributes should have been called once

	expectedEntityID := "urn:ngsi-ld:WaterConsumptionObserved:watermeter-01"
	is.Equal(cbClient.UpdateEntityAttributesCalls()[0].EntityID, expectedEntityID) // the entity id should be ...

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	const expectedPatchBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedPatchBody)
}

func TestThatWaterConsumptionObservedIsCreatedIfNonExisting(t *testing.T) {
	v := 1.009
	is := is.New(t)

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.869475538Z")

	msg := iotcore.NewMessageAccepted("watermeter-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3424", "watermeter-01"),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Rec("CurrentVolume", "", &v, nil, 0, nil),
		iotcore.Rec("CurrentDateTime", "2006-01-02T15:04:05.869475538Z", nil, nil, float64(ct.Unix()), nil))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return nil, fmt.Errorf("no such entity")
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"id":"urn:ngsi-ld:WaterConsumptionObserved:watermeter-01","location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"type":"WaterConsumptionObserved","waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedCreateBody)
}

func TestDeltaVolumes(t *testing.T) {
	is := is.New(t)

	v := 100.009
	dv1 := 10.0

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.869475538Z")

	msg := iotcore.NewMessageAccepted("watermeter-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3424", "watermeter-01"),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Rec("CurrentVolume", "", &v, nil, 0, nil),
		iotcore.Rec("CurrentDateTime", "2006-01-02T15:04:05.869475538Z", nil, nil, float64(ct.Unix()), nil),
		iotcore.Rec("DeltaVolume", "", &dv1, nil, float64(time.Now().UTC().UnixMilli()/1000), &dv1))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{}, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)
}

const statusPropertyWithOnValue string = `"status":{"type":"Property","value":"on"}`
