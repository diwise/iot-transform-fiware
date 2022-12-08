package transform

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/test"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
)

func base(baseName, deviceID string, baseTime time.Time) iotcore.EventDecoratorFunc {
	return func(m *iotcore.MessageAccepted) {
		m.Pack = append(m.Pack, senml.Record{
			BaseName:    baseName,
			BaseTime:    float64(baseTime.Unix()),
			Name:        "0",
			StringValue: deviceID,
		})
	}
}

func TestThatIndoorEnvironmentObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303", "deviceID", time.Now().UTC()), iotcore.Environment("indoors"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5700", "", &temp, nil, 0, nil))

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, ngsierrors.ErrNotFound
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

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303", "deviceID", time.Now().UTC()), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5700", "", &temp, nil, 0, nil))

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

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303", "deviceID", time.Now().UTC()), iotcore.Environment("water"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5700", "", &temp, nil, 0, nil))

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

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3428", "deviceID", time.Now().UTC()), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("17", "", &temp, nil, 0, nil))

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

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("", "deviceID", time.Now().UTC()), iotcore.Lat(62.362829), iotcore.Lon(17.509804))

	cbClient := &test.ContextBrokerClientMock{}
	err := AirQualityObserved(context.Background(), *msg, cbClient)

	is.True(err != nil)
	is.Equal(len(cbClient.CreateEntityCalls()), 0) // should not have been called
}

func TestThatDeviceCanBeCreated(t *testing.T) {
	p := true
	is := is.New(t)

	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3302", "deviceID", time.Now().UTC()), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5500", "", nil, &p, 0, nil))

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
	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3302", "deviceID", time.Now().UTC()), iotcore.Environment("Lifebuoy"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5500", "", nil, &p, 0, nil))

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
		base("urn:oma:lwm2m:ext:3424", "watermeter-01", time.Now().UTC()),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Rec("1", "", &v, nil, float64(ct.Unix()), &v))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{}, nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	is.Equal(len(cbClient.MergeEntityCalls()), 1) // update entity attributes should have been called once

	expectedEntityID := "urn:ngsi-ld:WaterConsumptionObserved:watermeter-01"
	is.Equal(cbClient.MergeEntityCalls()[0].EntityID, expectedEntityID) // the entity id should be ...

	b, _ := json.Marshal(cbClient.MergeEntityCalls()[0].Fragment)
	const expectedPatchBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"alarmStopsLeaks":{"type":"Property","value":0},"alarmWaterQuality":{"type":"Property","value":0},"location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedPatchBody)
}

func TestThatWaterConsumptionObservedIsCreatedIfNonExisting(t *testing.T) {
	v := 1.009
	is := is.New(t)

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05.869475538Z")

	msg := iotcore.NewMessageAccepted("watermeter-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3424", "watermeter-01", time.Now().UTC()),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Rec("1", "", &v, nil, float64(ct.Unix()), &v))

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return nil, ngsierrors.ErrNotFound
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return nil, ngsierrors.ErrNotFound
		},
	}

	err := WaterConsumptionObserved(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"alarmStopsLeaks":{"type":"Property","value":0},"alarmWaterQuality":{"type":"Property","value":0},"id":"urn:ngsi-ld:WaterConsumptionObserved:watermeter-01","location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"type":"WaterConsumptionObserved","waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedCreateBody)
}

// GreenspaceRecord test notes:
// Pressure and Condctivity may come as array of values from iot-core.
//  - first occurrence is treated as primary measurement
//	- subsequent occurrences are treated as additional measurements and ignored

func TestThatGreenspaceRecordIsCreatedIfNonExistant(t *testing.T) {
	pressure := float64(7)
	is := is.New(t)

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05Z")

	msg := iotcore.NewMessageAccepted("soilsensor-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3323", "soilsensor-01", ct),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Environment("soil"),
		iotcore.Rec("5700", "", &pressure, nil, 0, nil))

	msg.Timestamp = "2006-01-02T15:04:05Z"

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return nil, ngsierrors.ErrNotFound
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := GreenspaceRecord(context.Background(), *msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"dateObserved":{"type":"Property","value":{"@type":"DateTime","@value":"2006-01-02T15:04:05Z"}},"id":"urn:ngsi-ld:GreenspaceRecord:soilsensor-01","location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"soilMoisturePressure":{"type":"Property","value":7,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:soilsensor-01"},"unitCode":"KPA"},"type":"GreenspaceRecord"}`
	is.Equal(string(b), expectedCreateBody)
}

func TestThatGrenspaceRecordIsPatchedIfAlreadyExisting(t *testing.T) {
	conductivity := float64(536)
	is := is.New(t)

	ct, _ := time.Parse(time.RFC3339Nano, "2006-01-02T15:04:05Z")

	msg := iotcore.NewMessageAccepted("soilsensor-01", senml.Pack{},
		base("urn:oma:lwm2m:ext:3327", "soilsensor-01", ct),
		iotcore.Lat(62.362829),
		iotcore.Lon(17.509804),
		iotcore.Environment("soil"),
		iotcore.Rec("5700", "", &conductivity, nil, 0, nil))

	msg.Timestamp = "2006-01-02T15:04:05Z"

	cbClient := &test.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	err := GreenspaceRecord(context.Background(), *msg, cbClient)
	is.NoErr(err)

	is.Equal(len(cbClient.MergeEntityCalls()), 1) // Merge entity attributes should have been called once

	expectedEntityID := "urn:ngsi-ld:GreenspaceRecord:soilsensor-01"
	is.Equal(cbClient.MergeEntityCalls()[0].EntityID, expectedEntityID) // the entity id should be ...

	b, _ := json.Marshal(cbClient.MergeEntityCalls())
	const expectedCreateBody string = `[{"Ctx":0,"EntityID":"urn:ngsi-ld:GreenspaceRecord:soilsensor-01","Fragment":{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"dateObserved":{"type":"Property","value":{"@type":"DateTime","@value":"2006-01-02T15:04:05Z"}},"location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"soilMoistureEc":{"type":"Property","value":536,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:soilsensor-01"},"unitCode":"MHO"}},"Headers":{"Content-Type":["application/ld+json"]}}]`
	is.Equal(string(b), expectedCreateBody)
}

const statusPropertyWithOnValue string = `"status":{"type":"Property","value":"on"}`
