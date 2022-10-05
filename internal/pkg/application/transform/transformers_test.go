package transform

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/test"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
)

func TestThatWeatherObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", "Temperature", "", &temp, nil, "")

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WeatherObserved(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), fmt.Sprintf(temperaturePropertyFmt, 22.2))) // temperature should be 22.2
}

func TestThatWaterQualityObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3303", "Temperature", "water", &temp, nil, "")

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WaterQualityObserved(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), fmt.Sprintf(temperaturePropertyFmt, *msg.Pack[1].Value))) // temperature should be 22.2
}

func TestThatAirQualityObservedCanBeCreated(t *testing.T) {
	temp := 22.2
	is, pack := testSetup(t, "3428", "CO2", "", &temp, nil, "")

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := AirQualityObserved(context.Background(), msg, cbClient)

	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), fmt.Sprintf(co2PropertyFmt, *msg.Pack[1].Value))) // co2 should be 22.2
}

func TestThatAirQualityIsNotCreatedOnNoValidProperties(t *testing.T) {
	temp := 0.0
	is, pack := testSetup(t, "3428", "", "", &temp, nil, "")

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{}
	err := AirQualityObserved(context.Background(), msg, cbClient)

	is.True(err != nil)
	is.Equal(len(cbClient.CreateEntityCalls()), 0) // should not have been called
}

func TestThatDeviceCanBeCreated(t *testing.T) {
	p := true
	is, pack := testSetup(t, "3302", "Presence", "", nil, &p, "")

	msg := iotcore.NewMessageAccepted("urn:oma:lwm2m:ext:3302", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{Updated: []string{entityID}}, nil
		},
	}

	err := Device(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue))
}

func TestThatLifebuoyCanBeCreated(t *testing.T) {
	p := true
	is, pack := testSetup(t, "3302", "Presence", "lifebuoy", nil, &p, "")

	msg := iotcore.NewMessageAccepted("urn:oma:lwm2m:ext:3302", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{Updated: []string{entityID}}, nil
		},
	}

	err := Lifebuoy(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue))
}

func TestThatWaterConsumptionObservedIsPatchedIfAlreadyExisting(t *testing.T) {
	v := 1.009
	is, pack := testSetup(t, "3424", "CumulatedWaterVolume", "", &v, nil, "")

	pack = append(pack, senml.Record{
		Name:        "DeviceName",
		StringValue: "deviceName",
	},
		senml.Record{
			Name:        "CurrentDateTime",
			StringValue: "2006-01-02T15:04:05.869475538Z",
		})

	msg := iotcore.NewMessageAccepted("watermeter-01", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{}, nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), msg, cbClient)
	is.NoErr(err)

	is.Equal(len(cbClient.UpdateEntityAttributesCalls()), 1) // update entity attributes should have been called once

	expectedEntityID := "urn:ngsi-ld:WaterConsumptionObserved:watermeter-01"
	is.Equal(cbClient.UpdateEntityAttributesCalls()[0].EntityID, expectedEntityID) // the entity id should be ...

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	const expectedPatchBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedPatchBody)
}

func TestThatWaterConsumptionObservedIsCreatedIfNonExisting(t *testing.T) {
	v := 1.009
	is, pack := testSetup(t, "3424", "CumulatedWaterVolume", "", &v, nil, "")

	pack = append(pack, senml.Record{
		Name:        "DeviceName",
		StringValue: "deviceName",
	},
		senml.Record{
			Name:        "CurrentDateTime",
			StringValue: "2006-01-02T15:04:05.869475538Z",
		})

	msg := iotcore.NewMessageAccepted("watermeter-01", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return nil, fmt.Errorf("no such entity")
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := WaterConsumptionObserved(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"id":"urn:ngsi-ld:WaterConsumptionObserved:watermeter-01","type":"WaterConsumptionObserved","waterConsumption":{"type":"Property","value":1009,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:watermeter-01"},"unitCode":"LTR"}}`
	is.Equal(string(b), expectedCreateBody)
}

// GreenspaceRecord test notes:
// Pressure and Condctivity may come as array of values from iot-core.
//  - first occurances of these are treated as primary measurement
//	- subsequent occuranceses are treated as additional measurements and ignored

func TestThatGreenspaceRecordIsCreatedIfNonExisting(t *testing.T) {
	pressure := float64(7)

	is, pack := testSetup(t, "3304/soil", "Pressure", "", &pressure, nil, "")
	pack = append(pack, senml.Record{
		Name:        "DeviceName",
		StringValue: "deviceName",
	},
		senml.Record{
			Name:        "CurrentDateTime",
			StringValue: "2006-01-02T15:04:05.869475538Z",
		})

	msg := iotcore.NewMessageAccepted("soilsensor-01", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return nil, fmt.Errorf("no such entity")
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	err := GreenspaceRecord(context.Background(), msg, cbClient)
	is.NoErr(err)

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"id":"urn:ngsi-ld:GreenspaceRecord:soilsensor-01","location":{"type":"GeoProperty","value":{"type":"Point","coordinates":[17.509804,62.362829]}},"soilMoisturePressure":{"type":"Property","value":7,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:soilsensor-01"},"unitCode":"KPA"},"type":"GreenspaceRecord"}`
	is.Equal(string(b), expectedCreateBody)
}

func TestThatGrenspaceRecordIsPatchedIfNonExisting(t *testing.T) {
	conductivity := float64(536)

	is, pack := testSetup(t, "3304/soil", "Conductivity", "", &conductivity, nil, "")
	pack = append(pack, senml.Record{
		Name:        "DeviceName",
		StringValue: "deviceName",
	},
		senml.Record{
			Name:        "CurrentDateTime",
			StringValue: "2006-01-02T15:04:05.869475538Z",
		})

	msg := iotcore.NewMessageAccepted("soilsensor-01", pack).AtLocation(62.362829, 17.509804)

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{}, nil
		},
	}

	err := GreenspaceRecord(context.Background(), msg, cbClient)
	is.NoErr(err)

	is.Equal(len(cbClient.UpdateEntityAttributesCalls()), 1) // update entity attributes should have been called once

	expectedEntityID := "urn:ngsi-ld:GreenspaceRecord:soilsensor-01"
	is.Equal(cbClient.UpdateEntityAttributesCalls()[0].EntityID, expectedEntityID) // the entity id should be ...

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls())
	const expectedCreateBody string = `{"@context":["https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld"],"id":"urn:ngsi-ld:GreenspaceRecord:soilsensor-01","soilMoisturePressure":{"type":"Property","value":0,"observedAt":"2006-01-02T15:04:05Z","observedBy":{"type":"Relationship","object":"urn:ngsi-ld:Device:soilsensor-01"},"unitCode":"KPA"},"type":"GreenspaceRecord"}`
	is.Equal(string(b), expectedCreateBody)
}

func testSetup(t *testing.T, typeSuffix, typeName, typeEnv string, v *float64, vb *bool, vs string) (*is.I, senml.Pack) {
	is := is.New(t)
	var pack senml.Pack

	pack = append(pack, senml.Record{
		BaseName:    fmt.Sprintf("urn:oma:lwm2m:ext:%s", typeSuffix),
		Name:        "0",
		StringValue: "deviceID",
		BaseTime:    1136214245,
	}, senml.Record{
		Name:        typeName,
		Value:       v,
		BoolValue:   vb,
		StringValue: vs,
	}, senml.Record{
		Name:        "Env",
		StringValue: typeEnv,
	})

	return is, pack
}

const co2PropertyFmt string = `"co2":{"type":"Property","value":%.1f}`
const statusPropertyWithOnValue string = `"status":{"type":"Property","value":"on"}`
const temperaturePropertyFmt string = `"temperature":{"type":"Property","value":%.1f}`
