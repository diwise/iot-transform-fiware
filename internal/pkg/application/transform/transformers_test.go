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
