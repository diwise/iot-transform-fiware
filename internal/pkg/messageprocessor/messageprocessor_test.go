package messageprocessor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestThatWeatherObservedCanBeCreatedAndPosted(t *testing.T) {
	is, _, pack := testSetup(t, func() senml.Pack {
		var pack senml.Pack
		val := 22.2
		pack = append(pack, senml.Record{
			BaseName:    "urn:oma:lwm2m:ext:3303/air",
			Name:        "0",
			StringValue: "deviceID",
		}, senml.Record{
			Name:  "Temperature",
			Value: &val,
		})
		return pack
	})

	cbClient := &test.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
	}

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(cbClient)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
	is.Equal(len(cbClient.CreateEntityCalls()), 1) // should have been called once
}

func TestThatLifeBouyCanBeCreatedAndPosted(t *testing.T) {
	is, _, pack := testSetup(t, func() senml.Pack {
		var pack senml.Pack
		val := true
		pack = append(pack, senml.Record{
			BaseName:    "urn:oma:lwm2m:ext:3302/lifebuoy",
			Name:        "0",
			StringValue: "deviceID",
		}, senml.Record{
			Name:      measurements.Presence,
			BoolValue: &val,
		})
		return pack
	})

	cbClient := &test.ContextBrokerClientMock{
		UpdateEntityAttributesFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.UpdateEntityAttributesResult, error) {
			return &ngsild.UpdateEntityAttributesResult{Updated: []string{entityID}}, nil
		},
	}

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(cbClient)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
	is.Equal(len(cbClient.UpdateEntityAttributesCalls()), 1) // expected a single request to context broker

	b, _ := json.Marshal(cbClient.UpdateEntityAttributesCalls()[0].Fragment)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue)) // status should be "on"
}

func testSetup(t *testing.T, fn func() senml.Pack) (*is.I, zerolog.Logger, senml.Pack) {
	is := is.New(t)
	pack := fn()
	return is, zerolog.Logger{}, pack
}

const statusPropertyWithOnValue string = `"status":{"type":"Property","value":"on"}`
