package messageprocessor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	client "github.com/diwise/context-broker/pkg/test"
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

func TestThatWeatherObservedCanBeCreatedAndPosted(t *testing.T) {
	is := is.New(t)

	cbClient := &client.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, ngsierrors.ErrNotFound
		},
	}
	val := 22.2
	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3303", "deviceID"), iotcore.Environment("air"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5700", "", &val, nil, 0, nil))

	mp := NewMessageProcessor()
	err := mp.ProcessMessage(context.Background(), *msg, cbClient)

	is.NoErr(err)
	is.Equal(len(cbClient.CreateEntityCalls()), 1) // should have been called once
}

func TestThatLifeBouyCanBeCreatedAndPosted(t *testing.T) {
	is := is.New(t)

	cbClient := &client.ContextBrokerClientMock{
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			return ngsild.NewCreateEntityResult("ignored"), nil
		},
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, ngsierrors.ErrNotFound
		},
	}

	p := true
	msg := iotcore.NewMessageAccepted("deviceID", senml.Pack{}, base("urn:oma:lwm2m:ext:3302", "deviceID"), iotcore.Environment("lifebuoy"), iotcore.Lat(62.362829), iotcore.Lon(17.509804), iotcore.Rec("5500", "", nil, &p, 0, nil))

	mp := NewMessageProcessor()
	err := mp.ProcessMessage(context.Background(), *msg, cbClient)

	is.NoErr(err)
	is.Equal(len(cbClient.CreateEntityCalls()), 1) // expected a single request to context broker

	b, _ := json.Marshal(cbClient.CreateEntityCalls()[0].Entity)
	is.True(strings.Contains(string(b), statusPropertyWithOnValue)) // status should be "on"
}

const statusPropertyWithOnValue string = `"status":{"type":"Property","value":"on"}`
