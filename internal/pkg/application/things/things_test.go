package things

import (
	"context"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	testClient "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func TestContainerTopicMessageHandler(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	e := ""
	cb := &testClient.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			e = entityID
			return &ngsild.MergeEntityResult{}, nil
		},
	}
	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{BodyFunc: func() []byte { return []byte(wastecontainerJson) }}

	handler := NewContainerTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:WasteContainer:Soptunnor.XY")
}

const wastecontainerJson = `
{
	"id": "2bf440f4",
	"type": "Container",
	"thing": {
		"currentLevel": 0.91,
		"description": "",
		"id": "2bf440f4",
		"location": {
			"latitude": 62,
			"longitude": 17
		},
		"maxd": 0.94,
		"maxl": 0.79,
		"name": "Soptunnor.X",
		"alternativeName": "Soptunnor.XY",
		"observedAt": "2024-11-19T10:49:59Z",
		"percent": 56,
		"refDevices": [
			{
				"deviceID": "12345"
			}
		],
		"subType": "WasteContainer",
		"tags": [
			"160L"
		],
		"tenant": "default",
		"type": "Container",
		"validURN": [
			"urn:oma:lwm2m:ext:3330"
		]
	},
	"tenant": "default",
	"timestamp": "2024-11-19T10:49:59.748823813Z"
}
`
