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
		RetrieveEntityFunc: func(ctx context.Context, entityID string, headers map[string][]string) (types.Entity, error) {
			return nil, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			e = entity.ID()
			return &ngsild.CreateEntityResult{}, nil
		},
	}
	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(wastecontainerJson) },
		ContentTypeFunc: func() string { return "content-type" },
		TopicNameFunc:   func() string { return "topic" },
	}

	handler := NewContainerTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:WasteContainer:Soptunnor.XY")
}

func TestSewerMessage(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	e := ""
	cb := &testClient.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			e = entityID
			return &ngsild.MergeEntityResult{}, nil
		},
		RetrieveEntityFunc: func(ctx context.Context, entityID string, headers map[string][]string) (types.Entity, error) {
			return nil, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			e = entity.ID()
			return &ngsild.CreateEntityResult{}, nil
		},
	}
	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(sewerJson) },
		ContentTypeFunc: func() string { return "content-type" },
		TopicNameFunc:   func() string { return "topic" },
	}

	handler := NewSewerTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:CombinedSewerOverflow:05")
}

func TestPumpingStationMessage(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	e := ""
	cb := &testClient.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			e = entityID
			return &ngsild.MergeEntityResult{}, nil
		},
		RetrieveEntityFunc: func(ctx context.Context, entityID string, headers map[string][]string) (types.Entity, error) {
			return nil, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			e = entity.ID()
			return &ngsild.CreateEntityResult{}, nil
		},
	}
	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(pumpingStationJson) },
		ContentTypeFunc: func() string { return "content-type" },
		TopicNameFunc:   func() string { return "topic" },
	}

	handler := NewPumpingstationTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:SewagePumpingStation:pump-001")
}

/*
func TestPumpingStationMessageIntegration(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	e := ""
	cb := client.NewContextBrokerClient("http://localhost:63471", client.Debug("true"), client.Tenant("default"))

	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{BodyFunc: func() []byte { return []byte(pumpingStationJson) }}

	handler := NewPumpingstationTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:SewagePumpingStation:pump-001")
}
*/
/*
func TestSewerMessageIntegration(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	e := ""
	cb := client.NewContextBrokerClient("http://localhost:1026", client.Tenant("default"), client.Debug("true"))

	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{BodyFunc: func() []byte { return []byte(sewerJson) }}

	handler := NewSewerTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(e, "urn:ngsi-ld:CombinedSewerOverflow:05")
}
*/

const wastecontainerJson = `{
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
}`

const sewerJson = `
{
  "id": "25ba0559-3d49-4853-a537-3bbf7d2ae777",
  "type": "Sewer",
  "thing": {
    "id": "25ba0559-3d49-4853-a537-3bbf7d2ae777",
    "type": "Sewer",
    "subType": "CombinedSewerOverflow",
    "name": "05",
    "description": null,
    "location": {
      "latitude": 62.395275,
      "longitude": 17.462769
    },
    "refDevices": [
      {
        "deviceID": "eef259d2-0cf9-5fa3-82e6-e8f95159e931"
      }
    ],
    "observedAt": "2024-11-27T06:12:58Z",
    "tenant": "default",
    "currentLevel": 0,
    "percent": 0,
    "overflowObserved": false,
    "overflowObservedAt": null,
    "overflowDuration": null,
    "overflowCumulativeTime": 0
  },
  "tenant": "default",
  "timestamp": "2024-12-09T09:48:39.31863409Z"
}
`

const pumpingStationJson = `{"id":"pump-001","type":"PumpingStation","thing":{"id":"pump-001","location":{"latitude":0,"longitude":0},"name":"","observedAt":"2025-01-15T07:47:38Z","pumpingCumulativeTime":0,"pumpingDuration":null,"pumpingObserved":false,"pumpingObservedAt":null,"refDevices":[{"deviceID":"ce3acc09ab62"}],"tenant":"default","type":"PumpingStation","validURN":["urn:oma:lwm2m:ext:3200"]},"tenant":"default","timestamp":"2025-01-15T07:47:40.360378603Z"}`


func TestBeachMessage(t *testing.T) {
	is := is.New(t)

	ctx := context.Background()

	observationID := ""
	cb := &testClient.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			observationID = entityID
			return &ngsild.MergeEntityResult{}, nil
		},
		RetrieveEntityFunc: func(ctx context.Context, entityID string, headers map[string][]string) (types.Entity, error) {
			return nil, nil
		},
		CreateEntityFunc: func(ctx context.Context, entity types.Entity, headers map[string][]string) (*ngsild.CreateEntityResult, error) {
			observationID = entity.ID()
			return &ngsild.CreateEntityResult{}, nil
		},
	}
	msgCtx := &messaging.MsgContextMock{}
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(pointOfInterestJson) },
		ContentTypeFunc: func() string { return "content-type" },
		TopicNameFunc:   func() string { return "topic" },
	}

	handler := NewPointOfInterestTopicMessageHandler(msgCtx, func(s string) client.ContextBrokerClient {
		return cb
	})

	handler(ctx, itm, slog.Default())

	is.Equal(observationID, "urn:ngsi-ld:WaterQualityObserved:09089d61-8f40-5ac8-a631-c940dab1fc9b")
}


const pointOfInterestJson = `{
  "id": "71ed07e4-52c0-417c-be15-3110b8e1f4e8",
  "type": "PointOfInterest",
  "thing": {
    "current": {
      "ref": "09089d61-8f40-5ac8-a631-c940dab1fc9b",
      "timestamp": "2026-03-23T16:20:30Z",
      "v": 21.1
    },
    "id": "71ed07e4-52c0-417c-be15-3110b8e1f4e8",
    "location": {
      "latitude": 0,
      "longitude": 0
    },
    "name": "Teststrand",
    "observedAt": "2026-03-23T16:20:30Z",
    "refDevices": [
      {
        "deviceID": "3e85f04b-a64b-51a6-a3a9-e33aaf11986e"
      },
      {
        "deviceID": "d549c148-e73e-5cb6-bb74-96b588c59258"
      },
      {
        "deviceID": "09089d61-8f40-5ac8-a631-c940dab1fc9b"
      }
    ],
    "subType": "Beach",
    "temperature": {
      "ref": "09089d61-8f40-5ac8-a631-c940dab1fc9b",
      "timestamp": "2026-03-23T16:20:30Z",
      "v": 21.1
    },
    "tenant": "default",
    "type": "PointOfInterest",
    "validURN": [
      "urn:oma:lwm2m:ext:3303"
    ]
  },
  "tenant": "default",
  "timestamp": "2026-03-23T16:21:34.397588165Z"
}`
