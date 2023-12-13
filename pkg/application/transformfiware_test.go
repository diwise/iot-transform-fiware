package application

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	cbtest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

func TestHandleUpdatedFunction(t *testing.T) {
	ctx, is, msgCtx, incMsg := testSetup(t, functionUpdatedMessage)
	l := slog.New(slog.NewTextHandler(io.Discard, nil))

	brokerClientMock := &cbtest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			return &ngsild.MergeEntityResult{}, nil
		},
	}

	clientFactory := func(string) client.ContextBrokerClient { return brokerClientMock }

	app := New(ctx, nil, msgCtx, clientFactory)
	app.Start()

	topicMessageHandler := msgCtx.RegisterTopicMessageHandlerCalls()[1].Handler
	topicMessageHandler(ctx, incMsg, l)

	is.Equal(len(brokerClientMock.MergeEntityCalls()), 1)
	is.Equal(brokerClientMock.MergeEntityCalls()[0].EntityID, "urn:ngsi-ld:WaterQualityObserved:beach:a81758fffe04d819")
}

func testSetup(t *testing.T, body string) (context.Context, *is.I, *messaging.MsgContextMock, *messaging.IncomingTopicMessageMock) {
	is := is.New(t)

	msgctx := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerFunc: func(routingKey string, handler messaging.TopicMessageHandler) error {
			return nil
		},
	}

	incMsg := &messaging.IncomingTopicMessageMock{
		BodyFunc: func() []byte {
			msg := functions.Func{}

			err := json.Unmarshal([]byte(body), &msg)
			is.NoErr(err)

			bytes, err := json.Marshal(msg)
			is.NoErr(err)

			return bytes
		},
		TopicNameFunc: func() string {
			return ""
		},
		ContentTypeFunc: func() string {
			return ""
		},
	}

	return context.Background(), is, msgctx, incMsg
}

const functionUpdatedMessage string = `{
	"id": "a81758fffe04d819",
	"type": "waterquality",
	"subtype": "beach",
	"location":{"latitude":62.35277,"longitude":17.374},
	"tenant":"default",
	"waterquality":{"temperature":21.9}
}`
