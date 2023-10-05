package application

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	cbtest "github.com/diwise/context-broker/pkg/test"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
	"github.com/rabbitmq/amqp091-go"
)

func TestHandleUpdatedFunction(t *testing.T) {
	ctx, is, msgCtx := testSetup(t)
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
	topicMessageHandler(ctx, amqp091.Delivery{Body: []byte(functionUpdatedMessage)}, l)

	is.Equal(len(brokerClientMock.MergeEntityCalls()), 1)
	is.Equal(brokerClientMock.MergeEntityCalls()[0].EntityID, "urn:ngsi-ld:WaterQualityObserved:beach:a81758fffe04d819")
}

func testSetup(t *testing.T) (context.Context, *is.I, *messaging.MsgContextMock) {
	is := is.New(t)

	msgctx := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) {
		},
	}

	return context.Background(), is, msgctx
}

const functionUpdatedMessage string = `{
	"id": "a81758fffe04d819",
	"type": "waterquality",
	"subtype": "beach",
	"location":{"latitude":62.35277,"longitude":17.374},
	"tenant":"default",
	"data":{"temperature":21.9}
}`
