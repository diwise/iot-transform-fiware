package application

import (
	"context"
	"net/http"
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	testutils "github.com/diwise/service-chassis/pkg/test/http"
	"github.com/diwise/service-chassis/pkg/test/http/expects"
	"github.com/diwise/service-chassis/pkg/test/http/response"
	"github.com/matryer/is"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

var Expects = testutils.Expects
var Returns = testutils.Returns
var method = expects.RequestMethod
var path = expects.RequestPath

func TestHandleUpdatedFunction(t *testing.T) {
	ctx, is, msgCtx := testSetup(t)
	l := zerolog.Logger{}

	ms := testutils.NewMockServiceThat(
		Expects(is,
			method(http.MethodPatch),
			path("/ngsi-ld/v1/entities/urn:ngsi-ld:WaterQualityObserved:beach:a81758fffe04d819"),
		),
		Returns(response.Code(http.StatusCreated)),
	)

	app := New(ctx, nil, msgCtx, ms.URL())
	app.Start()

	topicMessageHandler := msgCtx.RegisterTopicMessageHandlerCalls()[1].Handler
	topicMessageHandler(ctx, amqp091.Delivery{Body: []byte(functionUpdatedMessage)}, l)

	is.True(true)
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
	"waterquality":{"temperature":21.9}
}`
