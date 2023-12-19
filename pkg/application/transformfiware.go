package application

import (
	"context"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	app "github.com/diwise/iot-transform-fiware/internal/pkg/application"
	"github.com/diwise/messaging-golang/pkg/messaging"
	infra "github.com/diwise/service-chassis/pkg/infrastructure/router"
)

type Application interface {
	Start()
}

func New(ctx context.Context, r infra.Router, messenger messaging.MsgContext, clientFactory func(string) client.ContextBrokerClient) Application {

	tfw := &impl{}

	messenger.RegisterTopicMessageHandler("message.accepted", app.NewMeasurementTopicMessageHandler(messenger, clientFactory))
	messenger.RegisterTopicMessageHandler("function.updated", app.NewFunctionUpdatedTopicMessageHandler(messenger, clientFactory))
	messenger.RegisterTopicMessageHandlerWithFilter("cip-function.updated", app.NewSewagePumpingStationHandler(messenger, clientFactory), messaging.MatchContentType("application/vnd+diwise.sewagepumpingstation+json"))

	return tfw
}

type impl struct{}

func (tfw *impl) Start() {
	// noop
}
