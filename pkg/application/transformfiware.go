package application

import (
	"context"

	app "github.com/diwise/iot-transform-fiware/internal/pkg/application"
	"github.com/diwise/messaging-golang/pkg/messaging"
	infra "github.com/diwise/service-chassis/pkg/infrastructure/router"
)

type Application interface {
	Start()
}

func New(ctx context.Context, r infra.Router, messenger messaging.MsgContext, contextBrokerUrl string) Application {

	tfw := &impl{}

	messenger.RegisterTopicMessageHandler("message.accepted", app.NewMeasurementTopicMessageHandler(messenger, contextBrokerUrl))
	messenger.RegisterTopicMessageHandler("function.updated", app.NewFunctionUpdatedTopicMessageHandler(messenger, contextBrokerUrl))

	return tfw
}

type impl struct{}

func (tfw *impl) Start() {
	// noop
}
