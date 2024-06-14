package application

import (
	"context"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	app "github.com/diwise/iot-transform-fiware/internal/pkg/application"
	"github.com/diwise/messaging-golang/pkg/messaging"
	infra "github.com/diwise/service-chassis/pkg/infrastructure/router"
)

const (
	FunctionUpdatedTopic    string = "function.updated"
	CipFunctionUpdatedTopic string = "cip-function.updated"
	MessageAcceptedTopic    string = "message.accepted"
)

type Application interface {
	Start()
}

func New(ctx context.Context, r infra.Router, messenger messaging.MsgContext, clientFactory func(string) client.ContextBrokerClient) Application {

	tfw := &impl{}

	messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, app.NewMeasurementTopicMessageHandler(messenger, clientFactory))
	messenger.RegisterTopicMessageHandler(FunctionUpdatedTopic, app.NewFunctionUpdatedTopicMessageHandler(messenger, clientFactory))
	messenger.RegisterTopicMessageHandler(CipFunctionUpdatedTopic, app.NewCipFunctionUpdatedTopicMessageHandler(messenger, clientFactory))

	/*
		TODO: use this registration instead of switch statement in application.go
		
		messenger.RegisterTopicMessageHandlerWithFilter(CipFunctionUpdatedTopic, app.NewSewagePumpingStationHandler(messenger, clientFactory), messaging.MatchContentType("application/vnd.diwise.sewagepumpingstation+json"))
		messenger.RegisterTopicMessageHandlerWithFilter(CipFunctionUpdatedTopic, app.NewWasteContainerHandler(messenger, clientFactory), messaging.MatchContentType("application/vnd.diwise.wastecontainer+json"))
		messenger.RegisterTopicMessageHandlerWithFilter(CipFunctionUpdatedTopic, app.NewSewerHandler(messenger, clientFactory), messaging.MatchContentType("application/vnd.diwise.sewer+json"))
		messenger.RegisterTopicMessageHandlerWithFilter(CipFunctionUpdatedTopic, app.NewCombinedSewageOverflowHandler(messenger, clientFactory), messaging.MatchContentType("application/vnd.diwise.combinedsewageoverflow+json"))
	*/

	return tfw
}

type impl struct{}

func (tfw *impl) Start() {
	// noop
}
