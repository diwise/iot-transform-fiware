package main

import (
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// HARM-003: locks the subscribed topic names. The thing.updated envelope
// shape is produced by iot-things (pkg/types.ThingUpdated) and the
// message.accepted envelope by iot-core (pkg/messaging/events); renames
// require a compatibility/migration plan and producer-side tests.
func TestSubscribedTopicContracts(t *testing.T) {
	is := is.New(t)

	is.Equal(ThingUpdatedTopic, "thing.updated")
	is.Equal(MessageAcceptedTopic, "message.accepted")
}

// Den dynamiska motorn registrerar en handler per topic (sak + mätvärde).
func TestRegisterHandlersRegistersDynamicHandlers(t *testing.T) {
	is := is.New(t)

	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	is.NoErr(registerHandlers(&appConfig{messenger: messenger}))

	plain := messenger.RegisterTopicMessageHandlerCalls()
	is.Equal(len(plain), 2)

	topics := map[string]bool{}
	for _, c := range plain {
		topics[c.RoutingKey] = true
	}
	is.True(topics[ThingUpdatedTopic])
	is.True(topics[MessageAcceptedTopic])

	is.Equal(len(messenger.RegisterTopicMessageHandlerWithFilterCalls()), 0)
}
