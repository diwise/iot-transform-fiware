package main

import (
	"testing"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// HARM-003: locks the subscribed topic names. The message.accepted envelope
// is produced by iot-core (pkg/messaging/events); things.v1.* envelopes by
// iot-things-v2 (pkg/types). Renames require a compatibility/migration plan
// and producer-side tests. thing.updated publiceras fortfarande (andra
// konsumenter, t.ex. iot-events) men konsumeras inte längre av denna tjänst
// (PLAN001 steg 7.6).
func TestSubscribedTopicContracts(t *testing.T) {
	is := is.New(t)

	is.Equal(MessageAcceptedTopic, "message.accepted")
	is.Equal(ThingValuesV1Topic, "things.v1.values")
	is.Equal(ThingRelationsV1Topic, "things.v1.relations")
	is.Equal(ThingLifecycleV1Topic, "things.v1.lifecycle")
}

// Motorn registrerar en handler per topic (mätvärde; versionerade
// sakhändelser endast under THINGS_V1_VALUES-flaggan, här av).
func TestRegisterHandlersRegistersDynamicHandlers(t *testing.T) {
	is := is.New(t)

	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	is.NoErr(registerHandlers(&appConfig{messenger: messenger}))

	plain := messenger.RegisterTopicMessageHandlerCalls()
	is.Equal(len(plain), 1)

	topics := map[string]bool{}
	for _, c := range plain {
		topics[c.RoutingKey] = true
	}
	is.True(topics[MessageAcceptedTopic])

	is.Equal(len(messenger.RegisterTopicMessageHandlerWithFilterCalls()), 0)
}

// Våg 4 K: med THINGS_V1_VALUES registreras även versionerade sakhändelser
// (relations/lifecycle med F:s projektionsstore; nil ger direktväg utan
// ändrat ack/retry-kontrakt).
func TestRegisterHandlersWithThingsV1AndStore(t *testing.T) {
	is := is.New(t)

	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	is.NoErr(registerHandlers(&appConfig{messenger: messenger, thingsV1Values: true}))

	topics := map[string]bool{}
	for _, c := range messenger.RegisterTopicMessageHandlerCalls() {
		topics[c.RoutingKey] = true
	}
	is.True(topics[MessageAcceptedTopic])
	is.True(topics[ThingValuesV1Topic])
	is.True(topics[ThingRelationsV1Topic])
	is.True(topics[ThingLifecycleV1Topic])
	is.Equal(len(messenger.RegisterTopicMessageHandlerCalls()), 4)
}
