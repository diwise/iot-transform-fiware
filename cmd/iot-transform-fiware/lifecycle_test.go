package main

import (
	"context"
	"errors"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// BASE-001: handler registration failures must propagate instead of
// being silently ignored at startup.
func TestRegisterHandlersPropagatesError(t *testing.T) {
	is := is.New(t)

	want := errors.New("registration failed")
	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerWithFilterFunc: func(string, messaging.TopicMessageHandler, messaging.MessageFilter) error {
			return want
		},
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	err := registerHandlers(messenger, func(string) client.ContextBrokerClient { return nil })
	is.True(err != nil)
	is.True(errors.Is(err, want))
}

// BASE-001: locks the current registration set (9 thing handlers with
// content type filters + 1 measurements handler) and their topics.
func TestRegisterHandlersRegistersAllHandlers(t *testing.T) {
	is := is.New(t)

	messenger := &messaging.MsgContextMock{
		RegisterTopicMessageHandlerWithFilterFunc: func(string, messaging.TopicMessageHandler, messaging.MessageFilter) error {
			return nil
		},
		RegisterTopicMessageHandlerFunc: func(string, messaging.TopicMessageHandler) error {
			return nil
		},
	}

	is.NoErr(registerHandlers(messenger, func(string) client.ContextBrokerClient { return nil }))

	filtered := messenger.RegisterTopicMessageHandlerWithFilterCalls()
	is.Equal(len(filtered), 9)
	for _, c := range filtered {
		is.Equal(c.RoutingKey, ThingUpdatedTopic)
	}

	plain := messenger.RegisterTopicMessageHandlerCalls()
	is.Equal(len(plain), 1)
	is.Equal(plain[0].RoutingKey, MessageAcceptedTopic)
}

// BASE-001: initialize must reject an empty context broker URL instead
// of starting a service that cannot deliver anything.
func TestInitializeRejectsEmptyContextBrokerURL(t *testing.T) {
	is := is.New(t)

	flags := defaultFlags()
	flags[contextbrokerUrl] = ""

	_, err := initialize(context.Background(), flags, &AppConfig{})
	is.True(err != nil)
}

// BASE-001: an OAuth token failure must not crash the process. The
// factory falls back to a client without authorization header so the
// failure surfaces as a logged context broker error instead of a panic.
func TestTokenFailureDoesNotPanic(t *testing.T) {
	is := is.New(t)

	factory := contextbroker.NewContextBrokerClientFactory(
		context.Background(),
		"http://context-broker", serviceName, "test",
		"client-id", "client-secret", "http://127.0.0.1:1/token",
		false,
	)

	// Would panic before BASE-001; a panic here fails the test.
	c := factory("default")
	is.True(c != nil)
}
