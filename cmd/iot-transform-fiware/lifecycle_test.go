package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/presentation/messaging/things"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// FINAL-001: readiness-stubben rapporterar alltid OK utan att röra
// något beroende.
func TestReadinessStubsAlwaysOK(t *testing.T) {
	is := is.New(t)

	probes := readinessProbes()
	is.Equal(len(probes), 1)

	status, err := probes["rabbitmq"](context.Background())
	is.NoErr(err)
	is.Equal(status, "ok")
}

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

	err := registerHandlers(messenger, func(string) (client.ContextBrokerClient, error) { return nil, nil })
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

	is.NoErr(registerHandlers(messenger, func(string) (client.ContextBrokerClient, error) { return nil, nil }))

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

	_, err := initialize(context.Background(), flags, &appConfig{})
	is.True(err != nil)
}

// REV-001: a configured OAuth token failure must abort the delivery
// without any broker call (no panic, no anonymous fallback). Verified
// through the real container handler, factory and broker call path.
func TestTokenFailureAbortsDeliveryWithoutBrokerCall(t *testing.T) {
	is := is.New(t)

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer tokenServer.Close()

	var brokerCalls atomic.Int64
	brokerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer brokerServer.Close()

	factory := contextbroker.NewContextBrokerClientFactory(
		context.Background(),
		brokerServer.URL, serviceName, "test",
		"client-id", "client-secret", tokenServer.URL,
		false,
	)

	_, err := factory("default")
	is.True(err != nil)

	handler := things.NewContainerTopicMessageHandler(factory)
	handler(context.Background(), containerMessage(t), slog.Default())

	is.Equal(brokerCalls.Load(), int64(0))
}

// REV-001: a valid token produces exactly one authorized broker request.
func TestSuccessfulDeliverySendsAuthorizedBrokerRequest(t *testing.T) {
	is := is.New(t)

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"access_token":"test-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer tokenServer.Close()

	var brokerCalls atomic.Int64
	var authHeader string
	var mu sync.Mutex
	brokerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerCalls.Add(1)
		mu.Lock()
		authHeader = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer brokerServer.Close()

	factory := contextbroker.NewContextBrokerClientFactory(
		context.Background(),
		brokerServer.URL, serviceName, "test",
		"client-id", "client-secret", tokenServer.URL,
		false,
	)

	handler := things.NewContainerTopicMessageHandler(factory)
	handler(context.Background(), containerMessage(t), slog.Default())

	is.Equal(brokerCalls.Load(), int64(1))
	mu.Lock()
	defer mu.Unlock()
	is.Equal(authHeader, "Bearer test-token")
}

// REV-001: deliberately unconfigured OAuth keeps working anonymously,
// with exactly one broker request and no Authorization header.
func TestAnonymousModeSkipsAuthorizationHeader(t *testing.T) {
	is := is.New(t)

	var brokerCalls atomic.Int64
	var authHeader string
	var mu sync.Mutex
	brokerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		brokerCalls.Add(1)
		mu.Lock()
		authHeader = r.Header.Get("Authorization")
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer brokerServer.Close()

	factory := contextbroker.NewContextBrokerClientFactory(
		context.Background(),
		brokerServer.URL, serviceName, "test",
		"", "", "",
		false,
	)

	handler := things.NewContainerTopicMessageHandler(factory)
	handler(context.Background(), containerMessage(t), slog.Default())

	is.Equal(brokerCalls.Load(), int64(1))
	mu.Lock()
	defer mu.Unlock()
	is.Equal(authHeader, "")
}

func containerMessage(t *testing.T) *messaging.IncomingTopicMessageMock {
	t.Helper()

	return &messaging.IncomingTopicMessageMock{
		BodyFunc: func() []byte { return []byte(containerEnvelopeJson) },
		ContentTypeFunc: func() string {
			return "application/vnd.diwise.container+json"
		},
		TopicNameFunc: func() string { return ThingUpdatedTopic },
	}
}

const containerEnvelopeJson = `{
	"id": "2bf440f4",
	"type": "Container",
	"thing": {
		"id": "2bf440f4",
		"type": "Container",
		"subType": "WasteContainer",
		"name": "Soptunnor.X",
		"alternativeName": "Soptunnor.XY",
		"location": {
			"latitude": 62,
			"longitude": 17
		},
		"observedAt": "2024-11-19T10:49:59Z",
		"percent": 56,
		"tenant": "default"
	},
	"tenant": "default",
	"timestamp": "2024-11-19T10:49:59.748823813Z"
}`
