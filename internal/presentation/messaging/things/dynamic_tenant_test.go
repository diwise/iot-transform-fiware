package things

import (
	"context"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// Tom tenant ska hoppas över utan brokeranrop även i dynamiskt läge.
func TestDynamicThingHandlerSkipsEmptyTenant(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewDynamicThingTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		appthings.NewDynamicThingTransformer([]transform.Rule{}),
	)

	body := `{"thing":{"id":"x","type":"Container","percent":50,"tenant":""},"tenant":"","timestamp":"2024-01-01T00:00:00Z"}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.container+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Brokerfel ska propageras (retry) även i dynamiskt läge.
func TestDynamicThingHandlerReturnsBrokerError(t *testing.T) {
	is := is.New(t)

	handler := NewDynamicThingTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			return nil, errBrokerDown{}
		},
		appthings.NewDynamicThingTransformer([]transform.Rule{{
			Match:    transform.Match{Kind: transform.KindThing, Type: "Container"},
			Entities: []transform.Entity{{ID: "urn:ngsi-ld:Thing:{{nameOrID}}", Type: "Thing"}},
		}}),
	)

	body := `{"thing":{"id":"x","type":"Container","name":"n","percent":50,"tenant":"default"},"tenant":"default","timestamp":"2024-01-01T00:00:00Z"}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.container+json" },
	}

	is.True(handler(context.Background(), itm, slog.Default()) != nil)
}

type errBrokerDown struct{}

func (errBrokerDown) Error() string { return "context broker unavailable" }
