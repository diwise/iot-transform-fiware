package things

import (
	"context"
	"log/slog"
	"testing"

	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// staticThingsProvider levererar en fast motor till handler-tester.
type staticThingsProvider struct {
	t *appthings.DynamicThingTransformer
}

func (s staticThingsProvider) ThingsTransformer() *appthings.DynamicThingTransformer {
	return s.t
}

// Tom tenant ska hoppas över utan brokeranrop även i dynamiskt läge.
func TestDynamicThingHandlerSkipsEmptyTenant(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)

	body := `{"thingId":"x","tenant":"","thingType":"room","values":[]}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Brokerfel ska propageras (retry) även i dynamiskt läge.
func TestDynamicThingHandlerReturnsBrokerError(t *testing.T) {
	is := is.New(t)

	handler := NewThingValuesTopicMessageHandler(
		func(string) (contextbroker.BrokerClient, error) {
			return nil, errBrokerDown{}
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{{
			Match:    transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "room"},
			Entities: []transform.Entity{{ID: "urn:ngsi-ld:Thing:{{id}}", Type: "Thing", Properties: []transform.Property{{Target: "name", Type: transform.TypeText, Value: "{{name}}"}}}},
		}})},
		"",
	)

	body := `{"thingId":"x","tenant":"default","thingType":"room",
		"thing":{"thingId":"x","name":"Rum"},"values":[]}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.True(handler(context.Background(), itm, slog.Default()) != nil)
}

type errBrokerDown struct{}

func (errBrokerDown) Error() string { return "context broker unavailable" }
