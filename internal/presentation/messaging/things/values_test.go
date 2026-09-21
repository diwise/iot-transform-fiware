package things

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

// Strukturskada i värdesevent är permanent (ingen retry läker den).
func TestThingValuesHandlerMalformedIsPermanent(t *testing.T) {
	is := is.New(t)

	handler := NewThingValuesTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) { return nil, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte("{ojsan") },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	err := handler(context.Background(), itm, slog.Default())
	is.True(err != nil)
	var permanent *messaging.PermanentError
	is.True(errors.As(err, &permanent))
}

// Tom tenant skippas utan broker-kontakt.
func TestThingValuesHandlerSkipsEmptyTenant(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingValuesTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
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

// Saknad regel skippas tyst (samma semantik som thing.updated: debug, ack).
func TestThingValuesHandlerSkipsWithoutRule(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingValuesTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	body := `{"thingId":"x","tenant":"t","thingType":"room","values":[]}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Brokerfel propageras för retry.
func TestThingValuesHandlerReturnsBrokerError(t *testing.T) {
	is := is.New(t)

	handler := NewThingValuesTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			return nil, errBrokerDown{}
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{{
			Match:    transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Values, Type: "room"},
			Entities: []transform.Entity{{ID: "urn:ngsi-ld:Thing:{{nameOrID}}", Type: "Thing"}},
		}})},
		"",
	)
	body := `{"thingId":"x","tenant":"t","thingType":"room","values":[]}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.True(handler(context.Background(), itm, slog.Default()) != nil)
}
