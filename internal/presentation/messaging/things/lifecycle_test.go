package things

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	clienttest "github.com/diwise/context-broker/pkg/test"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

const lifecycleDeletedBody = `{
	"eventId":"v1/t/room-a/lifecycle/deleted/4","tenant":"t",
	"thingId":"room-a","kind":"deleted",
	"thingType":"room","thingTypeVersion":"1",
	"configRevision":4,"changedAt":"2026-09-17T12:00:00Z",
	"thing":{"thingId":"room-a","name":"Room A","category":"room"}
}`

func lifecycleDeleteRules() []transform.Rule {
	return []transform.Rule{{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Lifecycle, Lifecycle: string(transform.LifecycleDeleted)},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:IndoorEnvironmentObserved:{{id}}", Type: "IndoorEnvironmentObserved",
			Delete: true,
		}},
	}}
}

// Strukturskada i lifecycle-event är permanent (ingen retry läker den).
func TestThingLifecycleHandlerMalformedIsPermanent(t *testing.T) {
	is := is.New(t)

	handler := NewThingLifecycleTopicMessageHandler(
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
func TestThingLifecycleHandlerSkipsEmptyTenant(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingLifecycleTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	body := `{"thingId":"x","kind":"deleted","tenant":"","thingType":"room"}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Saknad regel skippas tyst (samma semantik som values: debug, ack).
func TestThingLifecycleHandlerSkipsWithoutRule(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingLifecycleTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(lifecycleDeletedBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Brokerfel propageras för retry.
func TestThingLifecycleHandlerReturnsBrokerError(t *testing.T) {
	is := is.New(t)

	handler := NewThingLifecycleTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			return nil, errBrokerDown{}
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer(lifecycleDeleteRules())},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(lifecycleDeletedBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.True(handler(context.Background(), itm, slog.Default()) != nil)
}

// Lyckad deleted-händelse raderar entiteten via DeleteEntity (404 = OK).
func TestThingLifecycleHandlerDeletesEntity(t *testing.T) {
	is := is.New(t)

	var deletedID string
	cbClient := &clienttest.ContextBrokerClientMock{
		DeleteEntityFunc: func(ctx context.Context, entityID string) (*ngsild.DeleteEntityResult, error) {
			deletedID = entityID
			return &ngsild.DeleteEntityResult{}, nil
		},
	}
	handler := NewThingLifecycleTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) { return cbClient, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer(lifecycleDeleteRules())},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(lifecycleDeletedBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.Equal("urn:ngsi-ld:IndoorEnvironmentObserved:room-a", deletedID)
}
