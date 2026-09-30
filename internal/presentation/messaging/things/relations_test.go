package things

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	clienttest "github.com/diwise/context-broker/pkg/test"
	appthings "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/matryer/is"
)

const relationSetBody = `{
	"relationId":"partof-tank-1","type":"partOf",
	"fromId":"tank-1","toId":"greenhouse-a","tenant":"t",
	"revision":2,"changedAt":"2026-09-17T12:00:00Z",
	"fromThingType":"tank",
	"from":{"thingId":"tank-1","name":"Tank 1","category":"tank"},
	"toThingType":"greenhouse",
	"to":{"thingId":"greenhouse-a","name":"Växthus A","category":"greenhouse"}
}`

func relationRules() []transform.Rule {
	return []transform.Rule{{
		Match: transform.Match{Kind: transform.KindThing, Event: transform.EventThingsV1Relations, Relation: "partOf", Type: "tank"},
		Entities: []transform.Entity{{
			ID:   "urn:ngsi-ld:AgriGreenhouse:{{relation.to.thingId}}",
			Type: "AgriGreenhouse",
			Properties: []transform.Property{{
				Target: "hasWaterQualityObserved", Type: transform.TypeRelationship,
				Array: true, Source: transform.Source{Value: "urn:ngsi-ld:WaterQualityObserved:{{relation.from.thingId}}"},
				Required: true,
			}},
		}},
	}}
}

// Strukturskada i relationsevent är permanent (ingen retry läker den).
func TestThingRelationsHandlerMalformedIsPermanent(t *testing.T) {
	is := is.New(t)

	handler := NewThingRelationsTopicMessageHandler(
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
func TestThingRelationsHandlerSkipsEmptyTenant(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingRelationsTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	body := `{"relationId":"partof-x","type":"partOf","fromId":"x","tenant":""}`
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(body) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Saknad regel skippas tyst (samma semantik som values: debug, ack).
func TestThingRelationsHandlerSkipsWithoutRule(t *testing.T) {
	is := is.New(t)

	called := false
	handler := NewThingRelationsTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			called = true
			return nil, nil
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer([]transform.Rule{})},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(relationSetBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	is.True(!called)
}

// Brokerfel propageras för retry.
func TestThingRelationsHandlerReturnsBrokerError(t *testing.T) {
	is := is.New(t)

	handler := NewThingRelationsTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) {
			return nil, errBrokerDown{}
		},
		staticThingsProvider{appthings.NewDynamicThingTransformer(relationRules())},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(relationSetBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.True(handler(context.Background(), itm, slog.Default()) != nil)
}

// Lyckad partOf-set skriver hasWaterQualityObserved på AgriGreenhouse
// med tankens WaterQualityObserved-URN (array enligt modellen).
func TestThingRelationsHandlerWritesReference(t *testing.T) {
	is := is.New(t)

	var mergedID string
	var mergedBody []byte
	cbClient := &clienttest.ContextBrokerClientMock{
		MergeEntityFunc: func(ctx context.Context, entityID string, fragment types.EntityFragment, headers map[string][]string) (*ngsild.MergeEntityResult, error) {
			mergedID = entityID
			var err error
			mergedBody, err = fragment.MarshalJSON()
			if err != nil {
				return nil, err
			}
			return &ngsild.MergeEntityResult{}, nil
		},
	}
	handler := NewThingRelationsTopicMessageHandler(
		func(string) (client.ContextBrokerClient, error) { return cbClient, nil },
		staticThingsProvider{appthings.NewDynamicThingTransformer(relationRules())},
		"",
	)
	itm := &messaging.IncomingTopicMessageMock{
		BodyFunc:        func() []byte { return []byte(relationSetBody) },
		ContentTypeFunc: func() string { return "application/vnd.diwise.things.v1+json" },
	}

	is.NoErr(handler(context.Background(), itm, slog.Default()))
	// Båda sidor använder käll-ID, oberoende av sina namn.
	is.Equal("urn:ngsi-ld:AgriGreenhouse:greenhouse-a", mergedID)
	var frag map[string]any
	is.NoErr(json.Unmarshal(mergedBody, &frag))
	rel, ok := frag["hasWaterQualityObserved"].(map[string]any)
	is.True(ok)
	is.Equal("Relationship", rel["type"])
	objs, ok := rel["object"].([]any)
	is.True(ok)
	is.Equal(1, len(objs))
	is.Equal("urn:ngsi-ld:WaterQualityObserved:tank-1", objs[0])
}
