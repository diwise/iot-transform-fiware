package measurements

import (
	"context"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/matryer/is"
)

type captureClient struct {
	client.ContextBrokerClient
	calls []string
}

func (c *captureClient) CreateEntity(_ context.Context, e types.Entity, _ map[string][]string) (*ngsild.CreateEntityResult, error) {
	c.calls = append(c.calls, "create:"+e.ID())
	return &ngsild.CreateEntityResult{}, nil
}

func (c *captureClient) MergeEntity(_ context.Context, id string, _ types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
	c.calls = append(c.calls, "merge:"+id)
	return &ngsild.MergeEntityResult{}, nil
}

// Flera entiteter skrivs i listordning och create hanteras separat.
func TestDynamicWritesInOrderAndHandlesCreate(t *testing.T) {
	is := is.New(t)

	rules := []transform.Rule{{
		Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []transform.Entity{
			{ID: "urn:ngsi-ld:First:x", Type: "First", Create: true},
			{ID: "urn:ngsi-ld:Second:x", Type: "Second"},
		},
	}}

	dynamic := appmeasurements.NewDynamicTransformer(rules, nil)
	cb := &captureClient{}

	err := transformObservation(context.Background(), dynamic, dynamicObservation(time.Now().UTC()), cb, "")
	is.NoErr(err)
	is.Equal(cb.calls, []string{"create:urn:ngsi-ld:First:x", "merge:urn:ngsi-ld:Second:x"})
}
