package measurements

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

type contextCaptureClient struct {
	client.ContextBrokerClient
	last []byte
}

func (c *contextCaptureClient) CreateEntity(_ context.Context, e types.Entity, _ map[string][]string) (*ngsild.CreateEntityResult, error) {
	c.last, _ = json.Marshal(e)
	return &ngsild.CreateEntityResult{}, nil
}

func (c *contextCaptureClient) MergeEntity(_ context.Context, _ string, _ types.EntityFragment, _ map[string][]string) (*ngsild.MergeEntityResult, error) {
	return nil, ngsilderrors.ErrNotFound
}

func TestDefaultContextIsApplied(t *testing.T) {
	is := is.New(t)

	rules, err := config.ParseModel([]byte(`
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303" }
    entities:
      - id: "urn:ngsi-ld:WeatherObserved:{{deviceID}}"
        type: WeatherObserved
        properties: [{target: reading, type: Number, resource: "5700"}]
`))
	is.NoErr(err)

	dynamic := appmeasurements.NewDynamicTransformer(rules, nil)
	cc := &contextCaptureClient{}

	const fallback = "https://example.invalid/fallback-context.jsonld"
	is.NoErr(transformObservation(context.Background(), dynamic, dynamicObservation(time.Now().UTC()), cc, fallback))
	is.True(strings.Contains(string(cc.last), fallback))
}

func TestEntityContextOverridesDefault(t *testing.T) {
	is := is.New(t)

	rules, err := config.ParseModel([]byte(`
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303" }
    entities:
      - id: "urn:ngsi-ld:WeatherObserved:{{deviceID}}"
        type: WeatherObserved
        context: "https://example.invalid/entity-context.jsonld"
        properties: [{target: reading, type: Number, resource: "5700"}]
`))
	is.NoErr(err)

	dynamic := appmeasurements.NewDynamicTransformer(rules, nil)
	cc := &contextCaptureClient{}

	is.NoErr(transformObservation(context.Background(), dynamic, dynamicObservation(time.Now().UTC()), cc, "https://example.invalid/fallback-context.jsonld"))
	is.True(strings.Contains(string(cc.last), "https://example.invalid/entity-context.jsonld"))
	is.True(!strings.Contains(string(cc.last), "fallback-context.jsonld"))
}
