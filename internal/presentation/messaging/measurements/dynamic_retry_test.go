package measurements

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/iot-core/pkg/messaging/events"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/senml"
	"github.com/matryer/is"
)

type errClient struct {
	client.ContextBrokerClient
	err error
}

func (c errClient) CreateEntity(context.Context, types.Entity, map[string][]string) (*ngsild.CreateEntityResult, error) {
	return nil, c.err
}

func (c errClient) MergeEntity(context.Context, string, types.EntityFragment, map[string][]string) (*ngsild.MergeEntityResult, error) {
	return nil, c.err
}

func dynamicRule() []transform.Rule {
	return []transform.Rule{{
		Match: transform.Match{Kind: transform.KindMeasurement, Object: "urn:oma:lwm2m:ext:3303"},
		Entities: []transform.Entity{{
			ID: "urn:ngsi-ld:WeatherObserved:{{deviceID}}", Type: "WeatherObserved",
			Properties: []transform.Property{{
				Target: "temperature", Type: transform.TypeNumber, Source: transform.Source{Resource: "5700"},
			}},
		}},
	}}
}

func dynamicObservation(ts time.Time) events.MessageAccepted {
	v := 1.0
	return events.MessageAccepted{
		Pack_: senml.Pack{
			{Name: "dev-1/0", StringValue: "urn:oma:lwm2m:ext:3303", Time: float64(ts.Unix())},
			{Name: "dev-1/5700", Value: &v},
			{Name: "dev-1/tenant", StringValue: "default"},
		},
		Timestamp: ts,
	}
}

// Brokerfel från den dynamiska motorn ska propageras så att rapporten kan
// återlevereras.
func TestDynamicBrokerErrorIsPropagated(t *testing.T) {
	is := is.New(t)

	want := errors.New("broker down")
	dynamic := appmeasurements.NewDynamicTransformer(dynamicRule(), nil)

	err := transformObservation(context.Background(), dynamic, dynamicObservation(time.Now().UTC()), errClient{err: want})
	is.True(errors.Is(err, want))
}

// Ingen matchande regel ska vara en skip (ErrNoMatchingRule), inte retry.
func TestDynamicNoMatchIsSkipped(t *testing.T) {
	is := is.New(t)

	dynamic := appmeasurements.NewDynamicTransformer(dynamicRule(), nil)
	observation := dynamicObservation(time.Now().UTC())
	observation.Pack_[0].StringValue = "urn:oma:lwm2m:ext:9999"

	err := transformObservation(context.Background(), dynamic, observation, errClient{err: errors.New("should not be called")})
	is.True(errors.Is(err, transform.ErrNoMatchingRule))
}
