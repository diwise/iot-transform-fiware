package measurements

import (
	"errors"
	"log/slog"
	"testing"

	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func TestStage5SkipAndTransformErrorsDoNotPrepareBroker(t *testing.T) {
	for _, kind := range []string{"no-match", "required", "optional", "transform-error"} {
		t.Run(kind, func(t *testing.T) {
			rule := dynamicRule()[0]
			switch kind {
			case "no-match":
				rule.Match.Device = "different"
			case "required":
				rule.Entities[0].Properties[0].Resource = "9999"
				rule.Entities[0].Properties[0].Required = true
			case "optional":
				rule.Entities[0].Properties[0].Resource = "9999"
			case "transform-error":
				rule.Entities[0].Properties[0].Transform = []transform.Transform{{Op: transform.OpBoolToOnOff}}
			}
			if err := transform.Validate([]transform.Rule{rule}); err != nil {
				t.Fatal(err)
			}
			calls := 0
			factory := func(string) (contextbroker.BrokerClient, error) {
				calls++
				return nil, errors.New("broker/token unavailable")
			}
			handler := NewDynamicMeasurementTopicMessageHandler(factory, staticMeasurementsProvider{appmeasurements.NewDynamicTransformer([]transform.Rule{rule}, nil)}, "")
			msg := &messaging.IncomingTopicMessageMock{
				BodyFunc: func() []byte {
					return []byte(`{"pack":[{"bn":"sensor/3303/","bt":1790856000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":12},{"bn":"sensor/","n":"tenant","vs":"tenant-a"}],"timestamp":"2026-10-01T12:00:00Z"}`)
				},
				ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" },
			}
			err := handler(t.Context(), msg, slog.Default())
			if calls != 0 {
				t.Fatalf("prepared broker before result: %d calls (%v)", calls, err)
			}
			if (err != nil) != (kind == "transform-error") {
				t.Fatalf("unexpected outcome: %v", err)
			}
		})
	}
}
