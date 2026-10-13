package measurements

import (
	"log/slog"
	"testing"

	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
)

func TestStage6OutputAndFallbackContextRejectedBeforeFactory(t *testing.T) {
	for _, contextURL := range []string{"", "relative-context"} {
		rule := dynamicRule()[0]
		if contextURL == "" {
			rule.Entities[0].ID = "{{deviceID}}"
		}
		if err := transform.Validate([]transform.Rule{rule}); err != nil {
			t.Fatal(err)
		}
		calls := 0
		factory := func(string) (contextbroker.BrokerClient, error) { calls++; return nil, nil }
		handler := NewDynamicMeasurementTopicMessageHandler(factory, staticMeasurementsProvider{appmeasurements.NewDynamicTransformer([]transform.Rule{rule}, nil)}, contextURL)
		msg := &messaging.IncomingTopicMessageMock{BodyFunc: func() []byte {
			return []byte(`{"pack":[{"bn":"sensor/3303/","bt":1790856000,"n":"0","vs":"urn:oma:lwm2m:ext:3303"},{"n":"5700","v":12},{"bn":"sensor/","n":"tenant","vs":"tenant-a"}],"timestamp":"2026-10-01T12:00:00Z"}`)
		}, ContentTypeFunc: func() string { return "application/vnd.oma.lwm2m+json" }}
		if err := handler(t.Context(), msg, slog.Default()); err == nil || calls != 0 {
			t.Fatalf("invalid output reached factory: calls=%d err=%v", calls, err)
		}
	}
}
