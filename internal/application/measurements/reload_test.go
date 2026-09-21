package measurements

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const reloadMeasRules = `
rules:
  - match: { kind: measurement, object: "urn:oma:lwm2m:ext:3303", env: indoors }
    entities:
      - id: "urn:ngsi-ld:IndoorEnvironmentObserved:{{deviceID}}"
        type: IndoorEnvironmentObserved
        properties:
          - { target: temperature, type: Number, resource: "5700", observedAt: header }
          - { target: location, type: Geo, latlon: true, required: true }
          - { target: dateObserved, type: DateTime, timestamp: true, required: true }
`

// Reload byter reglmängd atomärt även för mätvärden (resolvern behålls).
func TestMeasurementsReloadSwapsRuleSet(t *testing.T) {
	is := is.New(t)

	rules, err := config.ParseModel([]byte(reloadMeasRules))
	is.NoErr(err)

	d := NewDynamicTransformer(rules, nil)
	ts := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	msg := events.MessageAccepted{Pack_: indoorPack(ts, "urn:oma:lwm2m:ext:3303", "5700", 22.5), Timestamp: ts}

	writes, err := d.Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	d.Reload(nil)

	_, err = d.Transform(context.Background(), msg)
	if !errors.Is(err, transform.ErrNoMatchingRule) {
		t.Fatalf("empty engine must not match, got %v", err)
	}

	d.Reload(rules)
	writes, err = d.Transform(context.Background(), msg)
	is.NoErr(err)
	is.Equal(len(writes), 1)
}
