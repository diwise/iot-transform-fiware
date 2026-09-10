package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func NewMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {

	log := logging.GetFromContext(context.Background())

	totalCounter, err := otel.Meter("iot-transform-fiware/measurements").Int64Counter(
		"diwise.transform.measurements.total",
		metric.WithUnit("1"),
		metric.WithDescription("Total number of received measurements"),
	)

	if err != nil {
		log.Error("failed to create otel total measurements counter", "err", err.Error())
	}

	transformedCounter, err := otel.Meter("iot-transform-fiware/measurements").Int64Counter(
		"diwise.transform.measurements.transformed",
		metric.WithUnit("1"),
		metric.WithDescription("Total number of successfully transformed measurements"),
	)

	if err != nil {
		log.Error("failed to create otel transformed measurements counter", "err", err.Error())
	}

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, log *slog.Logger) error {
		messageAccepted := events.MessageAccepted{}

		log = log.With(slog.String("content_type", msg.ContentType()))

		err := json.Unmarshal(msg.Body(), &messageAccepted)
		if err != nil {
			log.Error("unable to unmarshal incoming message", "err", err.Error())
			return messaging.Permanent(err)
		}

		// Referenstid för relativa SenML-tider: mottagningstid.
		parsed, err := diwisepkg.Parse(messageAccepted.Pack(), time.Now().UTC())
		if err != nil {
			log.Error("unable to parse incoming pack", "err", err.Error())
			return messaging.Permanent(err)
		}

		// Varje objektobservation transformeras för sig med sin egen
		// typ: packet partitioneras i enobservations-meddelanden med
		// delad packmetadata så att befintliga transformers fungerar
		// oförändrade och resurser aldrig läses från fel objekt.
		for _, o := range parsed.Objects() {
			totalCounter.Add(ctx, 1)

			scoped := scopePack(parsed, o)
			observation := events.MessageAccepted{
				Pack_:     scoped,
				Timestamp: messageAccepted.Timestamp,
			}

			measurementType := getMeasurementType(observation)
			if measurementType == "" {
				log.Debug("unable to determine measurement type from message, skipping")
				continue
			}

			transformer := appmeasurements.TransformerFor(measurementType)
			if transformer == nil {
				log.Debug("no transformer for measurement type, skipping", "measurement_type", measurementType)
				continue
			}

			deviceID := observation.DeviceID()
			if deviceID == "" {
				log.Debug("device id is missing in message, skipping")
				continue
			}

			tenant := observation.Tenant()
			if tenant == "" {
				log.Debug("tenant is missing in message, skipping")
				continue
			}

			olog := log.With(slog.String("device_id", deviceID), slog.String("tenant", tenant), slog.String("measurement_type", measurementType))
			octx := logging.NewContextWithLogger(ctx, olog)

			cbClient, err := cbClientFn(tenant)
			if err != nil {
				olog.Error("failed to create context broker client", "err", err.Error())
				continue
			}

			// Bevarad semantik: transformeringsfel loggas och ackas.
			// Klassificering till Temporary/Permanent kräver verifierad
			// idempotens mot context broker.
			err = transformer(octx, observation, cbClient, contextbroker.EntityWriter{})
			if err != nil {
				if errors.Is(err, appmeasurements.ErrNoRelevantProperties) {
					olog.Debug("message did not contain any relevant properties")
					continue
				}

				olog.Error("transform failed", "err", err.Error())

				continue
			}

			transformedCounter.Add(ctx, 1)

			olog.Debug("measurement handled successfully")
		}
		return nil
	}
}

// findHeader matchar objektheaders på kort eller fullständigt namn.
// Headers bär objektets URN.
func findHeader() senml.RecordFinder {
	return func(r senml.Record) bool {
		if r.Name != "0" && !strings.HasSuffix(r.Name, "/0") {
			return false
		}
		return strings.HasPrefix(r.StringValue, "urn:oma:lwm2m:")
	}
}

// findShortName matchar poster på kort eller fullständigt namn.
func findShortName(n string) senml.RecordFinder {
	return func(r senml.Record) bool {
		return r.Name == n || strings.HasSuffix(r.Name, "/"+n)
	}
}

// scopePack partitionerar en observation ur ett tolkat pack: headern, dess
// resurser samt packnivå- och observations egen metadata. Resultatet är ett
// fristående enobservationspack som befintliga transformers läser oförändrat.
func scopePack(parsed *diwisepkg.Pack, o diwisepkg.Object) senml.Pack {
	devicePrefix := parsed.DeviceID() + "/"

	var scoped senml.Pack
	scoped = append(scoped, o.Header())
	scoped = append(scoped, o.Resources()...)

	for _, r := range parsed.Pack() {
		name := r.Name
		idx := strings.LastIndex(name, "/")
		if idx < 0 {
			continue
		}
		trailing, prefix := name[idx+1:], name[:idx+1]
		if trailing == "" || trailing == "0" {
			continue
		}
		if _, err := strconv.Atoi(trailing); err == nil {
			continue
		}
		if prefix != devicePrefix && prefix != o.Prefix() {
			continue
		}
		scoped = append(scoped, r)
	}

	return scoped
}

func getMeasurementType(m events.MessageAccepted) string {
	urn, ok := m.Pack().GetStringValue(findHeader())
	if !ok {
		return ""
	}

	env, ok := m.Pack().GetStringValue(findShortName("env"))
	if ok {
		urn = fmt.Sprintf("%s/%s", urn, env)
	}

	return urn
}
