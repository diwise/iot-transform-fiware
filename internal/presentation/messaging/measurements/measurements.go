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

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-core/pkg/messaging/events"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	apptransform "github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	diwisepkg "github.com/diwise/senml/diwise"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// NewMeasurementTopicMessageHandler använder den statiska motorn.
func NewMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler {
	return newMeasurementTopicMessageHandler(cbClientFn, nil)
}

// NewDynamicMeasurementTopicMessageHandler använder den konfigurationsstyrda
// motorn.
func NewDynamicMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, dynamic *appmeasurements.DynamicTransformer) messaging.TopicMessageHandler {
	return newMeasurementTopicMessageHandler(cbClientFn, dynamic)
}

func newMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, dynamic *appmeasurements.DynamicTransformer) messaging.TopicMessageHandler {

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

		var errs []error

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

			olog := log.With(slog.String("device_id", deviceID), slog.String("tenant", tenant))
			octx := logging.NewContextWithLogger(ctx, olog)

			cbClient, err := cbClientFn(tenant)
			if err != nil {
				olog.Error("failed to create context broker client", "err", err.Error())
				errs = append(errs, err)
				continue
			}

			// Brokerfel propageras så att rapporten kan återlevereras.
			// NGSI-LD merge/create är idempotent, så en retry konvergerar.
			// Meddelanden utan relevanta egenskaper ackas (skip).
			err = transformObservation(octx, dynamic, observation, cbClient)
			if err != nil {
				if errors.Is(err, appmeasurements.ErrNoRelevantProperties) {
					olog.Debug("message did not contain any relevant properties")
					continue
				}

				if errors.Is(err, apptransform.ErrNoMatchingRule) {
					olog.Debug("no matching rule for measurement")
					continue
				}

				olog.Error("transform failed", "err", err.Error())
				errs = append(errs, err)
				continue
			}

			transformedCounter.Add(ctx, 1)

			olog.Debug("measurement handled successfully")
		}

		return errors.Join(errs...)
	}
}

// transformObservation väljer dynamisk motor när en sådan är konfigurerad,
// annars den statiska transformers-tabellen.
func transformObservation(ctx context.Context, dynamic *appmeasurements.DynamicTransformer, observation events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	if dynamic != nil {
		writes, err := dynamic.Transform(ctx, observation)
		if err != nil {
			return err
		}

		for _, w := range writes {
			if w.Create {
				err := contextbroker.CreateNewEntity(ctx, cbClient, w.EntityID, w.TypeName, w.Props)
				if err != nil && !errors.Is(err, contextbroker.ErrEntityAlreadyExists) {
					return err
				}
				continue
			}

			if err := contextbroker.MergeOrCreateWithContext(ctx, cbClient, w.EntityID, w.TypeName, w.Context, w.Props); err != nil {
				return err
			}
		}

		return nil
	}

	measurementType := getMeasurementType(observation)
	if measurementType == "" {
		return appmeasurements.ErrNoRelevantProperties
	}

	transformer := appmeasurements.TransformerFor(measurementType)
	if transformer == nil {
		return appmeasurements.ErrNoRelevantProperties
	}

	return transformer(ctx, observation, cbClient, contextbroker.EntityWriter{})
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
