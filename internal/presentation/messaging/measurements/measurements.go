package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/diwise/iot-core/pkg/messaging/events"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	apptransform "github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	delivery "github.com/diwise/iot-transform-fiware/internal/presentation/messaging"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// TransformerProvider levererar aktuell snapshot-motor per meddelande så
// att omladdade regler slår igenom utan omregistrering (TF-05).
type TransformerProvider interface {
	MeasurementsTransformer() *appmeasurements.DynamicTransformer
}

// NewDynamicMeasurementTopicMessageHandler använder den konfigurationsstyrda
// motorn. defaultContext används för entiteter som inte anger en egen context.
func NewDynamicMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {
	return newMeasurementTopicMessageHandler(cbClientFn, provider, defaultContext)
}

func newMeasurementTopicMessageHandler(cbClientFn contextbroker.ContextBrokerClientFactoryFunc, provider TransformerProvider, defaultContext string) messaging.TopicMessageHandler {

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

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, log *slog.Logger) (err error) {
		ctx, log, end := delivery.Begin(ctx, log, "handle-measurements")
		defer func() { end(err) }()
		messageAccepted := events.MessageAccepted{}

		log = log.With(slog.String("content_type", msg.ContentType()))

		err = json.Unmarshal(msg.Body(), &messageAccepted)
		if err != nil {
			log.Error("unable to unmarshal incoming message", "err", err.Error())
			return messaging.Permanent(err)
		}

		// Delad normalisering med preview (TF-06): samma partitionering
		// i enobservations-meddelanden. Referenstid för relativa
		// SenML-tider: mottagningstid.
		observations, err := appmeasurements.PartitionMessage(messageAccepted, time.Now().UTC())
		if err != nil {
			log.Error("unable to parse incoming pack", "err", err.Error())
			return messaging.Permanent(err)
		}

		var allWrites []apptransform.EntityWrite
		var batchTenant string
		writtenObservations := int64(0)

		// Snapshot per meddelande: alla objektobservationer i samma
		// meddelande transformeras med samma regelversion (TF-05).
		dynamic := provider.MeasurementsTransformer()

		// Varje objektobservation transformeras för sig med sin egen
		// typ: packet partitioneras i enobservations-meddelanden med
		// delad packmetadata så att befintliga transformers fungerar
		// oförändrade och resurser aldrig läses från fel objekt.
		for _, observation := range observations {
			totalCounter.Add(ctx, 1)

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
			writes, err := dynamic.Transform(octx, observation)
			if err != nil {
				if errors.Is(err, apptransform.ErrNoMatchingRule) {
					olog.Debug("no matching rule for measurement")
					continue
				}
				olog.Error("transform failed", "err", err.Error())
				return delivery.DataError(err)
			}
			if len(writes) == 0 {
				olog.Debug("measurement has no writable attributes")
				continue
			}
			if err := apptransform.ValidateEntityWrites(writes, defaultContext); err != nil {
				return delivery.DependencyError(err)
			}
			if batchTenant != "" && batchTenant != tenant {
				return delivery.DataError(errors.New("measurement batch contains multiple tenants"))
			}
			batchTenant = tenant
			allWrites = append(allWrites, writes...)
			writtenObservations++
		}
		if len(allWrites) == 0 {
			return nil
		}
		ctx = logging.NewContextWithLogger(ctx, log.With("tenant", batchTenant, "device_id", messageAccepted.DeviceID()))
		cbClient, err := cbClientFn(batchTenant)
		if err != nil {
			return delivery.DependencyError(err)
		}
		if err := contextbroker.ApplyEntityWrites(ctx, cbClient, allWrites, defaultContext); err != nil {
			return delivery.DependencyError(err)
		}
		transformedCounter.Add(ctx, writtenObservations)
		return nil
	}
}

// transformObservation kör den konfigurationsstyrda motorn och skriver
// entiteterna mot brokern med delad skrivsemantik (create/merge/removeAttributes).
func transformObservation(ctx context.Context, dynamic *appmeasurements.DynamicTransformer, observation events.MessageAccepted, cbClient contextbroker.BrokerClient, defaultContext string) error {
	writes, err := dynamic.Transform(ctx, observation)
	if err != nil {
		return err
	}

	return contextbroker.ApplyEntityWrites(ctx, cbClient, writes, defaultContext)
}
