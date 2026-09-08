package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/diwise/iot-core/pkg/messaging/events"
	appmeasurements "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
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

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, log *slog.Logger) {
		messageAccepted := events.MessageAccepted{}

		log = log.With(slog.String("content_type", msg.ContentType()))

		err := json.Unmarshal(msg.Body(), &messageAccepted)
		if err != nil {
			log.Error("unable to unmarshal incoming message", "err", err.Error())
			return
		}

		totalCounter.Add(ctx, 1)

		measurementType := getMeasurementType(messageAccepted)
		if measurementType == "" {
			log.Debug("unable to determine measurement type from message, skipping")
			return
		}

		transformer := appmeasurements.TransformerFor(measurementType)
		if transformer == nil {
			return
		}

		deviceID := messageAccepted.DeviceID()
		if deviceID == "" {
			log.Debug("device id is missing in message, skipping")
			return
		}

		tenant := messageAccepted.Tenant()
		if tenant == "" {
			log.Debug("tenant is missing in message, skipping")
			return
		}

		log = log.With(slog.String("device_id", deviceID), slog.String("tenant", tenant), slog.String("measurement_type", measurementType))
		ctx = logging.NewContextWithLogger(ctx, log)

		cbClient, err := cbClientFn(tenant)
		if err != nil {
			log.Error("failed to create context broker client", "err", err.Error())
			return
		}

		err = transformer(ctx, messageAccepted, cbClient)
		if err != nil {
			if errors.Is(err, appmeasurements.ErrNoRelevantProperties) {
				log.Debug("message did not contain any relevant properties")
				return
			}

			log.Error("transform failed", "err", err.Error())

			return
		}

		transformedCounter.Add(ctx, 1)

		log.Debug("measurement handled successfully")
	}
}

func getMeasurementType(m events.MessageAccepted) string {
	urn, ok := m.Pack().GetStringValue(senml.FindByName("0"))
	if !ok {
		return ""
	}

	env, ok := m.Pack().GetStringValue(senml.FindByName("env"))
	if ok {
		urn = fmt.Sprintf("%s/%s", urn, env)
	}

	return urn
}
