package application

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/registry"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func NewMeasurementTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger *slog.Logger) {
		messageAccepted := iotCore.MessageAccepted{}

		err := json.Unmarshal(msg.Body, &messageAccepted)
		if err != nil {
			logger.Error("unable to unmarshal incoming message", "err", err.Error())
			return
		}

		measurementType := measurements.GetMeasurementType(messageAccepted)

		logger = logger.With(
			slog.String("measurement_type", measurementType),
			slog.String("device_id", messageAccepted.Sensor),
		)
		ctx = logging.NewContextWithLogger(ctx, logger)

		transformer := transformerRegistry.GetTransformerForMeasurement(ctx, measurementType)
		if transformer == nil {
			logger.Error("transformer not found")
			return
		}

		logger.Debug("handling message")

		cbClient := getClientForTenant(messageAccepted.Tenant())
		err = transformer(ctx, messageAccepted, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func NewFunctionUpdatedTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger *slog.Logger) {
		fn := functions.Func{}

		err := json.Unmarshal(msg.Body, &fn)
		if err != nil {
			logger.Error("failed to unmarshal message body", "err", err.Error())
			return
		}

		logger = logger.With(
			slog.String("function_type", fmt.Sprintf("%s:%s", fn.Type, fn.SubType)),
			slog.String("function_id", fn.ID),
		)
		ctx = logging.NewContextWithLogger(ctx, logger)

		//TODO: should this come from the json body?
		fn.Timestamp = time.Now().UTC()

		transformer := transformerRegistry.GetTransformerForFunction(ctx, fn.Type)
		if transformer == nil {
			logger.Error("transformer not found")
			return
		}

		logger.Debug("handling message")

		cbClient := getClientForTenant(fn.Tenant)
		err = transformer(ctx, fn, cbClient)
		if err != nil {
			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}
