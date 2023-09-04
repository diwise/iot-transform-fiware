package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/registry"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/rs/zerolog"
)

func NewMeasurementTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		messageAccepted := iotCore.MessageAccepted{}

		err := json.Unmarshal(msg.Body, &messageAccepted)
		if err != nil {
			logger.Error().Err(err).Msg("unable to unmarshal incoming message")
			return
		}

		contextBrokerClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(messageAccepted.Tenant()))
		measurementType := measurements.GetMeasurementType(messageAccepted)

		logger = logger.With().
			Str("measurement_type", measurementType).
			Str("device_id", messageAccepted.Sensor).Logger()
		ctx = logging.NewContextWithLogger(ctx, logger)

		transformer := transformerRegistry.GetTransformerForMeasurement(ctx, measurementType)
		if transformer == nil {
			logger.Debug().Msg("transformer not found")
			return
		}

		logger.Debug().Msg("handling message")

		err = transformer(ctx, messageAccepted, contextBrokerClient)
		if err != nil {
			logger.Err(err).Msgf("transform failed")
			return
		}
	}
}

func NewFunctionUpdatedTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		fn := functions.Func{}

		err := json.Unmarshal(msg.Body, &fn)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message body")
			return
		}

		logger = logger.With().
			Str("function_type", fmt.Sprintf("%s:%s", fn.Type, fn.SubType)).
			Str("function_id", fn.ID).Logger()
		ctx = logging.NewContextWithLogger(ctx, logger)

		//TODO: should this come from the json body?
		fn.Timestamp = time.Now().UTC()

		cbClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(fn.Tenant))

		transformer := transformerRegistry.GetTransformerForFunction(ctx, fn)
		if transformer == nil {
			logger.Debug().Msg("transformer not found")
			return
		}

		logger.Debug().Msg("handling message")

		err = transformer(ctx, fn, cbClient)
		if err != nil {
			logger.Err(err).Msg("transform failed")
			return
		}
	}
}
