package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/features"
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

		logger = logger.With().Str("measurement_type", measurementType).Logger()
		ctx = logging.NewContextWithLogger(ctx, logger)

		transformer := transformerRegistry.GetTransformerForMeasurement(ctx, measurementType)
		if transformer == nil {
			logger.Error().Msg("transformer not found")
			return
		}

		logger.Debug().Msgf("handle message from %s", messageAccepted.Sensor)

		err = transformer(ctx, messageAccepted, contextBrokerClient)
		if err != nil {
			logger.Err(err).Msgf("transform failed")
			return
		}
	}
}

func NewFeatureTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		feature := features.Feat{}

		err := json.Unmarshal(msg.Body, &feature)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message body")
			return
		}

		logger = logger.With().Str("feature_type", fmt.Sprintf("%s:%s", feature.Type, feature.SubType)).Logger()
		ctx = logging.NewContextWithLogger(ctx, logger)

		//TODO: should this come from the json body?
		feature.Timestamp = time.Now().UTC()

		cbClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(feature.Tenant))

		transformer := transformerRegistry.GetTransformerForFeature(ctx, feature.Type)
		if transformer == nil {
			logger.Error().Msg("transformer not found")
			return
		}

		logger.Debug().Msgf("handle message from %s", feature.ID)

		err = transformer(ctx, feature, cbClient)
		if err != nil {
			logger.Err(err).Msg("transform failed")
			return
		}
	}
}
