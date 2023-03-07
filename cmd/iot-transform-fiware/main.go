package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/features"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/registry"
	"github.com/diwise/iot-transform-fiware/internal/pkg/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/rs/zerolog"
)

const serviceName string = "iot-transform-fiware"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	ctx, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	contextBrokerUrl := env.GetVariableOrDie(logger, "NGSI_CB_URL", "URL to ngsi-ld context broker")
	messenger := createMessagingContextOrDie(ctx, logger)

	api_ := initialize(ctx, messenger, contextBrokerUrl)

	servicePort := env.GetVariableOrDefault(logger, "SERVICE_PORT", "8080")
	err := http.ListenAndServe(":"+servicePort, api_.Router())
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start request router")
	}
}

func initialize(ctx context.Context, messenger messaging.MsgContext, contextBrokerUrl string) api.API {
	messenger.RegisterTopicMessageHandler("message.accepted", newMeasurementTopicMessageHandler(messenger, contextBrokerUrl))
	messenger.RegisterTopicMessageHandler("feature.updated", newFeatureTopicMessageHandler(messenger, contextBrokerUrl))

	return api.New()
}

func createMessagingContextOrDie(ctx context.Context, logger zerolog.Logger) messaging.MsgContext {
	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messaging")
	}

	return messenger
}

func newMeasurementTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
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

func newFeatureTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
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
