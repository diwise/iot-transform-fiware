package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/features"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/registry"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"

	iotCore "github.com/diwise/iot-core/pkg/messaging/events"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/rs/cors"
	"github.com/rs/zerolog"
)

const serviceName string = "iot-transform-fiware"

var contextBrokerUrl string

func main() {
	serviceVersion := buildinfo.SourceVersion()

	_, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	contextBrokerUrl = env.GetVariableOrDie(logger, "NGSI_CB_URL", "URL to ngsi-ld context broker")

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)

	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	messenger.RegisterTopicMessageHandler("message.accepted", NewMeasurementTopicMessageHandler(messenger, contextBrokerUrl))
	messenger.RegisterTopicMessageHandler("feature.updated", NewFeatureTopicMessageHandler(messenger, contextBrokerUrl))

	setupRouterAndWaitForConnections(logger)
}

func setupRouterAndWaitForConnections(logger zerolog.Logger) {
	r := chi.NewRouter()
	r.Use(cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		Debug:            false,
	}).Handler)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	err := http.ListenAndServe(":8080", r)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to start router")
	}
}

func NewMeasurementTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		ctx = logging.NewContextWithLogger(ctx, logger)
		logger.Info().Str("body", string(msg.Body)).Msg("received message")

		messageAccepted := iotCore.MessageAccepted{}

		err := json.Unmarshal(msg.Body, &messageAccepted)
		if err != nil {
			logger.Error().Err(err).Msg("unable to unmarshal incoming message")
			return
		}

		contextBrokerClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(messageAccepted.Tenant()))
		measurementType := measurements.GetMeasurementType(messageAccepted)

		transformer := transformerRegistry.GetTransformerForMeasurement(ctx, measurementType)
		if transformer == nil {
			logger.Error().Str("measurement type", measurementType).Msg("transformer not found!")
			return
		}

		err = transformer(ctx, messageAccepted, contextBrokerClient)
		if err != nil {
			logger.Err(err).Msgf("unable to transform type %s", measurementType)
			return
		}
	}
}

func NewFeatureTopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	transformerRegistry := registry.NewTransformerRegistry()

	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		ctx = logging.NewContextWithLogger(ctx, logger)

		feature := features.Feat{}

		err := json.Unmarshal(msg.Body, &feature)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message body")
			return
		}

		//TODO: should this come from the json body?
		feature.Timestamp = msg.Timestamp

		cbClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(feature.Tenant))

		transformer := transformerRegistry.GetTransformerForFeature(ctx, feature.Type)
		if transformer == nil {
			logger.Error().Str("feature type", feature.Type).Msg("transformer not found!")
			return
		}

		err = transformer(ctx, feature, cbClient)
		if err != nil {
			logger.Err(err).Msgf("unable to transform type %s", feature.Type)
			return
		}
	}
}
