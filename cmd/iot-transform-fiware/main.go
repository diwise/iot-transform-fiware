package main

import (
	"context"
	"encoding/json"
	"net/http"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/features"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"

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
	messageProcessor := messageprocessor.NewMessageProcessor()

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)

	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	messenger.RegisterTopicMessageHandler("message.accepted", newTopicMessageHandler(messenger, messageProcessor))
	messenger.RegisterTopicMessageHandler("feature.updated", features.TopicMessageHandler(messenger, contextBrokerUrl))

	setupRouterAndWaitForConnections(logger)
}

func newTopicMessageHandler(messenger messaging.MsgContext, app messageprocessor.MessageProcessor) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		ctx = logging.NewContextWithLogger(ctx, logger)
		logger.Info().Str("body", string(msg.Body)).Msg("received message")

		messageAccepted := iotcore.MessageAccepted{}

		if err := json.Unmarshal(msg.Body, &messageAccepted); err == nil {
			contextBrokerClient := client.NewContextBrokerClient(contextBrokerUrl, client.Tenant(messageAccepted.Tenant()))

			if err := app.ProcessMessage(ctx, messageAccepted, contextBrokerClient); err != nil {
				logger.Error().Err(err).Msg("failed to handle accepted message")
			}
		} else {
			logger.Error().Err(err).Msg("unable to unmarshal incoming message")
		}
	}
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
