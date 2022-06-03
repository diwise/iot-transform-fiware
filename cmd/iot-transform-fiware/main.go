package main

import (
	"context"
	"net/http"
	"os"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/iottransformfiware"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/go-chi/chi/v5"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/rs/cors"
	"github.com/rs/zerolog"
)

const serviceName string = "iot-transform-fiware"

func main() {
	serviceVersion := buildinfo.SourceVersion()

	_, logger, cleanup := o11y.Init(context.Background(), serviceName, serviceVersion)
	defer cleanup()

	logger.Info().Msg("starting up ...")

	app := SetupIoTTransformFiware(logger)

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)

	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	routingKey := "message.accepted"
	messenger.RegisterTopicMessageHandler(routingKey, newTopicMessageHandler(messenger, app))

	setupRouterAndWaitForConnections(logger)
}

func newTopicMessageHandler(messenger messaging.MsgContext, app iottransformfiware.IoTTransformFiware) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {

		ctx = logging.NewContextWithLogger(ctx, logger)
		logger.Info().Str("body", string(msg.Body)).Msg("received message")

		err := app.MessageAccepted(ctx, msg.Body)
		if err != nil {
			logger.Error().Err(err).Msg("failed to handle accepted message")
		}
	}
}

func SetupIoTTransformFiware(logger zerolog.Logger) iottransformfiware.IoTTransformFiware {
	contextBrokerUrl := os.Getenv("NGSI_CB_URL")
	c := client.NewContextBrokerClient(contextBrokerUrl)
	m := messageprocessor.NewMessageProcessor(c)

	return iottransformfiware.NewIoTTransformFiware(m, logger)
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
