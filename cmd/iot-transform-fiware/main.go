package main

import (
	"context"
	"os"
	"runtime/debug"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/iottransformfiware"
	"github.com/diwise/iot-transform-fiware/internal/pkg/infrastructure/tracing"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/rs/zerolog"
)

const serviceName string = "iot-transform-fiware"

func main() {
	serviceVersion := version()

	ctx, logger := logging.NewLogger(context.Background(), serviceName, serviceVersion)
	logger.Info().Msg("starting up ...")

	cleanup, err := tracing.Init(ctx, logger, serviceName, serviceVersion)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init tracing")
	}
	defer cleanup()

	app := SetupIoTTransformFiware(logger)

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)

	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	routingKey := "message.accepted"
	messenger.RegisterTopicMessageHandler(routingKey, newTopicMessageHandler(messenger, app))

	for {
		time.Sleep(1 * time.Second)
	}
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
	c := domain.NewContextBrokerClient(contextBrokerUrl, logger)
	m := messageprocessor.NewMessageProcessor(c)

	return iottransformfiware.NewIoTTransformFiware(m, logger)
}

func version() string {
	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}

	buildSettings := buildInfo.Settings
	infoMap := map[string]string{}
	for _, s := range buildSettings {
		infoMap[s.Key] = s.Value
	}

	sha := infoMap["vcs.revision"]
	if infoMap["vcs.modified"] == "true" {
		sha += "+"
	}

	return sha
}
