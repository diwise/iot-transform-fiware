package main

import (
	"context"
	"io/ioutil"
	"runtime/debug"
	"strings"

	"github.com/diwise/iot-transform-fiware/internal/pkg/infrastructure/tracing"
	"github.com/diwise/messaging-golang/pkg/messaging"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	serviceName := "iot-transform-fiware"
	serviceVersion := version()

	logger := log.With().Str("service", strings.ToLower(serviceName)).Str("version", serviceVersion).Logger()
	logger.Info().Msg("starting up ...")

	ctx := context.Background()

	cleanup, err := tracing.Init(ctx, logger, serviceName, serviceVersion)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to init tracing")
	}
	defer cleanup()

	config := messaging.LoadConfiguration(serviceName, logger)
	messenger, err := messaging.Initialize(config)

	if (err != nil){
		logger.Fatal().Err(err).Msg("failed to init messenger")
	}

	routingKey := "msg.recieved"
	messenger.RegisterTopicMessageHandler(routingKey, topicMessageHandler)
}

func topicMessageHandler(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
	logger.Info().Str("body", string(msg.Body)).Msgf("received message")

	//TODO: gör något bra med meddelandet.

	msg.Ack(true)
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
