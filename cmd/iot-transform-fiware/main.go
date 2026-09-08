package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/presentation/messaging/measurements"
	"github.com/diwise/iot-transform-fiware/internal/presentation/messaging/things"

	"github.com/diwise/messaging-golang/pkg/messaging"

	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

const serviceName string = "iot-transform-fiware"

func defaultFlags() flagMap {
	return flagMap{
		listenAddress:    "0.0.0.0",
		controlPort:      "8000",
		contextbrokerUrl: "http://context-broker",

		oauth2ClientId:     "",
		oauth2ClientSecret: "",
		oauth2TokenUrl:     "",
		oauth2InsecureURL:  "true",

		logLevel: "debug",
	}
}

const (
	ThingUpdatedTopic    string = "thing.updated"
	FunctionUpdatedTopic string = "function.updated"
	MessageAcceptedTopic string = "message.accepted"
)

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	logging.SetLogLevel(parseLogLevel(flags[logLevel]))

	messenger, err := messaging.Initialize(
		ctx, messaging.LoadConfiguration(ctx, serviceName, logger),
	)
	exitIf(err, logger, "failed to init messenger")

	factory := contextbroker.NewContextBrokerClientFactory(ctx, flags[contextbrokerUrl], serviceName, serviceVersion, flags[oauth2ClientId], flags[oauth2ClientSecret], flags[oauth2TokenUrl], flags[oauth2InsecureURL] == "true")

	cfg := &appConfig{
		messenger:  messenger,
		cbClientFn: factory,
	}

	runner, err := initialize(ctx, flags, cfg)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig) (servicerunner.Runner[appConfig], error) {
	if flags[contextbrokerUrl] == "" {
		return nil, fmt.Errorf("context broker URL is empty")
	}

	probes := map[string]k8shandlers.ServiceProber{
		"rabbitmq": func(context.Context) (string, error) { return "ok", nil },
	}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		onstarting(func(ctx context.Context, svcCfg *appConfig) error {
			svcCfg.messenger.Start()

			return registerHandlers(svcCfg.messenger, svcCfg.cbClientFn)
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			svcCfg.messenger.Close()
			return nil
		}))

	return runner, nil
}

func registerHandlers(messenger messaging.MsgContext, cbClientFn contextbroker.ContextBrokerClientFactoryFunc) error {
	var (
		building        = messaging.MatchContentType("application/vnd.diwise.building+json")
		container       = messaging.MatchContentType("application/vnd.diwise.container+json")
		lifebuoy        = messaging.MatchContentType("application/vnd.diwise.lifebuoy+json")
		passage         = messaging.MatchContentType("application/vnd.diwise.passage+json")
		pointofinterest = messaging.MatchContentType("application/vnd.diwise.pointofinterest+json")
		pumpingstation  = messaging.MatchContentType("application/vnd.diwise.pumpingstation+json")
		room            = messaging.MatchContentType("application/vnd.diwise.room+json")
		sewer           = messaging.MatchContentType("application/vnd.diwise.sewer+json")
		//watermeter      = messaging.MatchContentType("application/vnd.diwise.watermeter+json")
		desk = messaging.MatchContentType("application/vnd.diwise.desk+json")
	)

	// things
	thingHandlers := []struct {
		name    string
		handler func(contextbroker.ContextBrokerClientFactoryFunc) messaging.TopicMessageHandler
		filter  messaging.MessageFilter
	}{
		{"building", things.NewBuildingTopicMessageHandler, building},
		{"container", things.NewContainerTopicMessageHandler, container},
		{"lifebuoy", things.NewLifebuoyTopicMessageHandler, lifebuoy},
		{"passage", things.NewPassageTopicMessageHandler, passage},
		{"pointofinterest", things.NewPointOfInterestTopicMessageHandler, pointofinterest},
		{"pumpingstation", things.NewPumpingstationTopicMessageHandler, pumpingstation},
		{"room", things.NewRoomTopicMessageHandler, room},
		{"sewer", things.NewSewerTopicMessageHandler, sewer},
		{"desk", things.NewDeskTopicMessageHandler, desk},
	}

	for _, h := range thingHandlers {
		if err := messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, h.handler(cbClientFn), h.filter); err != nil {
			return fmt.Errorf("failed to register %s handler: %w", h.name, err)
		}
	}

	// measurements
	if err := messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewMeasurementTopicMessageHandler(cbClientFn)); err != nil {
		return fmt.Errorf("failed to register measurements handler: %w", err)
	}

	return nil
}

func parseExternalConfig(ctx context.Context, flags flagMap) (context.Context, flagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault

	flags[listenAddress] = envOrDef(ctx, "LISTEN_ADDRESS", flags[listenAddress])
	flags[controlPort] = envOrDef(ctx, "CONTROL_PORT", flags[controlPort])
	flags[contextbrokerUrl] = envOrDef(ctx, "NGSI_CB_URL", flags[contextbrokerUrl])
	flags[oauth2TokenUrl] = envOrDef(ctx, "OAUTH2_TOKEN_URL", flags[oauth2TokenUrl])
	flags[oauth2ClientId] = envOrDef(ctx, "OAUTH2_CLIENT_ID", flags[oauth2ClientId])
	flags[oauth2ClientSecret] = envOrDef(ctx, "OAUTH2_CLIENT_SECRET", flags[oauth2ClientSecret])
	flags[oauth2InsecureURL] = envOrDef(ctx, "OAUTH2_REALM_INSECURE", flags[oauth2InsecureURL])
	flags[logLevel] = envOrDef(ctx, "LOG_LEVEL", flags[logLevel])

	apply := func(f flagType) func(string) error {
		return func(value string) error {
			flags[f] = value
			return nil
		}
	}

	flag.Func("loglevel", "set the log level", apply(logLevel))
	flag.Parse()

	return ctx, flags
}

func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelDebug
	}
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		os.Exit(1)
	}
}
