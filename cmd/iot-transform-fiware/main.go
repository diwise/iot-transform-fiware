package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/things"

	"github.com/diwise/messaging-golang/pkg/messaging"

	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

const serviceName string = "iot-transform-fiware"

func defaultFlags() FlagMap {
	return FlagMap{
		listenAddress:    "0.0.0.0",
		servicePort:      "8080",
		controlPort:      "8000",
		contextbrokerUrl: "http://context-broker",
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

	messenger, err := messaging.Initialize(
		ctx, messaging.LoadConfiguration(ctx, serviceName, logger),
	)
	exitIf(err, logger, "failed to init messenger")

	factory := newContextBrokerClientFactory(flags[contextbrokerUrl], serviceName, serviceVersion)

	cfg := &AppConfig{
		messenger:  messenger,
		cbClientFn: factory,
	}

	runner, _ := initialize(ctx, flags, cfg)

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags FlagMap, cfg *AppConfig) (servicerunner.Runner[AppConfig], error) {

	var (
		building        = messaging.MatchContentType("application/vnd.diwise.building+json")
		container       = messaging.MatchContentType("application/vnd.diwise.container+json")
		lifebuoy        = messaging.MatchContentType("application/vnd.diwise.lifebuoy+json")
		passage         = messaging.MatchContentType("application/vnd.diwise.passage+json")
		pointofinterest = messaging.MatchContentType("application/vnd.diwise.pointofinterest+json")
		pumpingstation  = messaging.MatchContentType("application/vnd.diwise.pumpingstation+json")
		room            = messaging.MatchContentType("application/vnd.diwise.room+json")
		sewer           = messaging.MatchContentType("application/vnd.diwise.sewer+json")
		watermeter      = messaging.MatchContentType("application/vnd.diwise.watermeter+json")
	)

	probes := map[string]k8shandlers.ServiceProber{
		"rabbitmq": func(context.Context) (string, error) { return "ok", nil },
	}

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		), onstarting(func(ctx context.Context, svcCfg *AppConfig) (err error) {
			return nil
		}),
		onstarting(func(ctx context.Context, ac *AppConfig) error {
			cfg.messenger.Start()

			// things
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewBuildingTopicMessageHandler(cfg.messenger, cfg.cbClientFn), building)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewContainerTopicMessageHandler(cfg.messenger, cfg.cbClientFn), container)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewLifebuoyTopicMessageHandler(cfg.messenger, cfg.cbClientFn), lifebuoy)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPassageTopicMessageHandler(cfg.messenger, cfg.cbClientFn), passage)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPointOfInterestTopicMessageHandler(cfg.messenger, cfg.cbClientFn), pointofinterest)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPumpingstationTopicMessageHandler(cfg.messenger, cfg.cbClientFn), pumpingstation)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewRoomTopicMessageHandler(cfg.messenger, cfg.cbClientFn), room)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewSewerTopicMessageHandler(cfg.messenger, cfg.cbClientFn), sewer)
			cfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewWaterMeterTopicMessageHandler(cfg.messenger, cfg.cbClientFn), watermeter)
			// measurements
			cfg.messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewMeasurementTopicMessageHandler(cfg.messenger, cfg.cbClientFn))

			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *AppConfig) error {
			svcCfg.messenger.Close()
			return nil
		}))

	return runner, nil
}

func parseExternalConfig(ctx context.Context, flags FlagMap) (context.Context, FlagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
	flags[contextbrokerUrl] = envOrDef(ctx, "NGSI_CB_URL", flags[contextbrokerUrl])

	flag.Parse()

	return ctx, flags
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		os.Exit(1)
	}
}

type ContextBrokerClientFactoryFunc func(string) client.ContextBrokerClient

func newContextBrokerClientFactory(contextBrokerUrl, serviceName, serviceVersion string) ContextBrokerClientFactoryFunc {
	type request struct {
		tenant string
		result chan client.ContextBrokerClient
	}

	requestQueue := make(chan request)

	go func() {
		clients := map[string]client.ContextBrokerClient{}

		for r := range requestQueue {
			c, ok := clients[r.tenant]

			if !ok {
				c = client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(r.tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
				)
				clients[r.tenant] = c
			}

			r.result <- c
		}
	}()

	return func(tenant string) client.ContextBrokerClient {
		r := request{tenant: tenant, result: make(chan client.ContextBrokerClient)}
		requestQueue <- r
		return <-r.result
	}
}
