package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/diwise/messaging-golang/pkg/messaging"

	"github.com/diwise/service-chassis/pkg/infrastructure/buildinfo"
	"github.com/diwise/service-chassis/pkg/infrastructure/env"
	k8shandlers "github.com/diwise/service-chassis/pkg/infrastructure/net/http/handlers"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

const serviceName string = "iot-transform-fiware"

func defaultFlags() FlagMap {
	return FlagMap{
		listenAddress:    "0.0.0.0",
		servicePort:      "8080",
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

	factory := newContextBrokerClientFactory(ctx, flags[contextbrokerUrl], serviceName, serviceVersion, flags[oauth2ClientId], flags[oauth2ClientSecret], flags[oauth2TokenUrl], flags[oauth2InsecureURL] == "true")

	cfg := &AppConfig{
		messenger:  messenger,
		cbClientFn: factory,
	}

	runner, err := initialize(ctx, flags, cfg)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags FlagMap, cfg *AppConfig) (servicerunner.Runner[AppConfig], error) {
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
		onstarting(func(ctx context.Context, svcCfg *AppConfig) error {
			svcCfg.messenger.Start()

			return registerHandlers(svcCfg.messenger, svcCfg.cbClientFn)
		}),
		onshutdown(func(ctx context.Context, svcCfg *AppConfig) error {
			svcCfg.messenger.Close()
			return nil
		}))

	return runner, nil
}

func registerHandlers(messenger messaging.MsgContext, cbClientFn ContextBrokerClientFactoryFunc) error {
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
		handler func(messaging.MsgContext, func(string) client.ContextBrokerClient) messaging.TopicMessageHandler
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
		if err := messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, h.handler(messenger, cbClientFn), h.filter); err != nil {
			return fmt.Errorf("failed to register %s handler: %w", h.name, err)
		}
	}

	// measurements
	if err := messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewMeasurementTopicMessageHandler(messenger, cbClientFn)); err != nil {
		return fmt.Errorf("failed to register measurements handler: %w", err)
	}

	return nil
}

func parseExternalConfig(ctx context.Context, flags FlagMap) (context.Context, FlagMap) {
	// Allow environment variables to override certain defaults
	envOrDef := env.GetVariableOrDefault

	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
	flags[contextbrokerUrl] = envOrDef(ctx, "NGSI_CB_URL", flags[contextbrokerUrl])
	flags[oauth2TokenUrl] = envOrDef(ctx, "OAUTH2_TOKEN_URL", flags[oauth2TokenUrl])
	flags[oauth2ClientId] = envOrDef(ctx, "OAUTH2_CLIENT_ID", flags[oauth2ClientId])
	flags[oauth2ClientSecret] = envOrDef(ctx, "OAUTH2_CLIENT_SECRET", flags[oauth2ClientSecret])
	flags[oauth2InsecureURL] = envOrDef(ctx, "OAUTH2_REALM_INSECURE", flags[oauth2InsecureURL])
	flags[logLevel] = envOrDef(ctx, "LOG_LEVEL", flags[logLevel])

	apply := func(f FlagType) func(string) error {
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

type ContextBrokerClientFactoryFunc func(string) client.ContextBrokerClient

func newContextBrokerClientFactory(ctx context.Context, contextBrokerUrl, serviceName, serviceVersion, oauth2ClientId, oauth2ClientSecret, oauth2TokenUrl string, oauthInsecureURL bool) ContextBrokerClientFactoryFunc {
	log := logging.GetFromContext(ctx)

	var tokenSource oauth2.TokenSource

	if oauth2ClientId != "" && oauth2ClientSecret != "" && oauth2TokenUrl != "" {
		oauthConfig := &clientcredentials.Config{
			ClientID:     oauth2ClientId,
			ClientSecret: oauth2ClientSecret,
			TokenURL:     oauth2TokenUrl,
		}

		httpTransport := http.DefaultTransport
		if oauthInsecureURL {
			trans, ok := httpTransport.(*http.Transport)
			if ok {
				if trans.TLSClientConfig == nil {
					trans.TLSClientConfig = &tls.Config{}
				}
				trans.TLSClientConfig.InsecureSkipVerify = true
			}
		}

		httpClient := &http.Client{
			Transport: otelhttp.NewTransport(httpTransport),
		}

		ctx = context.WithValue(ctx, oauth2.HTTPClient, httpClient)

		tokenSource = oauthConfig.TokenSource(ctx)
	}

	return func(tenant string) client.ContextBrokerClient {
		if tokenSource != nil {
			token, err := tokenSource.Token()
			if err != nil {
				log.Error("failed to retrieve oauth2 token, continuing without authorization header", "err", err.Error())
			} else {
				return client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
					client.RequestHeader("Authorization", []string{fmt.Sprintf("%s %s", token.TokenType, token.AccessToken)}),
				)
			}
		}

		return client.NewContextBrokerClient(
			contextBrokerUrl,
			client.Tenant(tenant),
			client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
		)
	}
}
