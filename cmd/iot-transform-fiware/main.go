package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/things"
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

	factory := newContextBrokerClientFactory(ctx, flags[contextbrokerUrl], serviceName, serviceVersion, flags[oauth2ClientId], flags[oauth2ClientSecret], flags[oauth2TokenUrl], flags[oauth2InsecureURL] == "true")

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
		//watermeter      = messaging.MatchContentType("application/vnd.diwise.watermeter+json")
		desk = messaging.MatchContentType("application/vnd.diwise.desk+json")
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
		onstarting(func(ctx context.Context, svcCfg *AppConfig) error {
			svcCfg.messenger.Start()

			// things
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewBuildingTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), building)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewContainerTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), container)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewLifebuoyTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), lifebuoy)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPassageTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), passage)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPointOfInterestTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), pointofinterest)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewPumpingstationTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), pumpingstation)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewRoomTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), room)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewSewerTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), sewer)
			//svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewWaterMeterTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), watermeter)
			svcCfg.messenger.RegisterTopicMessageHandlerWithFilter(ThingUpdatedTopic, things.NewDeskTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn), desk)
			// measurements
			svcCfg.messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewMeasurementTopicMessageHandler(svcCfg.messenger, svcCfg.cbClientFn))

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
	flags[oauth2TokenUrl] = envOrDef(ctx, "OAUTH2_TOKEN_URL", flags[oauth2TokenUrl])
	flags[oauth2ClientId] = envOrDef(ctx, "OAUTH2_CLIENT_ID", flags[oauth2ClientId])
	flags[oauth2ClientSecret] = envOrDef(ctx, "OAUTH2_CLIENT_SECRET", flags[oauth2ClientSecret])
	flags[oauth2InsecureURL] = envOrDef(ctx, "OAUTH2_REALM_INSECURE", flags[oauth2InsecureURL])

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

func newContextBrokerClientFactory(ctx context.Context, contextBrokerUrl, serviceName, serviceVersion, oauth2ClientId, oauth2ClientSecret, oauth2TokenUrl string, oauthInsecureURL bool) ContextBrokerClientFactoryFunc {
	log := logging.GetFromContext(ctx)

	type request struct {
		tenant string
		result chan client.ContextBrokerClient
	}

	requestQueue := make(chan request)

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

	go func() {
		for r := range requestQueue {
			var c client.ContextBrokerClient

			if tokenSource != nil {
				token, err := tokenSource.Token()
				if err != nil {
					log.Error("failed to retrieve oauth2 token", "err", err.Error())
					r.result <- nil
					continue
				}

				c = client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(r.tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
					client.RequestHeader("Authorization", []string{fmt.Sprintf("%s %s", token.TokenType, token.AccessToken)}),
				)
			} else {
				c = client.NewContextBrokerClient(
					contextBrokerUrl,
					client.Tenant(r.tenant),
					client.UserAgent(fmt.Sprintf("%s/%s", serviceName, serviceVersion)),
				)
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
