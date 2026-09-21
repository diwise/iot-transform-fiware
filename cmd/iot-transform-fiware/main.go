package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	measurementsApp "github.com/diwise/iot-transform-fiware/internal/application/measurements"
	thingsApp "github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/devicemanagement"
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

// shutdownTimeout är en egen övre gräns för att stoppa inflöde och dränera
// pågående arbete. Runnerns shutdown-hook får ingen egen timeout.
const shutdownTimeout = 10 * time.Second

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

		modelsDir:         "/opt/diwise/config/models",
		deviceMgmtUrl:     "http://iot-device-mgmt",
		defaultContextUrl: "https://raw.githubusercontent.com/diwise/context-broker/refs/heads/main/assets/jsonldcontexts/default-context.jsonld",

		thingsV1Values: "false",
	}
}

// loadModels läser och validerar modellkonfigurationen.
func loadModels(flags flagMap) ([]transform.Rule, error) {
	rules, err := config.LoadModelsDir(flags[modelsDir])
	if err != nil {
		return nil, fmt.Errorf("failed to load models from %s: %w", flags[modelsDir], err)
	}

	return rules, nil
}

const (
	ThingUpdatedTopic    string = "thing.updated"
	MessageAcceptedTopic string = "message.accepted"
	// ThingValuesV1Topic bär versionerade värdesevent (things.v1.values).
	ThingValuesV1Topic string = transform.EventThingsV1Values
)

func main() {
	ctx, flags := parseExternalConfig(context.Background(), defaultFlags())

	serviceVersion := buildinfo.SourceVersion()
	ctx, logger, cleanup := o11y.Init(ctx, serviceName, serviceVersion, "json")
	defer cleanup()

	logging.SetLogLevel(parseLogLevel(flags[logLevel]))

	messengerConfig, err := messaging.LoadConfiguration(ctx, serviceName, logger)
	exitIf(err, logger, "messaging configuration error")

	messenger, err := messaging.Initialize(ctx, messengerConfig)
	exitIf(err, logger, "failed to init messenger")

	factory := contextbroker.NewContextBrokerClientFactory(ctx, flags[contextbrokerUrl], serviceName, serviceVersion, flags[oauth2ClientId], flags[oauth2ClientSecret], flags[oauth2TokenUrl], oauthInsecure(flags))

	models, err := loadModels(flags)
	exitIf(err, logger, "failed to load model configuration")

	var sensorTypes devicemanagement.SensorTypeResolver
	if flags[deviceMgmtUrl] != "" {
		dmClient := devicemanagement.NewOAuth2Client(ctx, flags[oauth2TokenUrl], flags[oauth2ClientId], flags[oauth2ClientSecret], oauthInsecure(flags))
		sensorTypes = devicemanagement.New(flags[deviceMgmtUrl], dmClient, devicemanagement.DefaultTTL)
	}

	cfg := &appConfig{
		messenger:      messenger,
		cbClientFn:     factory,
		models:         models,
		sensorTypes:    sensorTypes,
		defaultContext: flags[defaultContextUrl],
		thingsV1Values: thingsV1ValuesEnabled(flags),
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

	probes := readinessProbes()

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		onstarting(func(ctx context.Context, svcCfg *appConfig) error {
			if err := svcCfg.messenger.Start(ctx); err != nil {
				return fmt.Errorf("failed to start messenger: %w", err)
			}

			return registerHandlers(svcCfg)
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			if err := shutdownMessenger(ctx, svcCfg.messenger); err != nil {
				logging.GetFromContext(ctx).Debug("failed to shut down messenger", "err", err.Error())
			}
			return nil
		}))

	return runner, nil
}

// shutdownMessenger stoppar inflöde och dränerar pågående arbete inom en egen
// budget, oberoende av en redan avbruten stoppsignal.
func shutdownMessenger(ctx context.Context, messenger messaging.MsgContext) error {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	return messenger.Shutdown(shutdownCtx)
}

// readinessProbes returns the named readiness stubs. Per harmonization
// standard they always report OK and never call any dependency.
func readinessProbes() map[string]k8shandlers.ServiceProber {
	return map[string]k8shandlers.ServiceProber{
		"rabbitmq": func(context.Context) (string, error) { return "ok", nil },
	}
}

func registerHandlers(svcCfg *appConfig) error {
	messenger := svcCfg.messenger
	cbClientFn := svcCfg.cbClientFn

	// things
	dynamicThings := thingsApp.NewDynamicThingTransformer(svcCfg.models)
	if err := messenger.RegisterTopicMessageHandler(ThingUpdatedTopic, things.NewDynamicThingTopicMessageHandler(cbClientFn, dynamicThings, svcCfg.defaultContext)); err != nil {
		return fmt.Errorf("failed to register dynamic thing handler: %w", err)
	}

	// Versionerade värdesevent (E7): samma transformator, egen prenumeration.
	// Gatas av THINGS_V1_VALUES (exakt "true"); av som default så att
	// cutover till nya kontrakt är ett explicit driftbeslut.
	if svcCfg.thingsV1Values {
		if err := messenger.RegisterTopicMessageHandler(ThingValuesV1Topic, things.NewThingValuesTopicMessageHandler(cbClientFn, dynamicThings, svcCfg.defaultContext)); err != nil {
			return fmt.Errorf("failed to register thing values handler: %w", err)
		}
	}

	// measurements
	dynamicMeasurements := measurementsApp.NewDynamicTransformer(svcCfg.models, svcCfg.sensorTypes)
	if err := messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewDynamicMeasurementTopicMessageHandler(cbClientFn, dynamicMeasurements, svcCfg.defaultContext)); err != nil {
		return fmt.Errorf("failed to register measurements handler: %w", err)
	}

	return nil
}

// oauthInsecure is the minimal production seam for the TLS-verification
// toggle. Only the exact string "true" disables verification.
func oauthInsecure(flags flagMap) bool {
	return flags[oauth2InsecureURL] == "true"
}

// thingsV1ValuesEnabled is the minimal production seam for the versioned
// values subscription. Only the exact string "true" enables it.
func thingsV1ValuesEnabled(flags flagMap) bool {
	return flags[thingsV1Values] == "true"
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
	flags[modelsDir] = envOrDef(ctx, "MODELS_DIR", flags[modelsDir])
	flags[deviceMgmtUrl] = envOrDef(ctx, "DEV_MGMT_URL", flags[deviceMgmtUrl])
	flags[defaultContextUrl] = envOrDef(ctx, "DEFAULT_CONTEXT_URL", flags[defaultContextUrl])
	flags[thingsV1Values] = envOrDef(ctx, "THINGS_V1_VALUES", flags[thingsV1Values])

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
