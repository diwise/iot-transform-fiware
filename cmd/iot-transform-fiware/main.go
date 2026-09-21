package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/devicemanagement"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api"
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

// ruleSyncInterval är polling-intervallet för regelsynk mellan instanser.
// Ändringar slår igenom utan omstart inom ett intervall; fel loggas och
// nästa tick försöker igen.
const ruleSyncInterval = 5 * time.Second

// shutdownTimeout är en egen övre gräns för att stoppa inflöde och dränera
// pågående arbete. Runnerns shutdown-hook får ingen egen timeout.
const shutdownTimeout = 10 * time.Second

func defaultFlags() flagMap {
	return flagMap{
		listenAddress:    "0.0.0.0",
		controlPort:      "8000",
		servicePort:      "8080",
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

		policiesFile:      "/opt/diwise/config/authz.rego",
		authzAccessObject: "false",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",
	}
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

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	// Reglerna lever i postgres (enda backend, som övriga tjänster);
	// YAML-filerna är seed som läggs in idempotent vid start.
	ruleStore, err := rules.NewPostgres(ctx, storageConfigFromFlags(flags))
	exitIf(err, logger, "failed to connect to rule storage")
	seeded, err := rules.SeedDir(ruleStore, flags[modelsDir])
	exitIf(err, logger, "failed to seed model configuration")
	if seeded.Added+seeded.Updated > 0 {
		logger.Info("seeded model rules", "added", seeded.Added, "updated", seeded.Updated, "skipped", seeded.Skipped)
	}
	var sensorTypes devicemanagement.SensorTypeResolver
	if flags[deviceMgmtUrl] != "" {
		dmClient := devicemanagement.NewOAuth2Client(ctx, flags[oauth2TokenUrl], flags[oauth2ClientId], flags[oauth2ClientSecret], oauthInsecure(flags))
		sensorTypes = devicemanagement.New(flags[deviceMgmtUrl], dmClient, devicemanagement.DefaultTTL)
	}

	models := api.NewModels(ruleStore, sensorTypes)
	if err := models.Refresh(); err != nil {
		exitIf(err, logger, "failed to build initial rule snapshot")
	}
	stopSync := models.RunSync(ctx, ruleSyncInterval, logger)
	defer stopSync()

	cfg := &appConfig{
		messenger:      messenger,
		cbClientFn:     factory,
		ruleStore:      ruleStore,
		models:         models,
		sensorTypes:    sensorTypes,
		defaultContext: flags[defaultContextUrl],
		thingsV1Values: thingsV1ValuesEnabled(flags),
	}

	runner, err := initialize(ctx, flags, cfg, policies)
	exitIf(err, logger, "failed to initialize service runner")

	err = runner.Run(ctx)
	exitIf(err, logger, "failed to start service runner")
}

func initialize(ctx context.Context, flags flagMap, cfg *appConfig, policiesFile *os.File) (servicerunner.Runner[appConfig], error) {
	if flags[contextbrokerUrl] == "" {
		return nil, fmt.Errorf("context broker URL is empty")
	}

	probes := readinessProbes()

	_, runner := servicerunner.New(ctx, *cfg,
		webserver("control", listen(flags[listenAddress]), port(flags[controlPort]),
			pprof(), liveness(func() error { return nil }), readiness(probes),
		),
		webserver("public", listen(flags[listenAddress]), port(flags[servicePort]), tracing(true),
			muxinit(func(ctx context.Context, identifier string, port string, appCfg *appConfig, handler *http.ServeMux) error {
				defer policiesFile.Close()
				logging.GetFromContext(ctx).Debug("register api handlers...")
				return api.RegisterHandlers(ctx, handler,
					appCfg.models,
					policiesFile, api.WithAccessObjectAuthorization(accessObjectEnabled(flags)))
			}),
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
			if svcCfg.ruleStore != nil {
				svcCfg.ruleStore.Close()
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

	// things (snapshot-providern levererar aktuell motor per meddelande;
	// API-skrivningar och polling-synk byter snapshot atomärt)
	if err := messenger.RegisterTopicMessageHandler(ThingUpdatedTopic, things.NewDynamicThingTopicMessageHandler(cbClientFn, svcCfg.models, svcCfg.defaultContext)); err != nil {
		return fmt.Errorf("failed to register dynamic thing handler: %w", err)
	}

	// Versionerade värdesevent (E7): samma transformator, egen prenumeration.
	// Gatas av THINGS_V1_VALUES (exakt "true"); av som default så att
	// cutover till nya kontrakt är ett explicit driftbeslut.
	if svcCfg.thingsV1Values {
		if err := messenger.RegisterTopicMessageHandler(ThingValuesV1Topic, things.NewThingValuesTopicMessageHandler(cbClientFn, svcCfg.models, svcCfg.defaultContext)); err != nil {
			return fmt.Errorf("failed to register thing values handler: %w", err)
		}
	}

	// measurements
	if err := messenger.RegisterTopicMessageHandler(MessageAcceptedTopic, measurements.NewDynamicMeasurementTopicMessageHandler(cbClientFn, svcCfg.models, svcCfg.defaultContext)); err != nil {
		return fmt.Errorf("failed to register measurements handler: %w", err)
	}

	return nil
}

// oauthInsecure is the minimal production seam for the TLS-verification
// toggle. Only the exact string "true" disables verification.
func oauthInsecure(flags flagMap) bool {
	return flags[oauth2InsecureURL] == "true"
}

// storageConfigFromFlags builds the storage configuration explicitly
// from cmd-owned flags (samma mönster som övriga tjänster).
func storageConfigFromFlags(flags flagMap) rules.Config {
	return rules.NewConfig(flags[dbHost], flags[dbUser], flags[dbPassword], flags[dbPort], flags[dbName], flags[dbSSLMode])
}

// accessObjectEnabled is the minimal production seam for the
// access-object toggle, using strconv semantics: invalid values
// silently keep the legacy tenants model.
func accessObjectEnabled(flags flagMap) bool {
	v, _ := strconv.ParseBool(flags[authzAccessObject])
	return v
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
	flags[servicePort] = envOrDef(ctx, "SERVICE_PORT", flags[servicePort])
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
	flags[policiesFile] = envOrDef(ctx, "POLICIES_FILE", flags[policiesFile])
	flags[authzAccessObject] = envOrDef(ctx, "AUTHZ_ACCESS_OBJECT_ENABLED", flags[authzAccessObject])

	flags[dbHost] = envOrDef(ctx, "POSTGRES_HOST", flags[dbHost])
	flags[dbPort] = envOrDef(ctx, "POSTGRES_PORT", flags[dbPort])
	flags[dbName] = envOrDef(ctx, "POSTGRES_DBNAME", flags[dbName])
	flags[dbUser] = envOrDef(ctx, "POSTGRES_USER", flags[dbUser])
	flags[dbPassword] = envOrDef(ctx, "POSTGRES_PASSWORD", flags[dbPassword])
	flags[dbSSLMode] = envOrDef(ctx, "POSTGRES_SSLMODE", flags[dbSSLMode])

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
