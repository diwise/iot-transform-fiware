package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/devicemanagement"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
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

// Projektionsworkerns budgetar (Våg 4 K): poll-intervall, batchstorlek,
// claim-lease och slutdräneringsbudget inom shutdownTimeout. Workern äger
// aldrig lagret — Close sköter A via ruleStore/projectionStore.
const (
	projectionWorkerInterval   = 5 * time.Second
	projectionWorkerBatch      = 50
	projectionWorkerLease      = time.Minute
	projectionWorkerStopBudget = 8 * time.Second
)

func defaultFlags() flagMap {
	return flagMap{
		listenAddress:    "0.0.0.0",
		controlPort:      "8000",
		servicePort:      "8080",
		contextbrokerUrl: "http://context-broker",
		brokerTimeout:    "10s",
		tokenTimeout:     "5s",

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
		authzAccessObject: "true",

		dbHost:     "",
		dbUser:     "",
		dbPassword: "",
		dbPort:     "5432",
		dbName:     "diwise",
		dbSSLMode:  "disable",
	}
}

const (
	MessageAcceptedTopic string = "message.accepted"
	// ThingValuesV1Topic bär versionerade värdesevent (things.v1.values).
	ThingValuesV1Topic string = transform.EventThingsV1Values
	// ThingRelationsV1Topic bär versionerade relationshändelser
	// (things.v1.relations). Samma cutover-flagga som värdeseventen.
	ThingRelationsV1Topic string = transform.EventThingsV1Relations
	// ThingLifecycleV1Topic bär versionerade livscykelhändelser
	// (things.v1.lifecycle). Samma cutover-flagga som övriga things.v1.*.
	ThingLifecycleV1Topic string = transform.EventThingsV1Lifecycle
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

	brokerBudget, tokenBudget, err := timeoutBudgets(flags)
	exitIf(err, logger, "invalid broker/token timeouts")
	factory := contextbroker.NewContextBrokerClientFactory(ctx, flags[contextbrokerUrl], serviceName, serviceVersion, flags[oauth2ClientId], flags[oauth2ClientSecret], flags[oauth2TokenUrl], oauthInsecure(flags), contextbroker.WithTimeouts(brokerBudget, tokenBudget))

	policies, err := os.Open(flags[policiesFile])
	exitIf(err, logger, "unable to open opa policy file")

	// Reglerna lever i postgres (enda backend, som övriga tjänster);
	// YAML-filerna är seed som läggs in idempotent vid start. Imagen
	// innehåller ingen seed: saknad eller tom MODELS_DIR betyder tom
	// regeluppsättning vid start (varning, ingen startspärr). Trasiga
	// YAML-filer stoppar starten som förr.
	ruleStore, err := rules.NewPostgres(ctx, storageConfigFromFlags(flags))
	exitIf(err, logger, "failed to connect to rule storage")
	if empty, reason := modelsDirEmpty(flags[modelsDir]); empty {
		logger.Warn("models directory empty or missing, starting with empty rule set",
			"models_dir", flags[modelsDir], "reason", reason)
	} else if seeded, err := rules.SeedDir(ctx, ruleStore, flags[modelsDir]); err != nil {
		exitIf(err, logger, "failed to seed model configuration")
	} else if seeded.Added+seeded.Updated+seeded.Pruned > 0 {
		logger.Info("seeded model rules", "added", seeded.Added, "updated", seeded.Updated, "pruned", seeded.Pruned, "skipped", seeded.Skipped)
	}
	var sensorTypes devicemanagement.SensorTypeResolver
	if flags[deviceMgmtUrl] != "" {
		dmClient := devicemanagement.NewOAuth2Client(ctx, flags[oauth2TokenUrl], flags[oauth2ClientId], flags[oauth2ClientSecret], oauthInsecure(flags))
		sensorTypes = devicemanagement.New(flags[deviceMgmtUrl], dmClient, devicemanagement.DefaultTTL)
	}

	models := api.NewModels(ruleStore, sensorTypes)
	if err := models.Refresh(ctx); err != nil {
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

	// F-projektion på delad pool (Våg 4 K): NewForPool tar aldrig över
	// stängning (ruleStore äger poolen); schemat är redan migrerat via
	// regellagrets 005 men EnsureSchema är idempotent och verifierar.
	// IsReady ändrar inte probesemantiken (stubbar förblir OK) — den
	// verifieras här vid start och loggas.
	cfg.projectionStore = projection.NewForPool(ruleStore.Pool())
	if err := cfg.projectionStore.EnsureSchema(ctx); err != nil {
		exitIf(err, logger, "failed to ensure projection schema")
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := cfg.projectionStore.IsReady(readyCtx); err != nil {
		readyCancel()
		exitIf(err, logger, "projection store is not ready")
	}
	readyCancel()
	worker, err := projection.NewWorker(
		cfg.projectionStore,
		things.WorkerBrokerFunc(factory),
		projectionWorkerInterval, projectionWorkerBatch,
		projectionWorkerLease, projectionWorkerStopBudget,
	)
	exitIf(err, logger, "failed to create projection worker")
	cfg.projectionWorker = worker

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

			if err := registerHandlers(svcCfg); err != nil {
				return err
			}

			// Bakgrundsdränering (F Worker): ägd loop med livscykel-context
			// (verklig rot, inte request-context). Run blockerar tills
			// cancel och gör därefter en sista dränering inom stopBudget
			// (WithoutCancel inuti Workern) innan den returnerar.
			if svcCfg.projectionWorker != nil {
				wctx, cancel := context.WithCancel(context.Background())
				svcCfg.workerCancel = cancel
				done := make(chan struct{})
				svcCfg.workerDone = done
				go func() {
					defer close(done)
					_ = svcCfg.projectionWorker.Run(wctx)
				}()
			}
			return nil
		}),
		onshutdown(func(ctx context.Context, svcCfg *appConfig) error {
			// Stoppa inflöde: avbryt workern och invänta avslutad
			// slutdränering inom budget (oberoende av avbruten stoppsignal).
			if svcCfg.workerCancel != nil {
				svcCfg.workerCancel()
			}
			if svcCfg.workerDone != nil {
				select {
				case <-svcCfg.workerDone:
				case <-time.After(projectionWorkerStopBudget + time.Second):
					logging.GetFromContext(ctx).Debug("projection worker did not stop in budget")
				}
			}
			if err := shutdownMessenger(ctx, svcCfg.messenger); err != nil {
				logging.GetFromContext(ctx).Debug("failed to shut down messenger", "err", err.Error())
			}
			// Delad pool: projectionStore.Close stänger aldrig den delade
			// poolen (NewForPool, owned=false) — idempotent och nil-säker.
			// ruleStore äger och stänger poolen.
			if svcCfg.projectionStore != nil {
				svcCfg.projectionStore.Close()
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
	// API-skrivningar och polling-synk byter snapshot atomärt).
	// thing.updated konsumeras inte längre (PLAN001 steg 7.6): legacy
	// thing-reglerna är ersatta av things-v2-regler. Topicen publiceras
	// fortfarande för andra konsumenter (t.ex. iot-events) — endast denna
	// tjänsts prenumeration är borttagen.

	// Versionerade värdesevent (E7): samma transformator, egen prenumeration.
	// Gatas av THINGS_V1_VALUES (exakt "true"); av som default så att
	// cutover till nya kontrakt är ett explicit driftbeslut. Samma flagga
	// gates relations- (things.v1.relations) och livscykelhändelser
	// (things.v1.lifecycle): ett cutover-beslut för hela things.v1.*-familjen.
	// Relations/lifecycle-handlers kopplas med F:s projektionsstore
	// (WithStore; nil ger direktväg utan ändrat ack/retry-kontrakt).
	if svcCfg.thingsV1Values {
		if err := messenger.RegisterTopicMessageHandler(ThingValuesV1Topic, things.NewThingValuesTopicMessageHandler(cbClientFn, svcCfg.models, svcCfg.defaultContext)); err != nil {
			return fmt.Errorf("failed to register thing values handler: %w", err)
		}
		if err := messenger.RegisterTopicMessageHandler(ThingRelationsV1Topic, things.NewThingRelationsTopicMessageHandlerWithStore(cbClientFn, svcCfg.models, svcCfg.defaultContext, svcCfg.projectionStore)); err != nil {
			return fmt.Errorf("failed to register thing relations handler: %w", err)
		}
		if err := messenger.RegisterTopicMessageHandler(ThingLifecycleV1Topic, things.NewThingLifecycleTopicMessageHandlerWithStore(cbClientFn, svcCfg.models, svcCfg.defaultContext, svcCfg.projectionStore)); err != nil {
			return fmt.Errorf("failed to register thing lifecycle handler: %w", err)
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
	flags[brokerTimeout] = envOrDef(ctx, "NGSI_CB_TIMEOUT", flags[brokerTimeout])
	flags[tokenTimeout] = envOrDef(ctx, "OAUTH2_TOKEN_TIMEOUT", flags[tokenTimeout])
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

func timeoutBudgets(flags flagMap) (time.Duration, time.Duration, error) {
	broker, err := time.ParseDuration(flags[brokerTimeout])
	if err != nil || broker <= 0 {
		return 0, 0, fmt.Errorf("NGSI_CB_TIMEOUT must be a positive duration")
	}
	token, err := time.ParseDuration(flags[tokenTimeout])
	if err != nil || token <= 0 {
		return 0, 0, fmt.Errorf("OAUTH2_TOKEN_TIMEOUT must be a positive duration")
	}
	return broker, token, nil
}

// modelsDirEmpty rapporterar om dir saknas eller inte innehåller några
// .yaml/.yml-filer (samma urval som rules.SeedDir). Tjänsten startar då
// med tom regeluppsättning i stället för att vägra starta; regler
// tillförs via mount på MODELS_DIR eller via regel-API:t.
func modelsDirEmpty(dir string) (bool, string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return true, err.Error()
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		switch strings.ToLower(filepath.Ext(e.Name())) {
		case ".yaml", ".yml":
			return false, ""
		}
	}
	return true, "no model rules found"
}

func exitIf(err error, logger *slog.Logger, msg string, args ...any) {
	if err != nil {
		logger.With(args...).Error(msg, "err", err.Error())
		os.Exit(1)
	}
}
