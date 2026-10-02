package main

import (
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/devicemanagement"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/diwise/iot-transform-fiware/internal/presentation/api"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

type flagType int
type flagMap map[flagType]string

const (
	listenAddress flagType = iota
	controlPort
	servicePort
	contextbrokerUrl
	brokerTimeout
	tokenTimeout

	oauth2ClientId
	oauth2ClientSecret
	oauth2TokenUrl
	oauth2InsecureURL

	logLevel

	modelsDir
	deviceMgmtUrl
	defaultContextUrl

	thingsV1Values

	policiesFile
	authzAccessObject

	dbHost
	dbUser
	dbPassword
	dbPort
	dbName
	dbSSLMode
)

type appConfig struct {
	messenger  messaging.MsgContext
	cbClientFn contextbroker.ContextBrokerClientFactoryFunc

	// ruleStore är regel­lagret (enda sanningen; YAML är seed).
	ruleStore *rules.PgStore

	// models äger regeluppsättningen: CRUD-snapshot, omladdning och
	// synk mellan instanser. Meddelandehantering läser snapshot per
	// meddelande via provider-metoderna.
	models *api.Models

	// sensorTypes slår upp enhets sensortyp.
	sensorTypes devicemanagement.SensorTypeResolver

	// defaultContext används för entiteter som inte anger en egen context.
	defaultContext string

	// thingsV1Values slår på prenumerationen på versionerade sakhändelser
	// (things.v1.values, things.v1.relations, things.v1.lifecycle). Av som
	// default; sätts på vid cutover till nya kontrakt (E7). Samma motor och
	// modeller återanvänds; händelsekällan (match.event) skiljer reglerna åt.
	thingsV1Values bool
}

var onstarting = servicerunner.OnStarting[appConfig]
var onshutdown = servicerunner.OnShutdown[appConfig]
var webserver = servicerunner.WithHTTPServeMux[appConfig]
var muxinit = servicerunner.OnMuxInit[appConfig]
var tracing = servicerunner.WithTracing[appConfig]
var listen = servicerunner.WithListenAddr[appConfig]
var port = servicerunner.WithPort[appConfig]
var pprof = servicerunner.WithPPROF[appConfig]
var liveness = servicerunner.WithK8SLivenessProbe[appConfig]
var readiness = servicerunner.WithK8SReadinessProbes[appConfig]
