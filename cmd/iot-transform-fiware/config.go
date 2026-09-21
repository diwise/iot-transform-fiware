package main

import (
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/contextbroker"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/devicemanagement"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

type flagType int
type flagMap map[flagType]string

const (
	listenAddress flagType = iota
	controlPort
	contextbrokerUrl

	oauth2ClientId
	oauth2ClientSecret
	oauth2TokenUrl
	oauth2InsecureURL

	logLevel

	modelsDir
	deviceMgmtUrl
	defaultContextUrl

	thingsV1Values
)

type appConfig struct {
	messenger  messaging.MsgContext
	cbClientFn contextbroker.ContextBrokerClientFactoryFunc

	// models är den inlästa och validerade regellistan.
	models []transform.Rule

	// sensorTypes slår upp enhets sensortyp.
	sensorTypes devicemanagement.SensorTypeResolver

	// defaultContext används för entiteter som inte anger en egen context.
	defaultContext string

	// thingsV1Values slår på prenumerationen på versionerade värdesevent
	// (things.v1.values). Av som default; sätts på vid cutover till nya
	// kontrakt (E7). Samma motor och modeller återanvänds; händelsekällan
	// (match.event) skiljer reglerna åt.
	thingsV1Values bool
}

var onstarting = servicerunner.OnStarting[appConfig]
var onshutdown = servicerunner.OnShutdown[appConfig]
var webserver = servicerunner.WithHTTPServeMux[appConfig]
var listen = servicerunner.WithListenAddr[appConfig]
var port = servicerunner.WithPort[appConfig]
var pprof = servicerunner.WithPPROF[appConfig]
var liveness = servicerunner.WithK8SLivenessProbe[appConfig]
var readiness = servicerunner.WithK8SReadinessProbes[appConfig]
