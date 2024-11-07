package main

import (
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/service-chassis/pkg/infrastructure/servicerunner"
)

type FlagType int
type FlagMap map[FlagType]string

const (
	listenAddress FlagType = iota
	servicePort
	controlPort
	contextbrokerUrl
)

type AppConfig struct {
	messenger  messaging.MsgContext
	cbClientFn ContextBrokerClientFactoryFunc
}

var onstarting = servicerunner.OnStarting[AppConfig]
var onshutdown = servicerunner.OnShutdown[AppConfig]
var webserver = servicerunner.WithHTTPServeMux[AppConfig]
var listen = servicerunner.WithListenAddr[AppConfig]
var port = servicerunner.WithPort[AppConfig]
var pprof = servicerunner.WithPPROF[AppConfig]
var liveness = servicerunner.WithK8SLivenessProbe[AppConfig]
var readiness = servicerunner.WithK8SReadinessProbes[AppConfig]
