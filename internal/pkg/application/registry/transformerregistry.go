package registry

import (
	"context"
	"fmt"
	"reflect"
	"runtime"

	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	iotCore "github.com/diwise/iot-core/pkg/messaging/events"
)

const (
	AirQualityURN   string = "urn:oma:lwm2m:ext:3428"
	ConductivityURN string = "urn:oma:lwm2m:ext:3327"
	HumidityURN     string = "urn:oma:lwm2m:ext:3304"
	IlluminanceURN  string = "urn:oma:lwm2m:ext:3301"
	PeopleCountURN  string = "urn:oma:lwm2m:ext:3434"
	PresenceURN     string = "urn:oma:lwm2m:ext:3302"
	PressureURN     string = "urn:oma:lwm2m:ext:3323"
	TemperatureURN  string = "urn:oma:lwm2m:ext:3303"
	WatermeterURN   string = "urn:oma:lwm2m:ext:3424"
)

const (
	WaterQualityFunction string = "waterquality"
	AirQualityFunction   string = "airquality"
)

type MeasurementTransformerFunc func(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error

type FunctionTransformerFunc func(ctx context.Context, f functions.FnctUpdated, cbClient client.ContextBrokerClient) error

type TransformerRegistry interface {
	GetTransformerForMeasurement(ctx context.Context, measurementType string) MeasurementTransformerFunc
	GetTransformerForFunction(ctx context.Context, functionType string) FunctionTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MeasurementTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MeasurementTransformerFunc{
		AirQualityURN + "/indoors":  measurements.IndoorEnvironmentObserved,
		ConductivityURN + "/soil":   measurements.GreenspaceRecord,
		HumidityURN + "/indoors":    measurements.IndoorEnvironmentObserved,
		PeopleCountURN + "/indoors": measurements.IndoorEnvironmentObserved,
		PresenceURN + "/lifebuoy":   measurements.Lifebuoy,
		PresenceURN:                 measurements.Device,
		PressureURN + "/soil":       measurements.GreenspaceRecord,
		TemperatureURN + "/air":     measurements.WeatherObserved,
		TemperatureURN + "/indoors": measurements.IndoorEnvironmentObserved,
		WatermeterURN:               measurements.WaterConsumptionObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) GetTransformerForMeasurement(ctx context.Context, typeOfMeasurement string) MeasurementTransformerFunc {
	log := logging.GetFromContext(ctx)

	if mt, ok := tr.registeredTransformers[typeOfMeasurement]; ok {
		log.Info(fmt.Sprintf("type: %s, function: %s", typeOfMeasurement, GetFunctionName(mt)))
		return mt
	}

	log.Info(fmt.Sprintf("could not find transformer for measurement %s", typeOfMeasurement))

	return nil
}

func (tr *transformerRegistry) GetTransformerForFunction(ctx context.Context, functionType string) FunctionTransformerFunc {
	log := logging.GetFromContext(ctx)

	var fn FunctionTransformerFunc

	switch functionType {
	case WaterQualityFunction:
		fn = functions.WaterQualityObserved
	case AirQualityFunction:
		fn = functions.AirQualityObserved
	default:
		log.Info(fmt.Sprintf("could not find transformer for function %s", functionType))
		return nil
	}

	log.Info(fmt.Sprintf("type: %s, function: %s", functionType, GetFunctionName(fn)))

	return fn
}

func GetFunctionName(i any) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
