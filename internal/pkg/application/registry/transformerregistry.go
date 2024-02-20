package registry

import (
	"context"

	"github.com/diwise/iot-transform-fiware/internal/pkg/application/functions"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/measurements"

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
	LevelFunction string = "level"
)

type MeasurementTransformerFunc func(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error

type FunctionTransformerFunc func(ctx context.Context, f functions.Func, cbClient client.ContextBrokerClient) error

type TransformerRegistry interface {
	GetTransformerForMeasurement(ctx context.Context, measurementType string) MeasurementTransformerFunc
	GetTransformerForFunction(ctx context.Context, functionType string) FunctionTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MeasurementTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MeasurementTransformerFunc{
		AirQualityURN:               measurements.AirQualityObserved,
		AirQualityURN + "/indoors":  measurements.IndoorEnvironmentObserved,
		HumidityURN + "/indoors":    measurements.IndoorEnvironmentObserved,
		TemperatureURN + "/indoors": measurements.IndoorEnvironmentObserved,
		PeopleCountURN + "/indoors": measurements.IndoorEnvironmentObserved,
		ConductivityURN + "/soil":   measurements.GreenspaceRecord,
		PressureURN + "/soil":       measurements.GreenspaceRecord,
		PresenceURN:                 measurements.Device,
		PresenceURN + "/lifebuoy":   measurements.Lifebuoy,
		TemperatureURN + "/air":     measurements.WeatherObserved,
		WatermeterURN:               measurements.WaterConsumptionObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) GetTransformerForMeasurement(ctx context.Context, typeOfMeasurement string) MeasurementTransformerFunc {
	if mt, ok := tr.registeredTransformers[typeOfMeasurement]; ok {
		return mt
	}

	return nil
}

func (tr *transformerRegistry) GetTransformerForFunction(ctx context.Context, functionType string) FunctionTransformerFunc {
	switch functionType {
	case WaterQualityFunction:
		return functions.WaterQualityObserved
	case LevelFunction:
		return functions.WasteContainer
	default:
		return nil
	}
}
