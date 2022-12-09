package transform

import (
	"context"
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

type TransformerRegistry interface {
	GetTransformerForSensorType(ctx context.Context, typeOfSensor string) MessageTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MessageTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MessageTransformerFunc{
		AirQualityURN:               AirQualityObserved,
		AirQualityURN + "/indoors":  IndoorEnvironmentObserved,
		HumidityURN + "/indoors":    IndoorEnvironmentObserved,
		TemperatureURN + "/indoors": IndoorEnvironmentObserved,
		PeopleCountURN + "/indoors": IndoorEnvironmentObserved,
		ConductivityURN + "/soil":   GreenspaceRecord,
		PressureURN + "/soil":       GreenspaceRecord,
		PresenceURN:                 Device,
		PresenceURN + "/lifebuoy":   Lifebuoy,
		TemperatureURN + "/air":     WeatherObserved,
		TemperatureURN + "/water":   WaterQualityObserved,
		WatermeterURN:               WaterConsumptionObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) GetTransformerForSensorType(ctx context.Context, typeOfSensor string) MessageTransformerFunc {
	if mt, ok := tr.registeredTransformers[typeOfSensor]; ok {
		return mt
	}

	return nil
}
