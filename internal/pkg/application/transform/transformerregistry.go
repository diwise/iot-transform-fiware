package transform

import (
	"context"
)

type TransformerRegistry interface {
	GetTransformerForSensorType(ctx context.Context, typeOfSensor string) MessageTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MessageTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MessageTransformerFunc{
		"urn:oma:lwm2m:ext:3303/water":    WaterQualityObserved,
		"urn:oma:lwm2m:ext:3303/air":      WeatherObserved,
		"urn:oma:lwm2m:ext:3303/indoors":  AirQualityObserved,
		"urn:oma:lwm2m:ext:3428/indoors":  AirQualityObserved,
		"urn:oma:lwm2m:ext:3302":          Device,
		"urn:oma:lwm2m:ext:3302/lifebuoy": Lifebuoy,
		"urn:oma:lwm2m:ext:3304/soil":     GreenspaceRecord,
		"urn:oma:lwm2m:ext:3424":          WaterConsumptionObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) GetTransformerForSensorType(ctx context.Context, typeOfSensor string) MessageTransformerFunc {

	mt, ok := tr.registeredTransformers[typeOfSensor] //TODO: better lookup logic...

	if !ok {
		return nil
	}

	return mt
}
