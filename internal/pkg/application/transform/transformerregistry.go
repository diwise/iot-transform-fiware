package transform

import (
	"context"
)

type TransformerRegistry interface {
	DesignateTransformers(ctx context.Context, typeOfSensor string) MessageTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MessageTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MessageTransformerFunc{
		"urn:oma:lwm2m:ext:3303/water": WaterQualityObserved,
		"urn:oma:lwm2m:ext:3303":       WeatherObserved,
		"urn:oma:lwm2m:ext:3428":       AirQualityObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) DesignateTransformers(ctx context.Context, typeOfSensor string) MessageTransformerFunc {

	mt, exist := tr.registeredTransformers[typeOfSensor] //TODO: better lookup logic...

	if exist {
		return mt
	}

	return nil
}
