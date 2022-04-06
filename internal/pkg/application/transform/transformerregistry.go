package transform

import (
	"context"
)

type TransformerRegistry interface {
	DesignateTransformers(ctx context.Context, typeOfMessage string, typeOfSensor string) MessageTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MessageTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MessageTransformerFunc{
		"temperature/water": WaterQualityObserved,
		"temperature": WeatherObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) DesignateTransformers(ctx context.Context, typeOfMessage string, typeOfSensor string) MessageTransformerFunc {

	mt, exist := tr.registeredTransformers[typeOfSensor] //TODO: better lookup logic...

	if exist {
		return mt
	}

	return nil
}
