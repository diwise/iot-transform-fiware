package transform

import (
	"context"
)

type TransformerRegistry interface {
	DesignateTransformers(ctx context.Context, typeOfMsg string) MessageTransformerFunc
}

type transformerRegistry struct {
	registeredTransformers map[string]MessageTransformerFunc
}

func NewTransformerRegistry() TransformerRegistry {
	transformers := map[string]MessageTransformerFunc{
		"WeatherObserved": WeatherObserved,
	}

	return &transformerRegistry{
		registeredTransformers: transformers,
	}
}

func (tr *transformerRegistry) DesignateTransformers(ctx context.Context, typeOfMsg string) MessageTransformerFunc {
	mt, exist := tr.registeredTransformers[typeOfMsg]
	if exist {
		return mt
	}
	return nil
}
