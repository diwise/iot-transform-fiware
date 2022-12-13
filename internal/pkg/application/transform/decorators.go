package transform

import (
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

func Temperature(temp float64, observedAt time.Time) entities.EntityDecoratorFunc {
	ts := observedAt.UTC().Format(time.RFC3339Nano)
	return decorators.Number("temperature", temp, properties.ObservedAt(ts))
}

func Humidity(humidity float64, observedAt time.Time) entities.EntityDecoratorFunc {
	ts := observedAt.UTC().Format(time.RFC3339Nano)
	return decorators.Number("humidity", humidity, properties.ObservedAt(ts))
}

func Illuminance(illuminance float64, observedAt time.Time) entities.EntityDecoratorFunc {
	ts := observedAt.UTC().Format(time.RFC3339Nano)
	return decorators.Number("illuminance", illuminance, properties.ObservedAt(ts))
}

func PeopleCount(peopleCount float64, observedAt time.Time) entities.EntityDecoratorFunc {
	ts := observedAt.UTC().Format(time.RFC3339Nano)
	return decorators.Number("peopleCount", peopleCount, properties.ObservedAt(ts))
}

func CO2(co2 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	ts := observedAt.UTC().Format(time.RFC3339Nano)
	return decorators.Number("co2", co2, properties.ObservedAt(ts))
}