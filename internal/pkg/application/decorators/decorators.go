package decorators

import (
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
)

func Temperature(temp float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("temperature", temp, properties.ObservedAt(formatTime(observedAt)))
}

func Humidity(humidity float64, observedAt time.Time) entities.EntityDecoratorFunc {	
	return decorators.Number("humidity", humidity, properties.ObservedAt(formatTime(observedAt)))
}

func Illuminance(illuminance float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("illuminance", illuminance, properties.ObservedAt(formatTime(observedAt)))
}

func PeopleCount(peopleCount float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("peopleCount", peopleCount, properties.ObservedAt(formatTime(observedAt)))
}

func CO2(co2 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("co2", co2, properties.ObservedAt(formatTime(observedAt)))
}

func PM10(pm10 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm10", pm10, properties.ObservedAt(formatTime(observedAt)))
}

func PM25(pm25 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm25", pm25, properties.ObservedAt(formatTime(observedAt)))
}

func PM1(pm1 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm1", pm1, properties.ObservedAt(formatTime(observedAt)))
}

func NO2(no2 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("no2", no2, properties.ObservedAt(formatTime(observedAt)))
}

func NO(no float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("no", no, properties.ObservedAt(formatTime(observedAt)))
}

func FillingLevel(v float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("fillingLevel", v, properties.ObservedAt(formatTime(observedAt)))
}

func formatTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339)
}