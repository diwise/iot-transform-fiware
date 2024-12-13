package decorators

import (
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/context-broker/pkg/ngsild/types/properties"
	"github.com/diwise/context-broker/pkg/ngsild/types/relationships"
)

func Temperature(temp float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("temperature", temp, properties.ObservedAt(FormatTime(observedAt)))
}

func Humidity(humidity float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("humidity", humidity, properties.ObservedAt(FormatTime(observedAt)))
}

func Illuminance(illuminance float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("illuminance", illuminance, properties.ObservedAt(FormatTime(observedAt)))
}

func PeopleCount(peopleCount float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("peopleCount", peopleCount, properties.ObservedAt(FormatTime(observedAt)))
}

func CO2(co2 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("co2", co2, properties.ObservedAt(FormatTime(observedAt)))
}

func PM10(pm10 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm10", pm10, properties.ObservedAt(FormatTime(observedAt)))
}

func PM25(pm25 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm25", pm25, properties.ObservedAt(FormatTime(observedAt)))
}

func PM1(pm1 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("pm1", pm1, properties.ObservedAt(FormatTime(observedAt)))
}

func NO2(no2 float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("no2", no2, properties.ObservedAt(FormatTime(observedAt)))
}

func NO(no float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("no", no, properties.ObservedAt(FormatTime(observedAt)))
}

func FillingLevel(v float64, observedAt time.Time) entities.EntityDecoratorFunc {
	return decorators.Number("fillingLevel", v, properties.ObservedAt(FormatTime(observedAt)))
}

func Name(s string) entities.EntityDecoratorFunc {
	return decorators.Text("name", s)
}

func AlternativeName(s string) entities.EntityDecoratorFunc {
	return decorators.Text("alternativeName", s)
}

func Description(s string) entities.EntityDecoratorFunc {
	return decorators.Text("description", s)
}

func RefDevices(devices []string) entities.EntityDecoratorFunc {
	return entities.R("refDevices", relationships.NewMultiObjectRelationship(devices))
}

func FormatTime(ts time.Time) string {
	return ts.UTC().Format(time.RFC3339)
}
