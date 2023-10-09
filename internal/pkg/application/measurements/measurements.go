package measurements

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	. "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	p "github.com/diwise/context-broker/pkg/ngsild/types/properties"
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

var statusValue = map[bool]string{true: "on", false: "off"}

func GetMeasurementType(m iotCore.MessageAccepted) string {
	typeOfMeasurement := m.BaseName()

	for _, m := range m.Pack {
		if strings.EqualFold(m.Name, "env") && m.StringValue != "" {
			return fmt.Sprintf("%s/%s", typeOfMeasurement, m.StringValue)
		}
	}

	return typeOfMeasurement
}

func Device(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties,
		decorators.DateLastValueReported(msg.Timestamp),
	)

	const DigitalInputState int = 5500
	v, ok := iotCore.Get[bool](msg, PresenceURN, DigitalInputState)
	if !ok {
		return fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	properties = append(properties, decorators.Status(statusValue[v]))

	if msg.HasLocation() {
		properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
	}

	id := fiware.DeviceIDPrefix + msg.Sensor

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.DeviceTypeName, properties)
}

func GreenspaceRecord(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties, decorators.DateObserved(msg.Timestamp))

	id := fmt.Sprintf("%s%s", fiware.GreenspaceRecordIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	const SensorValue int = 5700
	if pr, ok := iotCore.Get[float64](msg, PressureURN, SensorValue); ok {
		kPa := pr / 1000.0
		properties = append(properties, decorators.Number("soilMoisturePressure", kPa, p.UnitCode("KPA"), p.ObservedAt(msg.Timestamp), p.ObservedBy(observedBy)))
	}

	if co, ok := iotCore.Get[float64](msg, ConductivityURN, SensorValue); ok {
		properties = append(properties, decorators.Number("soilMoistureEc", co, p.UnitCode("MHO"), p.ObservedAt(msg.Timestamp), p.ObservedBy(observedBy)))
	}

	if msg.HasLocation() {
		properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.GreenspaceRecordTypeName, properties)
}

func IndoorEnvironmentObserved(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 10)

	properties = append(properties,
		decorators.Location(msg.Latitude(), msg.Longitude()),
		decorators.DateObserved(msg.Timestamp),
	)

	const (
		ActualNumberOfPersons int = 1
		SensorValue           int = 5700
	)

	temp, tempOk := iotCore.Get[float64](msg, TemperatureURN, SensorValue)
	if tempOk {
		properties = append(properties, Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)))
	}

	humidity, humidityOk := iotCore.Get[float64](msg, HumidityURN, SensorValue)
	if humidityOk {
		properties = append(properties, Humidity(humidity, time.Unix(int64(msg.BaseTime()), 0)))
	}

	illuminance, illuminanceOk := iotCore.Get[float64](msg, IlluminanceURN, SensorValue)
	if illuminanceOk {
		properties = append(properties, Illuminance(illuminance, time.Unix(int64(msg.BaseTime()), 0)))
	}

	peopleCount, peopleCountOk := iotCore.Get[float64](msg, PeopleCountURN, ActualNumberOfPersons)
	if peopleCountOk {
		properties = append(properties, PeopleCount(peopleCount, time.Unix(int64(msg.BaseTime()), 0)))
	}

	if !tempOk && !humidityOk && !illuminanceOk && !peopleCountOk {
		return nil
	}

	id := fiware.IndoorEnvironmentObservedIDPrefix + msg.Sensor

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.IndoorEnvironmentObservedTypeName, properties)
}

func Lifebuoy(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties, decorators.DateLastValueReported(msg.Timestamp))

	const DigitalInputState int = 5500
	v, ok := iotCore.Get[bool](msg, PresenceURN, DigitalInputState)
	if !ok {
		return fmt.Errorf("unable to update lifebuoy because presence is missing in pack from %s", msg.Sensor)
	}

	properties = append(properties, decorators.Status(statusValue[v]))

	if msg.HasLocation() {
		properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
	}

	typeName := "Lifebuoy"
	id := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, msg.Sensor)

	return cip.MergeOrCreate(ctx, cbClient, id, typeName, properties)
}

func WaterConsumptionObserved(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 10)

	const (
		CumulatedWaterVolume string = "1"
		TypeOfMeter          int    = 3
		LeakDetected         int    = 10
		BackFlowDetected     int    = 11
	)

	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))

	// Alarm signifying the potential for an intermittent leak
	if leak, ok := iotCore.Get[bool](msg, WatermeterURN, LeakDetected); ok && leak {
		properties = append(properties, decorators.Number("alarmStopsLeaks", float64(1)))
	} else {
		properties = append(properties, decorators.Number("alarmStopsLeaks", float64(0)))
	}

	// Alarm signifying the potential of backflows occurring
	if backflow, ok := iotCore.Get[bool](msg, WatermeterURN, BackFlowDetected); ok && backflow {
		properties = append(properties, decorators.Number("alarmWaterQuality", float64(1)))
	} else {
		properties = append(properties, decorators.Number("alarmWaterQuality", float64(0)))
	}

	// An alternative name for this item
	if t, ok := iotCore.Get[string](msg, WatermeterURN, TypeOfMeter); ok {
		properties = append(properties, decorators.Text("alternateName", t))
	}

	// lwm2m reports water volume in m3, but the context broker expects litres as default
	toLtr := func(m3 float64) float64 {
		return math.Floor((m3 + 0.0005) * 1000)
	}

	toDateStr := func(t float64) string {
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339Nano)
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With(slog.String("entityID", entityID))

	for _, rec := range msg.Pack {
		if rec.Name == CumulatedWaterVolume {
			w := decorators.Number("waterConsumption", toLtr(*rec.Sum), p.UnitCode("LTR"), p.ObservedAt(toDateStr(rec.Time)), p.ObservedBy(observedBy))
			propsForEachReading := append(properties, w)

			err := cip.MergeOrCreate(ctx, cbClient, entityID, fiware.WaterConsumptionObservedTypeName, propsForEachReading)
			if err != nil {
				logger.Error("failed to merge or create waterConsumption", "err", err.Error())
			}
		}
	}

	return nil
}

func WeatherObserved(ctx context.Context, msg iotCore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	const SensorValue int = 5700
	temp, ok := iotCore.Get[float64](msg, TemperatureURN, SensorValue)
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.Sensor)
	}

	properties = append(properties,
		decorators.DateObserved(msg.Timestamp),
		Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)),
	)

	if src, ok := msg.GetString("source"); ok {
		properties = append(properties, decorators.Source(src))
	}

	if msg.HasLocation() {
		properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
	}

	id := fiware.WeatherObservedIDPrefix + msg.Sensor

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.WeatherObservedTypeName, properties)
}
