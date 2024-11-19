package measurements

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"

	"github.com/diwise/iot-transform-fiware/internal/pkg/application/cip"
	//lint:ignore ST1001 "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators" is a valid import path
	. "github.com/diwise/iot-transform-fiware/internal/pkg/application/decorators"

	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"

	p "github.com/diwise/context-broker/pkg/ngsild/types/properties"
	"github.com/diwise/iot-core/pkg/messaging/events"
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

type MeasurementTransformerFunc func(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error

var (
	statusValue = map[bool]string{true: "on", false: "off"}

	transformers = map[string]MeasurementTransformerFunc{
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
		WatermeterURN:               WaterConsumptionObserved,
	}

	ErrNoRelevantProperties = errors.New("no relevant properties were found in message")
)

func NewMeasurementTopicMessageHandler(messenger messaging.MsgContext, getClientForTenant func(string) client.ContextBrokerClient) messaging.TopicMessageHandler {

	getTransformer := func(m string) MeasurementTransformerFunc {
		if mt, ok := transformers[m]; ok {
			return mt
		}
		return nil
	}

	return func(ctx context.Context, msg messaging.IncomingTopicMessage, logger *slog.Logger) {
		messageAccepted := events.MessageAccepted{}

		err := json.Unmarshal(msg.Body(), &messageAccepted)
		if err != nil {
			logger.Error("unable to unmarshal incoming message", "err", err.Error())
			return
		}

		measurementType := GetMeasurementType(messageAccepted)

		logger = logger.With(
			slog.String("measurement_type", measurementType),
			slog.String("device_id", messageAccepted.DeviceID()),
		)
		ctx = logging.NewContextWithLogger(ctx, logger)

		transformer := getTransformer(measurementType)
		if transformer == nil {
			return
		}

		cbClient := getClientForTenant(messageAccepted.Tenant())
		err = transformer(ctx, messageAccepted, cbClient)
		if err != nil {
			if errors.Is(err, ErrNoRelevantProperties) {
				return
			}

			logger.Error("transform failed", "err", err.Error())
			return
		}
	}
}

func GetMeasurementType(m events.MessageAccepted) string {
	urn, ok := m.Pack().GetStringValue(senml.FindByName("0"))
	if !ok {
		return ""
	}

	env, ok := m.Pack().GetStringValue(senml.FindByName("env"))
	if ok {
		urn = fmt.Sprintf("%s/%s", urn, env)
	}

	return urn
}

func finder(p events.MessageAccepted, objectURN string, n int) senml.RecordFinder {
	falseFn := func(r senml.Record) bool {
		return false
	}

	//TODO: refactor code not to use this logic. Use messaging with filters instead?

	urn, ok := p.Pack().GetStringValue(senml.FindByName("0"))
	if !ok {
		return falseFn
	}

	if !strings.EqualFold(urn, objectURN) {
		return falseFn
	}

	return senml.FindByName(strconv.Itoa(n))
}

func timestamp(msg events.MessageAccepted) time.Time {

	//TODO: get time from the actual record instead

	ts, ok := msg.Pack().GetTime(senml.FindByName("0"))
	if !ok {
		return time.Now().UTC()
	}
	return ts
}

func AirQualityObserved(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties, decorators.DateObserved(msg.Timestamp.Format(time.RFC3339)))

	const (
		SensorValue         int = 5700
		CarbonDioxide       int = 17
		ParticulateMatter10 int = 1
		ParticulateMatter25 int = 3
		ParticulateMatter1  int = 5
		NitrogenDioxide     int = 15
		NitrogenMonoxide    int = 19
	)

	temp, tempOk := msg.Pack().GetValue(finder(msg, TemperatureURN, SensorValue))
	if tempOk {
		properties = append(properties, Temperature(temp, timestamp(msg)))
	}

	co2, co2Ok := msg.Pack().GetValue(finder(msg, AirQualityURN, CarbonDioxide))
	if co2Ok {
		properties = append(properties, CO2(co2, timestamp(msg)))
	}

	pm10, pm10Ok := msg.Pack().GetValue(finder(msg, AirQualityURN, ParticulateMatter10))
	if pm10Ok {
		properties = append(properties, PM10(pm10, timestamp(msg)))
	}

	pm1, pm1Ok := msg.Pack().GetValue(finder(msg, AirQualityURN, ParticulateMatter1))
	if pm1Ok {
		properties = append(properties, PM1(pm1, timestamp(msg)))
	}

	pm25, pm25Ok := msg.Pack().GetValue(finder(msg, AirQualityURN, ParticulateMatter25))
	if pm25Ok {
		properties = append(properties, PM25(pm25, timestamp(msg)))
	}

	no2, no2Ok := msg.Pack().GetValue(finder(msg, AirQualityURN, NitrogenDioxide))
	if no2Ok {
		properties = append(properties, NO2(no2, timestamp(msg)))
	}

	no, noOk := msg.Pack().GetValue(finder(msg, AirQualityURN, NitrogenMonoxide))
	if noOk {
		properties = append(properties, NO(no, timestamp(msg)))
	}

	if !tempOk && !co2Ok && !pm10Ok && !pm1Ok && !pm25Ok && !no2Ok && !noOk {
		return ErrNoRelevantProperties
	}

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	id := fiware.AirQualityObservedIDPrefix + msg.DeviceID()

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.AirQualityObservedTypeName, properties)
}

func Device(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties,
		decorators.DateLastValueReported(msg.Timestamp.Format(time.RFC3339)),
	)

	const DigitalInputState int = 5500
	v, ok := msg.Pack().GetBoolValue(finder(msg, PresenceURN, DigitalInputState))
	if !ok {
		return ErrNoRelevantProperties
	}

	properties = append(properties, decorators.Status(statusValue[v]))

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	id := fiware.DeviceIDPrefix + msg.DeviceID()

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.DeviceTypeName, properties)
}

func GreenspaceRecord(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties, decorators.DateObserved(msg.Timestamp.Format(time.RFC3339)))

	id := fmt.Sprintf("%s%s", fiware.GreenspaceRecordIDPrefix, msg.DeviceID())
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.DeviceID())

	const SensorValue int = 5700
	if pr, ok := msg.Pack().GetValue(finder(msg, PressureURN, SensorValue)); ok {
		kPa := pr / 1000.0
		properties = append(properties, decorators.Number("soilMoisturePressure", kPa, p.UnitCode("KPA"), p.ObservedAt(msg.Timestamp.Format(time.RFC3339)), p.ObservedBy(observedBy)))
	}

	if co, ok := msg.Pack().GetValue(finder(msg, ConductivityURN, SensorValue)); ok {
		properties = append(properties, decorators.Number("soilMoistureEc", co, p.UnitCode("MHO"), p.ObservedAt(msg.Timestamp.Format(time.RFC3339)), p.ObservedBy(observedBy)))
	}

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.GreenspaceRecordTypeName, properties)
}

func IndoorEnvironmentObserved(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 10)

	properties = append(properties, decorators.DateObserved(msg.Timestamp.Format(time.RFC3339)))

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	const (
		ActualNumberOfPersons int = 1
		SensorValue           int = 5700
	)

	temp, tempOk := msg.Pack().GetValue(finder(msg, TemperatureURN, SensorValue))
	if tempOk {
		properties = append(properties, Temperature(temp, timestamp(msg)))
	}

	humidity, humidityOk := msg.Pack().GetValue(finder(msg, HumidityURN, SensorValue))
	if humidityOk {
		properties = append(properties, Humidity(humidity, timestamp(msg)))
	}

	illuminance, illuminanceOk := msg.Pack().GetValue(finder(msg, IlluminanceURN, SensorValue))
	if illuminanceOk {
		properties = append(properties, Illuminance(illuminance, timestamp(msg)))
	}

	peopleCount, peopleCountOk := msg.Pack().GetValue(finder(msg, PeopleCountURN, ActualNumberOfPersons))
	if peopleCountOk {
		properties = append(properties, PeopleCount(peopleCount, timestamp(msg)))
	}

	if !tempOk && !humidityOk && !illuminanceOk && !peopleCountOk {
		return ErrNoRelevantProperties
	}

	id := fiware.IndoorEnvironmentObservedIDPrefix + msg.DeviceID()

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.IndoorEnvironmentObservedTypeName, properties)
}

func Lifebuoy(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties, decorators.DateLastValueReported(msg.Timestamp.Format(time.RFC3339)))

	const DigitalInputState int = 5500
	v, ok := msg.Pack().GetBoolValue(finder(msg, PresenceURN, DigitalInputState))
	if !ok {
		return fmt.Errorf("unable to update lifebuoy because presence is missing in pack from %s", msg.DeviceID())
	}

	properties = append(properties, decorators.Status(statusValue[v]))

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	typeName := "Lifebuoy"
	id := fmt.Sprintf("urn:ngsi-ld:%s:%s", typeName, msg.DeviceID())

	return cip.MergeOrCreate(ctx, cbClient, id, typeName, properties)
}

func WaterConsumptionObserved(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 10)

	const (
		CumulatedWaterVolume string = "1"
		TypeOfMeter          int    = 3
		LeakDetected         int    = 10
		BackFlowDetected     int    = 11
	)

	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.DeviceID())
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.DeviceID())

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	// Alarm signifying the potential for an intermittent leak
	if leak, ok := msg.Pack().GetBoolValue(finder(msg, WatermeterURN, LeakDetected)); ok && leak {
		properties = append(properties, decorators.Number("alarmStopsLeaks", float64(1)))
	} else {
		properties = append(properties, decorators.Number("alarmStopsLeaks", float64(0)))
	}

	// Alarm signifying the potential of backflows occurring
	if backflow, ok := msg.Pack().GetBoolValue(finder(msg, WatermeterURN, BackFlowDetected)); ok && backflow {
		properties = append(properties, decorators.Number("alarmWaterQuality", float64(1)))
	} else {
		properties = append(properties, decorators.Number("alarmWaterQuality", float64(0)))
	}

	// An alternative name for this item
	if t, ok := msg.Pack().GetStringValue(finder(msg, WatermeterURN, TypeOfMeter)); ok {
		properties = append(properties, decorators.Text("alternateName", t))
	}

	// lwm2m reports water volume in m3, but the context broker expects litres as default
	toLtr := func(m3 float64) float64 {
		return math.Floor((m3 + 0.0005) * 1000)
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With(slog.String("entityID", entityID))

	r, ok := msg.Pack().GetRecord(senml.FindByName(CumulatedWaterVolume))

	if !ok {
		return fmt.Errorf("unable to get record for CumulatedWaterVolume")
	}

	vol, volOk := r.GetValue()
	ts, timeOk := r.GetTime()

	if !(volOk && timeOk) {
		return fmt.Errorf("unable to get value (%t) or time (%t)", volOk, timeOk)
	}

	w := decorators.Number("waterConsumption", toLtr(vol), p.UnitCode("LTR"), p.ObservedAt(ts.Format(time.RFC3339)), p.ObservedBy(observedBy))
	propsForEachReading := append(properties, w)

	err := cip.MergeOrCreate(ctx, cbClient, entityID, fiware.WaterConsumptionObservedTypeName, propsForEachReading)
	if err != nil {
		logger.Error("failed to merge or create waterConsumption", "err", err.Error())
	}

	return nil
}

func WeatherObserved(ctx context.Context, msg events.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	const SensorValue int = 5700
	temp, ok := msg.Pack().GetValue(finder(msg, TemperatureURN, SensorValue))
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.DeviceID())
	}

	properties = append(properties,
		decorators.DateObserved(msg.Timestamp.Format(time.RFC3339)),
		Temperature(temp, timestamp(msg)),
	)

	if src, ok := msg.Pack().GetStringValue(senml.FindByName("source")); ok {
		properties = append(properties, decorators.Source(src))
	}

	if lat, lon, ok := msg.Pack().GetLatLon(); ok {
		properties = append(properties, decorators.Location(lat, lon))
	}

	id := fiware.WeatherObservedIDPrefix + msg.DeviceID()

	return cip.MergeOrCreate(ctx, cbClient, id, fiware.WeatherObservedTypeName, properties)
}
