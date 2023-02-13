package transform

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	p "github.com/diwise/context-broker/pkg/ngsild/types/properties"
	core "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type MessageTransformerFunc func(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error

func AirQualityObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties,
		decorators.DateObserved(msg.Timestamp),
	)

	const (
		SensorValue   int = 5700
		CarbonDioxide int = 17
	)

	temp, tempOk := core.Get[float64](msg, TemperatureURN, SensorValue)
	if tempOk {
		properties = append(properties, Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)))
	}

	co2, co2Ok := core.Get[float64](msg, AirQualityURN, CarbonDioxide)
	if co2Ok {
		properties = append(properties, CO2(co2, time.Unix(int64(msg.BaseTime()), 0)))
	}

	if !tempOk && !co2Ok {
		return fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.AirQualityObservedIDPrefix + msg.Sensor

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	fragment, _ := entities.NewFragment(properties...)

	_, err := cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		properties = append(properties, entities.DefaultContext(), decorators.Location(msg.Latitude(), msg.Longitude()))

		aqo, err := entities.New(id, fiware.AirQualityObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, aqo, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func Device(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties,
		decorators.DateLastValueReported(msg.Timestamp),
	)

	const DigitalInputState int = 5500
	if v, ok := core.Get[bool](msg, PresenceURN, DigitalInputState); ok {
		if v {
			properties = append(properties, decorators.Status("on"))
		} else {
			properties = append(properties, decorators.Status("off"))
		}
	} else {
		return fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.DeviceIDPrefix + msg.Sensor

	fragment, _ := entities.NewFragment(properties...)

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	_, err := cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		if msg.HasLocation() {
			properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
		}

		properties = append(properties, entities.DefaultContext())

		dev, err := entities.New(id, fiware.DeviceTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, dev, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func GreenspaceRecord(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	props := make([]entities.EntityDecoratorFunc, 0, 5)

	props = append(props, decorators.DateObserved(msg.Timestamp))

	id := fmt.Sprintf("%s%s", fiware.GreenspaceRecordIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	const SensorValue int = 5700
	if pr, ok := core.Get[float64](msg, PressureURN, SensorValue); ok {
		props = append(props, decorators.Number("soilMoisturePressure", pr, p.UnitCode("KPA"), p.ObservedAt(msg.Timestamp), p.ObservedBy(observedBy)))
	}

	if co, ok := core.Get[float64](msg, ConductivityURN, SensorValue); ok {
		props = append(props, decorators.Number("soilMoistureEc", co, p.UnitCode("MHO"), p.ObservedAt(msg.Timestamp), p.ObservedBy(observedBy)))
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	fragment, _ := entities.NewFragment(props...)

	_, err := cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		if msg.HasLocation() {
			props = append(props, decorators.Location(msg.Latitude(), msg.Longitude()))
		}

		props = append(props, entities.DefaultContext())

		gsr, err := entities.New(id, fiware.GreenspaceRecordTypeName, props...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, gsr, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func IndoorEnvironmentObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 10)

	properties = append(properties,
		entities.DefaultContext(),
		decorators.Location(msg.Latitude(), msg.Longitude()),
		decorators.DateObserved(msg.Timestamp),
	)

	const (
		ActualNumberOfPersons int = 1
		SensorValue           int = 5700
	)

	temp, tempOk := core.Get[float64](msg, TemperatureURN, SensorValue)
	if tempOk {
		properties = append(properties, Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)))
	}

	humidity, humidityOk := core.Get[float64](msg, HumidityURN, SensorValue)
	if humidityOk {
		properties = append(properties, Humidity(humidity, time.Unix(int64(msg.BaseTime()), 0)))
	}

	illuminance, illuminanceOk := core.Get[float64](msg, IlluminanceURN, SensorValue)
	if illuminanceOk {
		properties = append(properties, Illuminance(illuminance, time.Unix(int64(msg.BaseTime()), 0)))
	}

	peopleCount, peopleCountOk := core.Get[float64](msg, PeopleCountURN, ActualNumberOfPersons)
	if peopleCountOk {
		properties = append(properties, PeopleCount(peopleCount, time.Unix(int64(msg.BaseTime()), 0)))
	}

	if !tempOk && !humidityOk && !illuminanceOk && !peopleCountOk {
		return fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.IndoorEnvironmentObservedIDPrefix + msg.Sensor

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		ieo, err := entities.New(id, fiware.IndoorEnvironmentObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, ieo, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func Lifebuoy(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	properties = append(properties,
		entities.DefaultContext(),
		decorators.DateLastValueReported(msg.Timestamp),
	)

	const DigitalInputState int = 5500
	if v, ok := core.Get[bool](msg, PresenceURN, DigitalInputState); ok {
		if v {
			properties = append(properties, decorators.Status("on"))
		} else {
			properties = append(properties, decorators.Status("off"))
		}
	} else {
		return fmt.Errorf("unable to update lifebuoy because presence is missing in pack from %s", msg.Sensor)
	}

	id := "urn:ngsi-ld:Lifebuoy:" + msg.Sensor

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		if msg.HasLocation() {
			properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
		}

		entity, _ := entities.New(id, "Lifebuoy", properties...)
		_, err = cbClient.CreateEntity(ctx, entity, headers)

		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func WaterConsumptionObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	props := make([]entities.EntityDecoratorFunc, 0, 10)

	const (
		CumulatedWaterVolume string = "1"
		TypeOfMeter          int    = 3
		LeakDetected         int    = 10
		BackFlowDetected     int    = 11
	)

	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", entityID).Logger()

	props = append(props,
		entities.DefaultContext(),
		decorators.Location(msg.Latitude(), msg.Longitude()),
	)

	// Alarm signifying the potential for an intermittent leak
	if leak, ok := core.Get[bool](msg, WatermeterURN, LeakDetected); ok && leak {
		props = append(props, decorators.Number("alarmStopsLeaks", float64(1)))
	} else {
		props = append(props, decorators.Number("alarmStopsLeaks", float64(0)))
	}

	// Alarm signifying the potential of backflows occurring
	if backflow, ok := core.Get[bool](msg, WatermeterURN, BackFlowDetected); ok && backflow {
		props = append(props, decorators.Number("alarmWaterQuality", float64(1)))
	} else {
		props = append(props, decorators.Number("alarmWaterQuality", float64(0)))
	}

	// An alternative name for this item
	if t, ok := core.Get[string](msg, WatermeterURN, TypeOfMeter); ok {
		props = append(props, decorators.Text("alternateName", t))
	}

	// lwm2m reports water volume in m3, but the context broker expects litres as default
	toLtr := func(m3 float64) float64 {
		return math.Floor((m3 + 0.0005) * 1000)
	}

	toDateStr := func(t float64) string {
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339Nano)
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	for _, rec := range msg.Pack {
		if rec.Name == CumulatedWaterVolume {
			w := decorators.Number("waterConsumption", toLtr(*rec.Sum), p.UnitCode("LTR"), p.ObservedAt(toDateStr(rec.Time)), p.ObservedBy(observedBy))
			props := append(props, w)

			fragment, _ := entities.NewFragment(props...)

			_, err := cbClient.MergeEntity(ctx, entityID, fragment, headers)
			if err != nil {
				if !errors.Is(err, ngsierrors.ErrNotFound) {
					logger.Error().Err(err).Msg("failed to merge entity")
					return err
				}

				if msg.HasLocation() {
					props = append(props, decorators.Location(msg.Latitude(), msg.Longitude()))
				}

				props = append(props, entities.DefaultContext())

				wqo, err := entities.New(entityID, fiware.WaterConsumptionObservedTypeName, props...)
				if err != nil {
					return err
				}

				_, err = cbClient.CreateEntity(ctx, wqo, headers)
				if err != nil {
					logger.Error().Err(err).Msg("failed to create entity")
					return err
				}

				logger.Info().Msg("entity created")
			} else {
				logger.Info().Msg("entity merged")
			}
		}
	}

	return nil
}

func WaterQualityObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	const SensorValue int = 5700
	temp, ok := core.Get[float64](msg, TemperatureURN, SensorValue)
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.Sensor)
	}

	properties = append(properties,
		decorators.DateObserved(msg.Timestamp),
		Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)),
	)

	id := fiware.WaterQualityObservedIDPrefix + msg.Sensor

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create fragment")
		return err
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		// TODO: Decide if we should filter out observations without a location
		if msg.HasLocation() {
			properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
		}

		properties = append(properties, entities.DefaultContext())

		wqo, err := entities.New(id, fiware.WaterQualityObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, wqo, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}

func WeatherObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	const SensorValue int = 5700
	temp, ok := core.Get[float64](msg, TemperatureURN, SensorValue)
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.Sensor)
	}

	properties = append(properties,
		decorators.DateObserved(msg.Timestamp),
		Temperature(temp, time.Unix(int64(msg.BaseTime()), 0)),
	)

	id := fiware.WeatherObservedIDPrefix + msg.Sensor

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		logger.Error().Err(err).Msg("failed to create fragment")
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("failed to merge entity")
			return err
		}

		// TODO: Decide if we should filter out observations without a location
		if msg.HasLocation() {
			properties = append(properties, decorators.Location(msg.Latitude(), msg.Longitude()))
		}

		properties = append(properties, entities.DefaultContext())

		wo, err := entities.New(id, fiware.WeatherObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, wo, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	} else {
		logger.Info().Msg("entity merged")
	}

	return nil
}
