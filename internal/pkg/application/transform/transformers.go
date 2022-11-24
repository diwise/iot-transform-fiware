package transform

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	. "github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	p "github.com/diwise/context-broker/pkg/ngsild/types/properties"
	core "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type MessageTransformerFunc func(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error

func WeatherObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	const SensorValue int = 5700

	temp, ok := core.Get[float64](msg, "urn:oma:lwm2m:ext:3303", SensorValue)
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.WeatherObservedIDPrefix + msg.Sensor + ":" + msg.Timestamp

	wo, err := fiware.NewWeatherObserved(id, msg.Latitude(), msg.Longitude(), msg.Timestamp, Temperature(temp))
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, wo, headers)

	if err != nil {
		logger.Error().Err(err).Msg("failed to create entity")
		return err
	}

	logger.Info().Msg("entity created")

	return nil
}

func WaterQualityObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	const SensorValue int = 5700

	temp, ok := core.Get[float64](msg, "urn:oma:lwm2m:ext:3303", SensorValue)
	if !ok {
		return fmt.Errorf("no temperature property was found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.WaterQualityObservedIDPrefix + msg.Sensor + ":" + msg.Timestamp

	wqo, err := entities.New(
		id, fiware.WaterQualityObservedTypeName, entities.DefaultContext(),
		Location(msg.Latitude(), msg.Longitude()),
		DateObserved(msg.Timestamp),
		Temperature(temp),
	)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, wqo, headers)

	if err != nil {
		logger.Error().Err(err).Msg("failed to create entity")
		return err
	}

	logger.Info().Msg("entity created")

	return nil
}

func AirQualityObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		Location(msg.Latitude(), msg.Longitude()),
		DateObserved(msg.Timestamp),
	}

	const SensorValue int = 5700
	const CO2 int = 17

	temp, tempOk := core.Get[float64](msg, "urn:oma:lwm2m:ext:3303", SensorValue)
	if tempOk {
		properties = append(properties, Temperature(temp))
	}

	co2, co2Ok := core.Get[float64](msg, "urn:oma:lwm2m:ext:3428", CO2)
	if co2Ok {
		properties = append(properties, Number("co2", co2))
	}

	if !tempOk && !co2Ok {
		return fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	id := fiware.AirQualityObservedIDPrefix + msg.Sensor + ":" + msg.Timestamp

	aqo, err := entities.New(
		id, fiware.AirQualityObservedTypeName, properties...)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, aqo, headers)

	if err != nil {
		logger.Error().Err(err).Msg("failed to create entity")
		return err
	}

	logger.Info().Msg("entity created")

	return nil
}

func IndoorEnvironmentObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		Location(msg.Latitude(), msg.Longitude()),
		DateObserved(msg.Timestamp),
	}

	const SensorValue int = 5700

	temp, tempOk := core.Get[float64](msg, "urn:oma:lwm2m:ext:3303", SensorValue)
	if tempOk {
		properties = append(properties, Temperature(temp))
	}

	humidity, humidityOk := msg.GetFloat64("humidity")
	if humidityOk {
		properties = append(properties, Number("humidity", humidity))
	}

	luminance, luminanceOk := msg.GetFloat64("luminance")
	if luminanceOk {
		properties = append(properties, Number("luminance", luminance))
	}

	if !tempOk && !humidityOk && !luminanceOk {
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

		ieo, err := entities.New(
			id, fiware.IndoorEnvironmentObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, ieo, headers)
		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}

		logger.Info().Msg("entity created")
	}

	return nil
}

func Device(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := []entities.EntityDecoratorFunc{
		DateLastValueReported(msg.Timestamp),
	}

	const DigitalInputState int = 5500

	if v, ok := core.Get[bool](msg, "urn:oma:lwm2m:ext:3302", DigitalInputState); ok {
		if v {
			properties = append(properties, Status("on"))
		} else {
			properties = append(properties, Status("off"))
		}
	} else {
		return fmt.Errorf("unable to update Device for deviceID %s", msg.Sensor)
	}

	if msg.HasLocation() {
		properties = append(properties, Location(msg.Latitude(), msg.Longitude()))
	}

	entity, err := fiware.NewDevice(msg.Sensor, properties...)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", entity.ID()).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, entity.ID(), entity, headers)

	if err != nil {
		logger.Error().Err(err).Msg("failed to update entity attributes")
		return err
	}

	logger.Info().Msg("entity attributes updated")

	return nil
}

func Lifebuoy(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		DateLastValueReported(msg.Timestamp),
	}

	const DigitalInputState int = 5500

	if v, ok := core.Get[bool](msg, "urn:oma:lwm2m:ext:3302", DigitalInputState); ok {
		if v {
			properties = append(properties, Status("on"))
		} else {
			properties = append(properties, Status("off"))
		}
	} else {
		return fmt.Errorf("unable to update lifebuoy because presence is missing in pack from %s", msg.Sensor)
	}

	if msg.HasLocation() {
		properties = append(properties, Location(msg.Latitude(), msg.Longitude()))
	}

	id := "urn:ngsi-ld:Lifebuoy:" + msg.Sensor

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	logger := logging.GetFromContext(ctx)
	logger = logger.With().Str("entityID", id).Logger()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, id, fragment, headers)

	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			logger.Error().Err(err).Msg("unable to update entity attributes")
			return err
		}

		logger.Info().Msg("failed to update entity attributes (entity not found)")

		entity, _ := entities.New(id, "Lifebuoy", properties...)
		_, err = cbClient.CreateEntity(ctx, entity, headers)

		if err != nil {
			logger.Error().Err(err).Msg("failed to create entity")
			return err
		}
	}

	logger.Info().Msg("entity updated")

	return nil
}

func WaterConsumptionObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	const (
		ObjectURN            string = "urn:oma:lwm2m:ext:3424"
		CumulatedWaterVolume string = "1"
		TypeOfMeter          int    = 3
		LeakDetected         int    = 10
		BackFlowDetected     int    = 11
	)

	log := logging.GetFromContext(ctx)
	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	log = log.With().Str("entityID", entityID).Logger()
	log.Debug().Msgf("transforming message from %s", msg.Sensor)

	props := []entities.EntityDecoratorFunc{entities.DefaultContext()}

	mergeOrCreateEntity := func(entityID string, properties ...entities.EntityDecoratorFunc) error {
		headers := map[string][]string{"Content-Type": {"application/ld+json"}}

		if fragment, err := entities.NewFragment(properties...); err == nil {
			if _, err := cbClient.MergeEntity(ctx, entityID, fragment, headers); err != nil {
				log.Debug().Msgf("could not merge entity, will try to create a new entity %s.", entityID)
				if entity, err := entities.New(entityID, fiware.WaterConsumptionObservedTypeName, properties...); err == nil {
					if _, err = cbClient.CreateEntity(ctx, entity, headers); err != nil {
						return fmt.Errorf("create entity failed: %w", err)
					}
				} else {
					return fmt.Errorf("entities.New failed: %w", err)
				}
			}
		} else {
			return fmt.Errorf("entities.NewFragment failed: %w", err)
		}

		return nil
	}

	props = append(props, Location(msg.Latitude(), msg.Longitude()))

	// Alarm signifying the potential for an intermittent leak
	if leak, ok := core.Get[bool](msg, ObjectURN, LeakDetected); ok && leak {
		props = append(props, Number("alarmStopsLeaks", float64(1)))
	}

	// Alarm signifying the potential of backflows occurring
	if backflow, ok := core.Get[bool](msg, ObjectURN, BackFlowDetected); ok && backflow {
		props = append(props, Number("alarmWaterQuality", float64(1)))
	}

	// An alternative name for this item
	if t, ok := core.Get[string](msg, ObjectURN, TypeOfMeter); ok {
		props = append(props, Text("alternateName", t))
	}

	// lwm2m reports water volume in m3, but the context broker expects litres as default
	toLtr := func(m3 float64) float64 {
		return math.Floor((m3 + 0.0005) * 1000)
	}

	toDateStr := func(t float64) string {
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339Nano)
	}

	for _, rec := range msg.Pack {
		if rec.Name == CumulatedWaterVolume {
			w := Number("waterConsumption", toLtr(*rec.Sum), p.UnitCode("LTR"), p.ObservedAt(toDateStr(rec.Time)), p.ObservedBy(observedBy))
			p := append(props, w)
			err := mergeOrCreateEntity(entityID, p...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GreenspaceRecord(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	curDateTime := msg.Timestamp
	if cdt, ok := msg.GetString("CurrentDateTime"); ok {
		if idx := strings.Index(cdt, "."); idx > 0 {
			curDateTime = cdt[0:idx] + "Z"
		}
	}

	entityID := fmt.Sprintf("%s%s", "urn:ngsi-ld:GreenspaceRecord:", msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	buildfragment := func(patchProperties ...entities.EntityDecoratorFunc) error {
		fragment, err := entities.NewFragment(patchProperties...)
		if err != nil {
			return fmt.Errorf("entities.NewFragment failed: %w", err)
		}

		properties := append(patchProperties, DateObserved(curDateTime))

		//_, err = cbClient.UpdateEntityAttributes(ctx, entityID, fragment, headers)
		_, err = cbClient.MergeEntity(ctx, entityID, fragment, headers)

		if err != nil {
			// If we failed to update the entity's attributes, we need to create it
			properties := append(properties, entities.DefaultContext())

			if msg.HasLocation() {
				properties = append(properties, Location(msg.Latitude(), msg.Longitude()))
			}

			var entity types.Entity
			entity, err = entities.New(entityID, fiware.GreenspaceRecordTypeName, properties...)
			if err != nil {
				return fmt.Errorf("entities.New failed: %w", err)
			}

			_, err = cbClient.CreateEntity(ctx, entity, headers)
			if err != nil {
				err = fmt.Errorf("create entity failed: %w", err)
			}
		}

		return err
	}

	// GreenspaceRecord is called by one of its properties. First out creates the entity, all other subsequent calls, independent which property, updates the entity.
	pr, ok := msg.GetFloat64("Pressure")
	if ok {
		patchProperties := []entities.EntityDecoratorFunc{
			Number("soilMoisturePressure", pr, p.UnitCode("KPA"), p.ObservedAt(curDateTime), p.ObservedBy(observedBy)),
		}

		return buildfragment(patchProperties...)
	}

	co, ok := msg.GetFloat64("Conductivity")
	if ok {
		patchProperties := []entities.EntityDecoratorFunc{
			Number("soilMoistureEc", co, p.UnitCode("MHO"), p.ObservedAt(curDateTime), p.ObservedBy(observedBy)),
		}

		return buildfragment(patchProperties...)
	}

	return nil
}
