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
	lwm2m "github.com/diwise/iot-core/pkg/lwm2m"
	measurements "github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/rs/zerolog"
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	temp, ok := msg.GetFloat64(measurements.Temperature)
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

func WaterQualityObserved(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	temp, ok := msg.GetFloat64(measurements.Temperature)
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

func AirQualityObserved(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		Location(msg.Latitude(), msg.Longitude()),
		DateObserved(msg.Timestamp),
	}

	temp, tempOk := msg.GetFloat64(measurements.Temperature)
	if tempOk {
		properties = append(properties, Temperature(temp))
	}

	co2, co2Ok := msg.GetFloat64(measurements.CO2)
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

func Device(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	v, ok := msg.GetBool(measurements.Presence)

	if !strings.EqualFold(msg.BaseName(), lwm2m.Presence) || !ok {
		return fmt.Errorf("unable to update Device for deviceID %s", msg.Sensor)
	}

	properties := []entities.EntityDecoratorFunc{
		DateLastValueReported(msg.Timestamp),
	}

	if v {
		properties = append(properties, Status("on"))
	} else {
		properties = append(properties, Status("off"))
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

func Lifebuoy(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	v, ok := msg.GetBool(measurements.Presence)
	if !ok {
		return fmt.Errorf("unable to update lifebuoy because presence is missing in pack from %s", msg.Sensor)
	}

	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		DateLastValueReported(msg.Timestamp),
	}

	if v {
		properties = append(properties, Status("on"))
	} else {
		properties = append(properties, Status("off"))
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

func WaterConsumptionObserved(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	// w1h: currentTime, logDateTime, logVolume, deltas
	// w1e(w1t): currentTime, currentVolume, (temperature), logDateTime, logVolume, deltas

	log := logging.GetFromContext(ctx)
	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	log = log.With().Str("entityID", entityID).Logger()
	log.Debug().Msgf("transforming message from %s", msg.Sensor)

	// currentTime: sensor time
	// currentVolume: current volume
	// logDateTime: time for first log
	// logVolume: volume for first log
	// deltas: incremental volume each hour

	// lwm2m reports water volume in m3, but the context broker expects litres as default

	// first create WaterConsumptionObserved for first logValue
	if logVolume, ok := msg.GetFloat64("LogVolume"); ok {
		if logDateTime, ok := msg.GetTime("LogDateTime"); ok {
			vol := math.Floor((logVolume + 0.0005) * 1000)
			dt := time.Unix(int64(logDateTime), 0).UTC().Format(time.RFC3339Nano)
			props := waterConsumptionProps(vol, dt, observedBy, msg)

			err := storeWaterConsumption(ctx, log, cbClient, entityID, props...)
			if err != nil {
				return err
			}
		}
	}

	// then continue with delta volumes
	for _, record := range msg.Pack {
		if strings.EqualFold("DeltaVolume", record.Name) {
			vol := math.Floor((*record.Sum + 0.0005) * 1000)
			dt := time.Unix(int64(record.Time), 0).UTC().Format(time.RFC3339Nano)
			props := waterConsumptionProps(vol, dt, observedBy, msg)

			err := storeWaterConsumption(ctx, log, cbClient, entityID, props...)
			if err != nil {
				return err
			}
		}
	}

	// last add current volume if exists
	if currentVolume, ok := msg.GetFloat64("CurrentVolume"); ok {
		if currentDateTime, ok := msg.GetTime("CurrentDateTime"); ok {
			vol := math.Floor((currentVolume + 0.0005) * 1000)
			dt := time.Unix(int64(currentDateTime), 0).UTC().Format(time.RFC3339Nano)
			props := waterConsumptionProps(vol, dt, observedBy, msg)

			err := storeWaterConsumption(ctx, log, cbClient, entityID, props...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GreenspaceRecord(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {
	curDateTime := msg.Timestamp
	if cdt, ok := msg.GetString("CurrentDateTime"); ok {
		if idx := strings.Index(cdt, "."); idx > 0 {
			cdt = cdt[0:idx] + "Z"
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

			if msg.IsLocated() {
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

func waterConsumptionProps(vol float64, dt, observedBy string, msg iotcore.MessageAccepted) []entities.EntityDecoratorFunc {
	props := []entities.EntityDecoratorFunc{
		Number("waterConsumption", vol, p.UnitCode("LTR"), p.ObservedAt(dt), p.ObservedBy(observedBy)),
	}

	if msg.HasLocation() {
		props = append(props, Location(msg.Latitude(), msg.Longitude()))
	}

	return props
}

func storeWaterConsumption(ctx context.Context, log zerolog.Logger, cbClient client.ContextBrokerClient, entityID string, props ...entities.EntityDecoratorFunc) error {
	if err := updateWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
		log.Debug().Msgf("could not update entity, will try to create a new entity %s.", entityID)

		if err := createWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
			log.Error().Err(err).Msg("failed to create entity")
			return fmt.Errorf("failed to create new entity: %w", err)
		}
	}

	return nil
}

func createWaterConsumption(ctx context.Context, cbClient client.ContextBrokerClient, entityID string, props ...entities.EntityDecoratorFunc) error {
	var entity types.Entity

	props = append(props, entities.DefaultContext())

	entity, err := entities.New(entityID, fiware.WaterConsumptionObservedTypeName, props...)
	if err != nil {
		return fmt.Errorf("entities.New failed: %w", err)
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, entity, headers)
	if err != nil {
		return fmt.Errorf("create entity failed: %w", err)
	}

	return nil
}

func updateWaterConsumption(ctx context.Context, cbClient client.ContextBrokerClient, entityID string, props ...entities.EntityDecoratorFunc) error {
	fragment, err := entities.NewFragment(props...)
	if err != nil {
		return fmt.Errorf("entities.NewFragment failed: %w", err)
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, entityID, fragment, headers)
	if err != nil {
		return fmt.Errorf("update entity failed: %w", err)
	}

	return nil
}
