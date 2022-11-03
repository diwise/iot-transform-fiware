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

	if msg.IsLocated() {
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
		return fmt.Errorf("unable to update lifebuoy, ignoring %s", msg.Sensor)
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

	if msg.IsLocated() {
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
	log := logging.GetFromContext(ctx)
	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	log = log.With().Str("entityID", entityID).Logger()

	log.Debug().Msgf("transforming message from %s", msg.Sensor)

	// start with delta volumes
	for _, record := range msg.Pack {
		if strings.EqualFold("DeltaVolume", record.Name) {
			// lwm2m reports water volume in m3, but the context broker expects litres as default
			vol := math.Floor((*record.Sum + 0.0005) * 1000)
			dt := time.UnixMilli(int64(record.Time * 1000)).Format(time.RFC3339Nano)

			props := waterConsumptionProps(vol, dt, observedBy, msg)

			if err := updateWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
				if err := createWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
					err = fmt.Errorf("failed to create delta volume: %w", err)
					log.Error().Err(err).Msg("create failed")
					return err
				}
			}
		}
	}

	// add current volume
	if cumulatedWaterVolume, ok := msg.GetFloat64(measurements.CumulatedWaterVolume); ok {
		curDateTime := msg.Timestamp
		if cdt, ok := msg.GetString("CurrentDateTime"); ok {
			if idx := strings.Index(cdt, "."); idx > 0 {
				cdt = cdt[0:idx] + "Z"
			}
			curDateTime = cdt
		}

		// lwm2m reports water volume in m3, but the context broker expects litres as default
		cumulatedWaterVolume = math.Floor((cumulatedWaterVolume + 0.0005) * 1000)

		props := waterConsumptionProps(cumulatedWaterVolume, curDateTime, observedBy, msg)

		if err := updateWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
			if err := createWaterConsumption(ctx, cbClient, entityID, props...); err != nil {
				log.Error().Err(err).Msg("failed to create entity")
				return fmt.Errorf("failed to create new entity: %w", err)
			}
		}

		log.Info().Msg("entity updated")

	} else {
		return fmt.Errorf("no volume property was found in message from %s, ignoring", msg.Sensor)
	}

	return nil
}

func waterConsumptionProps(vol float64, dt, observedBy string, msg iotcore.MessageAccepted) []entities.EntityDecoratorFunc {
	props := []entities.EntityDecoratorFunc{
		Number("waterConsumption", vol, p.UnitCode("LTR"), p.ObservedAt(dt), p.ObservedBy(observedBy)),
	}

	if msg.IsLocated() {
		props = append(props, Location(msg.Latitude(), msg.Longitude()))
	}

	return props
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
