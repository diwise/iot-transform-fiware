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
	"github.com/rs/zerolog"
)

type MessageTransformerFunc func(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error

func WeatherObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	/*
		ObjectURN: urn:oma:lwm2m:ext:3303
		ID      Name            Type     Unit
		5700    Sensor Value    Float
	*/

	temp, ok := msg.GetFloat64("5700")
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
	/*
		ObjectURN: urn:oma:lwm2m:ext:3303
		ID      Name            Type     Unit
		5700    Sensor Value    Float
	*/

	temp, ok := msg.GetFloat64("5700")
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
	/*
		ObjectURN: urn:oma:lwm2m:ext:3303
		ID      Name            Type     Unit
		5700    Sensor Value    Float

		ObjectURN: urn:oma:lwm2m:ext:3428
		ID  Name    Type    Unit
		17  CO2     Float   ppm
	*/

	properties := []entities.EntityDecoratorFunc{
		entities.DefaultContext(),
		Location(msg.Latitude(), msg.Longitude()),
		DateObserved(msg.Timestamp),
	}

	temp, tempOk := msg.GetFloat64("5700")
	if tempOk {
		properties = append(properties, Temperature(temp))
	}

	co2, co2Ok := msg.GetFloat64("17")
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

	/*
		ObjectURN: urn:oma:lwm2m:ext:3303
		ID      Name            Type     Unit
		5700    Sensor Value    Float

		ObjectURN: urn:oma:lwm2m:ext:3428
		ID  Name    Type    Unit
		17  CO2     Float   ppm
	*/

	temp, tempOk := msg.GetFloat64("measurements")
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
	/*
		ObjectURN: urn:oma:lwm2m:ext:3302
		ID      Name                    Type       Unit
		5500    Digital Input State     Boolean
	*/

	v, ok := msg.GetBool("5500")

	if !strings.EqualFold(msg.BaseName(), "urn:oma:lwm2m:ext:3302") || !ok {
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

func Lifebuoy(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	/*
		ObjectURN: urn:oma:lwm2m:ext:3302
		ID      Name                    Type       Unit
		5500    Digital Input State     Boolean
	*/

	v, ok := msg.GetBool("5500")
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

func WaterConsumptionObserved(ctx context.Context, msg core.MessageAccepted, cbClient client.ContextBrokerClient) error {
	/*
		ObjectURN: urn:oma:lwm2m:ext:3424
		ID   Name                       Type        Unit
		1    Cumulated water volume     Float       m3
		3    Type of meter              String
		10   Leak detected              Boolean
		11   Back flow detected         Boolean
	*/

	const ObjectURN string = "urn:oma:lwm2m:ext:3424"

	log := logging.GetFromContext(ctx)
	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	log = log.With().Str("entityID", entityID).Logger()
	log.Debug().Msgf("transforming message from %s", msg.Sensor)

	var props []entities.EntityDecoratorFunc

	if msg.HasLocation() {
		props = append(props, Location(msg.Latitude(), msg.Longitude()))
	}

	// Alarm signifying the potential for an intermittent leak
	if leak, ok := core.Get[bool](msg, ObjectURN, 10); ok && leak {
		props = append(props, Number("alarmStopsLeaks", float64(1)))
	}

	// Alarm signifying the potential of backflows occurring
	if backflow, ok := core.Get[bool](msg, ObjectURN, 11); ok && backflow {
		props = append(props, Number("alarmWaterQuality", float64(1)))
	}

	// An alternative name for this item
	if t, ok := core.Get[string](msg, ObjectURN, 3); ok {
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
		if rec.Name == "1" {
			w := Number("waterConsumption", toLtr(*rec.Sum), p.UnitCode("LTR"), p.ObservedAt(toDateStr(rec.Time)), p.ObservedBy(observedBy))
			p := append(props, w)
			err := storeWaterConsumption(ctx, log, cbClient, entityID, p...)
			if err != nil {
				return err
			}
		}
	}

	return nil
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
