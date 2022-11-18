package transform

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	. "github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	p "github.com/diwise/context-broker/pkg/ngsild/types/properties"
	lwm2m "github.com/diwise/iot-core/pkg/lwm2m"
	measurements "github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/rs/zerolog/log"
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

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, wo, headers)

	return err
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

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, wqo, headers)

	return err

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

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.CreateEntity(ctx, aqo, headers)

	return err
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

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, entity.ID(), entity, headers)

	return err
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

	entity, err := entities.New(id, "Lifebuoy", properties...)
	if err != nil {
		return err
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, entity.ID(), entity, headers)

	return err
}

func WaterConsumptionObserved(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {

	curDateTime := msg.Timestamp
	if cdt, ok := msg.GetString("CurrentDateTime"); ok {
		if idx := strings.Index(cdt, "."); idx > 0 {
			cdt = cdt[0:idx] + "Z"
		}
		curDateTime = cdt
	}

	v, ok := msg.GetFloat64(measurements.CumulatedWaterVolume)
	if !ok {
		return fmt.Errorf("no CumulatedWaterVolume property was found in message from %s, ignoring", msg.Sensor)
	}

	entityID := fmt.Sprintf("%s%s", fiware.WaterConsumptionObservedIDPrefix, msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	// lwm2m reports water volume in m3, but the context broker expects litres as default
	v = math.Floor((v + 0.0005) * 1000)

	patchProperties := []entities.EntityDecoratorFunc{
		Number("waterConsumption", v, p.UnitCode("LTR"), p.ObservedAt(curDateTime), p.ObservedBy(observedBy)),
	}

	fragment, err := entities.NewFragment(patchProperties...)
	if err != nil {
		return fmt.Errorf("entities.NewFragment failed: %w", err)
	}

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}
	_, err = cbClient.UpdateEntityAttributes(ctx, entityID, fragment, headers)

	if err != nil {
		// If we failed to update the entity's attributes, we need to create it
		properties := append(patchProperties, entities.DefaultContext())
		var entity types.Entity

		entity, err = entities.New(entityID, fiware.WaterConsumptionObservedTypeName, properties...)
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

func GreenspaceRecord(ctx context.Context, msg iotcore.MessageAccepted, cbClient client.ContextBrokerClient) error {

	curDateTime := msg.Timestamp
	if cdt, ok := msg.GetString("CurrentDateTime"); ok {
		if idx := strings.Index(cdt, "."); idx > 0 {
			cdt = cdt[0:idx] + "Z"
		}
		curDateTime = cdt
	}

	entityID := fmt.Sprintf("%s%s", "urn:ngsi-ld:GreenspaceRecord:", msg.Sensor)
	observedBy := fmt.Sprintf("%s%s", fiware.DeviceIDPrefix, msg.Sensor)

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	buildfragment := func(patchProperties ...entities.EntityDecoratorFunc) error {
		log.Info().Msg("*** buildfragment ***")

		fragment, err := entities.NewFragment(patchProperties...)
		if err != nil {
			return fmt.Errorf("entities.NewFragment failed: %w", err)
		}

		properties := append(patchProperties, DateObserved(curDateTime))

		_, err = cbClient.UpdateEntityAttributes(ctx, entityID, fragment, headers)

		if err != nil {
			log.Info().Msg("*** buildfragment New *** ")
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

		log.Info().Msg("*** buildfragment Update/New *** ")

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
