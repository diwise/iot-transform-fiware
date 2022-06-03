package transform

import (
	"context"
	"fmt"
	"strings"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	. "github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	lwm2m "github.com/diwise/iot-core/pkg/lwm2m"
	measurements "github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
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
