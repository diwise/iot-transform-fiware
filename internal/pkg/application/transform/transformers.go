package transform

import (
	"context"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	ngsi "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/types"
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted) (any, error)

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	weatherObserved := fiware.NewWeatherObserved(msg.Sensor, msg.Latitude(), msg.Longitude(), msg.Timestamp)
	weatherObserved.Temperature = ngsi.NewNumberProperty(msg.SensorValue)

	return weatherObserved, nil
}

func WaterQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	waterQualityObserved := fiware.NewWaterQualityObserved(msg.Sensor, msg.Latitude(), msg.Longitude(), msg.Timestamp)
	waterQualityObserved.Temperature = ngsi.NewNumberProperty(msg.SensorValue)

	return waterQualityObserved, nil
}
