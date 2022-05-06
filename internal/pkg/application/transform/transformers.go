package transform

import (
	"context"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	ngsi "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/types"
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted) (any, error)

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	weatherObserved := fiware.NewWeatherObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)
	weatherObserved.Temperature = ngsi.NewNumberProperty(*msg.Pack[1].Value)

	return weatherObserved, nil
}

func WaterQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	waterQualityObserved := fiware.NewWaterQualityObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)
	waterQualityObserved.Temperature = ngsi.NewNumberProperty(*msg.Pack[1].Value)

	return waterQualityObserved, nil
}

func AirQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	airQualityObserved := fiware.NewAirQualityObserved("", 0.0, 0.0, msg.Timestamp)
	airQualityObserved.CO2 = ngsi.NewNumberProperty(*msg.Pack[1].Value)

	return airQualityObserved, nil
}
