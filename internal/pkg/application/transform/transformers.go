package transform

import (
	"context"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"		
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted) (Entity, error)

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted) (Entity, error) {
	
	weatherObserved := fiware.NewWeatherObserved(msg.Sensor, 0.0, 0.0, "observedAt")
	weatherObserved.Temperature.Value = msg.SensorValue

	return weatherObserved, nil
}

type Entity interface { }
