package transform

import (
	"context"
	"fmt"
	"math"
	"time"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	ngsi "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/types"
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted) (any, error)

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	weatherObserved := fiware.NewWeatherObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)

	temp, ok := msg.GetFloat64("Temperature")
	if ok {
		weatherObserved.Temperature = ngsi.NewNumberProperty(temp)
	} else {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.Pack[0].BaseTime, 0.0) {
		t := parseTime(msg.Pack[0].BaseTime)
		weatherObserved.DateObserved = *ngsi.CreateDateTimeProperty(t)
	}

	return weatherObserved, nil
}

func WaterQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	waterQualityObserved := fiware.NewWaterQualityObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)

	temp, ok := msg.GetFloat64("Temperature")
	if ok {
		waterQualityObserved.Temperature = ngsi.NewNumberProperty(temp)
	} else {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.Pack[0].BaseTime, 0.0) {
		t := parseTime(msg.Pack[0].BaseTime)
		waterQualityObserved.DateObserved = *ngsi.CreateDateTimeProperty(t)
	}

	return waterQualityObserved, nil
}

func AirQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	airQualityObserved := fiware.NewAirQualityObserved("", 0.0, 0.0, msg.Timestamp)

	co2, ok := msg.GetFloat64("CO2")
	if ok {
		airQualityObserved.CO2 = ngsi.NewNumberProperty(co2)
	} else {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.Pack[0].BaseTime, 0.0) {
		t := parseTime(msg.Pack[0].BaseTime)
		airQualityObserved.DateObserved = *ngsi.NewTextProperty(t)
	}

	return airQualityObserved, nil
}

func parseTime(unixTime float64) string {

	n := int64(unixTime)
	t := time.Unix(n, 0)

	return t.UTC().Format(time.RFC3339)
}

const float64EqualityThreshold = 1e-9

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}
