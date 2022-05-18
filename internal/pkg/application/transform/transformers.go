package transform

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	lwm2m "github.com/diwise/iot-core/pkg/lwm2m"
	measurements "github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	diwise "github.com/diwise/ngsi-ld-golang/pkg/datamodels/diwise"
	fiware "github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	geojson "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/geojson"
	ngsi "github.com/diwise/ngsi-ld-golang/pkg/ngsi-ld/types"
)

type MessageTransformerFunc func(ctx context.Context, msg iotcore.MessageAccepted) (any, error)

func WeatherObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	weatherObserved := fiware.NewWeatherObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)

	temp, ok := msg.GetFloat64(measurements.Temperature)
	if ok {
		weatherObserved.Temperature = ngsi.NewNumberProperty(temp)
	} else {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.BaseTime(), 0.0) {
		t := parseTime(msg.BaseTime())
		weatherObserved.DateObserved = *ngsi.CreateDateTimeProperty(t)
	}

	return weatherObserved, nil
}

func WaterQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	waterQualityObserved := fiware.NewWaterQualityObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)

	temp, ok := msg.GetFloat64(measurements.Temperature)
	if ok {
		waterQualityObserved.Temperature = ngsi.NewNumberProperty(temp)
	} else {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.BaseTime(), 0.0) {
		t := parseTime(msg.BaseTime())
		waterQualityObserved.DateObserved = *ngsi.CreateDateTimeProperty(t)
	}

	return waterQualityObserved, nil
}

func AirQualityObserved(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	airQualityObserved := fiware.NewAirQualityObserved("", msg.Latitude(), msg.Longitude(), msg.Timestamp)

	temp, tempOk := msg.GetFloat64(measurements.Temperature)
	if tempOk {
		airQualityObserved.Temperature = ngsi.NewNumberProperty(temp)
	}
	co2, co2Ok := msg.GetFloat64(measurements.CO2)
	if co2Ok {
		airQualityObserved.CO2 = ngsi.NewNumberProperty(co2)
	}

	if !tempOk && !co2Ok {
		return nil, fmt.Errorf("no relevant properties were found in message from %s, ignoring", msg.Sensor)
	}

	if !almostEqual(msg.BaseTime(), 0.0) {
		t := parseTime(msg.BaseTime())
		airQualityObserved.DateObserved = *ngsi.NewTextProperty(t)
	}

	return airQualityObserved, nil
}

func Device(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {
	var device *fiware.Device

	if strings.EqualFold(msg.BaseName(), lwm2m.Presence) {
		if v, ok := msg.GetBool(measurements.Presence); ok {
			if v {
				device = fiware.NewDevice(msg.Sensor, "on")
			} else {
				device = fiware.NewDevice(msg.Sensor, "off")
			}
		}
	} else {
		return nil, fmt.Errorf("unable to create Device for deviceID %s", msg.Sensor)
	}

	device.DateCreated = ngsi.CreateDateTimeProperty(msg.Timestamp)

	if msg.IsLocated() {
		device.Location = geojson.CreateGeoJSONPropertyFromWGS84(msg.Longitude(), msg.Latitude())
	}

	return device, nil
}

func Lifebuoy(ctx context.Context, msg iotcore.MessageAccepted) (any, error) {

	var lifebuoy *diwise.Lifebuoy

	if v, ok := msg.GetBool(measurements.Presence); ok {
		if v {
			lifebuoy = diwise.NewLifebuoy(msg.Sensor, "on")
		} else {
			lifebuoy = diwise.NewLifebuoy(msg.Sensor, "off")
		}
	} else {
		return nil, fmt.Errorf("unable to create lifebuoy, ignoring %s", msg.Sensor)
	}

	lifebuoy.DateObserved = ngsi.CreateDateTimeProperty(msg.Timestamp)

	if msg.IsLocated() {
		lifebuoy.Location = *geojson.CreateGeoJSONPropertyFromWGS84(msg.Longitude(), msg.Latitude())
	}

	return lifebuoy, nil
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
