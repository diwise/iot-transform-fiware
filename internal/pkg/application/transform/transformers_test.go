package transform

import (
	"context"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	"github.com/matryer/is"
)

func TestThatWeatherObservedCanBeCreated(t *testing.T) {
	is := testSetup(t)

	msg := iotcore.NewMessageAccepted("deviceID", "temperature", "Temperature", 2).AtLocation(62.362829, 17.509804)

	e, err := WeatherObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.WeatherObserved)
	is.Equal(f.Temperature.Value, msg.SensorValue)
}

func TestThatWaterQualityObservedCanBeCreated(t *testing.T) {
	is := testSetup(t)

	msg := iotcore.NewMessageAccepted("deviceID", "temperature/water", "Temperature", 2).AtLocation(62.362829, 17.509804)

	e, err := WaterQualityObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.WaterQualityObserved)
	is.Equal(f.Temperature.Value, msg.SensorValue)
}

func testSetup(t *testing.T) *is.I {
	is := is.New(t)
	return is
}
