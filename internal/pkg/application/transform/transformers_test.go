package transform

import (
	"context"
	"fmt"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
)

func TestThatWeatherObservedCanBeCreated(t *testing.T) {
	is, pack := testSetup(t, "3303", "Temperature", "", 22.2)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	e, err := WeatherObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.WeatherObserved)
	is.Equal(f.Temperature.Value, *msg.Pack[1].Value)
}

func TestThatWaterQualityObservedCanBeCreated(t *testing.T) {
	is, pack := testSetup(t, "3303", "Temperature", "water", 22.2)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	e, err := WaterQualityObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.WaterQualityObserved)
	is.Equal(f.Temperature.Value, *msg.Pack[1].Value)
}

func TestThatAirQualityObservedCanBeCreated(t *testing.T) {
	is, pack := testSetup(t, "3428", "CO2", "", 22.2)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	e, err := AirQualityObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.AirQualityObserved)
	is.Equal(f.CO2.Value, *msg.Pack[1].Value)
}

func TestThatAirQualityIsNotCreatedOnNoValidProperties(t *testing.T) {
	is, pack := testSetup(t, "3428", "", "", 0.0)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	_, err := AirQualityObserved(context.Background(), msg)

	is.True(err != nil)
}

func TestThatTimeParsesCorrectly(t *testing.T) {
	is, pack := testSetup(t, "3428", "CO2", "", 22.2)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	e, err := AirQualityObserved(context.Background(), msg)

	is.NoErr(err)
	f := e.(*fiware.AirQualityObserved)
	is.Equal(f.DateObserved.Value, "2006-01-02T15:04:05Z")
}

func testSetup(t *testing.T, typeSuffix, typeName, typeEnv string, value float64) (*is.I, senml.Pack) {
	is := is.New(t)
	var pack senml.Pack

	pack = append(pack, senml.Record{
		BaseName:    fmt.Sprintf("urn:oma:lwm2m:ext:%s", typeSuffix),
		Name:        "0",
		StringValue: "deviceID",
		BaseTime:    1136214245,
	}, senml.Record{
		Name:  typeName,
		Value: &value,
	}, senml.Record{
		Name:        "Env",
		StringValue: typeEnv,
	})

	return is, pack
}
