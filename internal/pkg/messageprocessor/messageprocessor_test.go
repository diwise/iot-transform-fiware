package messageprocessor

import (
	"context"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/ngsi-ld-golang/pkg/datamodels/fiware"
	"github.com/matryer/is"
)

func TestThatWeatherObservedCanBeCreated(t *testing.T) {
	is := testSetup(t)

	msg := iotcore.MessageAccepted { 
		Sensor: "xxxxxxxxxxxxxx", 
		Type: "Temperature", 
		SensorValue: 2,
	}
	
	mp := NewMessageProcessor()
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
}

func testSetup(t *testing.T) *is.I {
	is := is.New(t)
	return is;
}