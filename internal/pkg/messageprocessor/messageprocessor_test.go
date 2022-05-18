package messageprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/diwise/iot-core/pkg/measurements"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestThatWeatherObservedCanBeCreatedAndPosted(t *testing.T) {
	is, log, pack := testSetup(t, func() senml.Pack {
		var pack senml.Pack
		val := 22.2				
		pack = append(pack, senml.Record{
			BaseName:    "urn:oma:lwm2m:ext:3303/air",
			Name:        "0",
			StringValue: "deviceID",
		}, senml.Record{
			Name:  "Temperature",
			Value: &val,
		})
		return pack
	})
	var entityWasPosted bool = false

	contextBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entityWasPosted = true
		w.WriteHeader(201)
	}))
	defer contextBroker.Close()

	contextBrokerClient := domain.NewContextBrokerClient(contextBroker.URL, log)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(contextBrokerClient)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
	is.True(entityWasPosted) // expected a request to mock context broker
}

func TestThatLifeBouyCanBeCreatedAndPosted(t *testing.T) {
	is, log, pack := testSetup(t, func() senml.Pack {
		var pack senml.Pack
		val := true				
		pack = append(pack, senml.Record{
			BaseName:    "urn:oma:lwm2m:ext:3302/lifebuoy",
			Name:        "0",
			StringValue: "deviceID",
		}, senml.Record{
			Name:  measurements.Presence,
			BoolValue: &val,
		})
		return pack
	})
	var entityWasPosted bool = false

	contextBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entityWasPosted = true
		w.WriteHeader(201)
	}))
	defer contextBroker.Close()

	contextBrokerClient := domain.NewContextBrokerClient(contextBroker.URL, log)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(contextBrokerClient)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
	is.True(entityWasPosted) // expected a request to mock context broker
}

func testSetup(t *testing.T, fn func() senml.Pack) (*is.I, zerolog.Logger, senml.Pack) {
	is := is.New(t)
	pack := fn()
	return is, zerolog.Logger{}, pack
}
