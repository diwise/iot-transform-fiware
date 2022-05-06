package messageprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestThatWeatherObservedCanBeCreatedAndPosted(t *testing.T) {
	is, log, pack := testSetup(t)

	contextBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
	}))
	defer contextBroker.Close()

	contextBrokerClient := domain.NewContextBrokerClient(contextBroker.URL, log)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(contextBrokerClient)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
}

func testSetup(t *testing.T) (*is.I, zerolog.Logger, senml.Pack) {
	is := is.New(t)
	var pack senml.Pack

	pack = append(pack, senml.Record{
		BaseName:    "urn:oma:lwm2m:ext:3303",
		Name:        "0",
		StringValue: "deviceID",
	})

	return is, zerolog.Logger{}, pack
}
