package messageprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestThatWeatherObservedCanBeCreatedAndPosted(t *testing.T) {
	is, log := testSetup(t)

	contextBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)		
	}))
	defer contextBroker.Close()

	contextBrokerClient := domain.NewContextBrokerClient(contextBroker.URL, log)

	msg := iotcore.NewMessageAccepted("deviceID", "temperature", "Temperature", 2).AtLocation(62.362829, 17.509804)

	mp := NewMessageProcessor(contextBrokerClient, log)
	err := mp.ProcessMessage(context.Background(), msg)

	is.NoErr(err)
}

func testSetup(t *testing.T) (*is.I,  zerolog.Logger) {
	is := is.New(t)
	return is, zerolog.Logger{}
}
