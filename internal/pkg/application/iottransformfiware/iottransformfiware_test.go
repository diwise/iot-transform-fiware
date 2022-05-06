package iottransformfiware

import (
	"context"
	"encoding/json"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/farshidtz/senml/v2"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestMessageAcceptedShouldUnmarshalAndProcess(t *testing.T) {
	is, log, pack := testSetup(t)

	m := newFakeMessageProcessor()
	i := NewIoTTransformFiware(m, log)

	msg := iotcore.NewMessageAccepted("deviceID", pack).AtLocation(62.362829, 17.509804)

	messageBytes, _ := json.MarshalIndent(msg, "", " ")

	err := i.MessageAccepted(context.Background(), messageBytes)

	is.NoErr(err)
	is.Equal(1, m.(*fakeMessageProcessor).count)
}

type fakeMessageProcessor struct {
	count int
}

func (mp *fakeMessageProcessor) ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error {
	mp.count++
	return nil
}

func newFakeMessageProcessor() messageprocessor.MessageProcessor {
	return &fakeMessageProcessor{
		count: 0,
	}
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
