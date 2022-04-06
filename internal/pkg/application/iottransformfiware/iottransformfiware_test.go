package iottransformfiware

import (
	"context"
	"encoding/json"
	"testing"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

func TestMessageAcceptedShouldUnmarshalAndProcess(t *testing.T) {
	is, log := testSetup(t)

	m := newFakeMessageProcessor()
	i := NewIoTTransformFiware(m, log)

	msg := iotcore.MessageAccepted{
		Sensor:      "xxxxxxxxxxxxxx",
		Type:        "Temperature",
		SensorValue: 2,
	}

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

func testSetup(t *testing.T) (*is.I, zerolog.Logger) {
	is := is.New(t)
	return is, zerolog.Logger{}
}
