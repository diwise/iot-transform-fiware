package iottransformfiware

import (
	"context"
	"encoding/json"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"

	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"github.com/rs/zerolog"
)

type IoTTransformFiware interface {
	MessageAccepted(ctx context.Context, msg []byte) error
}

type iotTransformFiware struct {
	messageProcessor messageprocessor.MessageProcessor
}

func (t *iotTransformFiware) MessageAccepted(ctx context.Context, msg []byte) error {

	ma := iotcore.MessageAccepted{}
	err := json.Unmarshal(msg, &ma)

	if err != nil {
		log := logging.GetFromContext(ctx)
		log.Error().Err(err).Msg("unable to unmarshal incoming message")
		return err
	}

	return t.messageProcessor.ProcessMessage(ctx, ma)
}

func NewIoTTransformFiware(m messageprocessor.MessageProcessor, log zerolog.Logger) IoTTransformFiware {
	return &iotTransformFiware{
		messageProcessor: m,
	}
}
