package iottransformfiware

import (
	"context"
	"encoding/json"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"

	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/rs/zerolog"
)

type IoTTransformFiware interface { 
	MessageAccepted(ctx context.Context, msg []byte) error	
}

type iotTransformFiware struct {
	messageProcessor messageprocessor.MessageProcessor
	log zerolog.Logger
}

func (t *iotTransformFiware) MessageAccepted(ctx context.Context, msg []byte) error	{
	
	ma := iotcore.MessageAccepted{}
	err := json.Unmarshal(msg, &ma)

	if err != nil {
		t.log.Err(err).Msgf("unable to unmarshal MessageAccepted")
		return err
	}
	
	err = t.messageProcessor.ProcessMessage(ctx, ma)

	if (err != nil){
		return err
	}

	return nil
}

func NewIoTTransformFiware(contextBrokerClient domain.ContextBrokerClient, log zerolog.Logger) IoTTransformFiware {
	messageProcessor := messageprocessor.NewMessageProcessor(contextBrokerClient, log)
	
	return &iotTransformFiware { 
		messageProcessor : messageProcessor,
		log: log,
	}
}