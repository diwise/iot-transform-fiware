package iottransformfiware

import (
	"context"

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
	err := t.messageProcessor.ProcessMessage(ctx, msg)

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