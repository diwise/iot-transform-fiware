package iottransformfiware

import (
	"context"

	"github.com/diwise/iot-transform-fiware/internal/pkg/messageprocessor"
	"github.com/rs/zerolog"
)

type IoTTransformFiware interface { 
	MessageAccepted(ctx context.Context, msg []byte) error	
}

type iotTransformFiware struct {
	mp messageprocessor.MessageProcessor
}

func (t *iotTransformFiware) MessageAccepted(ctx context.Context, msg []byte) error	{
	err := t.mp.ProcessMessage(ctx, msg)

	if (err != nil){
		return err
	}

	return nil
}

func NewIoTTransformFiware(ctx context.Context, log zerolog.Logger) IoTTransformFiware {
	msgProc := messageprocessor.NewMessageProcessor()
	
	return &iotTransformFiware { 
		mp : msgProc,
	}
}