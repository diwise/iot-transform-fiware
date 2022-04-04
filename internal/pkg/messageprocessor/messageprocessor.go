package messageprocessor

import (
	"context"
	"encoding/json"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"

	"github.com/rs/zerolog/log"
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg []byte) error
}

type messageProcessor struct {
	tReg transform.TransformerRegistry
	cbClient domain.ContextBrokerClient
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg []byte) error {
	ma := &iotcore.MessageAccepted{}
	err := json.Unmarshal(msg, ma)

	if err != nil {
		log.Err(err).Msgf("unable to unmarshal MessageAccepted")
		return err
	}

	transformer := mp.tReg.DesignateTransformers(ctx, ma.Type)

	if transformer == nil {
		log.Info().Msgf("no transformer found for type %s", ma.Type)
		return nil
	}

	entity, err := transformer(ctx, msg)

	if (err != nil){
		log.Err(err).Msgf("unable to transform type %s", ma.Type)
		return nil
	}

	err := mp.cbClient.Post(entity)	

	if (err != nil){
		log.Err(err).Msgf("unable to upload type %s", ma.Type)
		return nil
	}

	return nil
}

func NewMessageProcessor() MessageProcessor {
	tr := transform.NewTransformerRegistry()

	return &messageProcessor{
		tReg: tr,
	}
}
