package messageprocessor

import (
	"context"
	"encoding/json"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"

	"github.com/rs/zerolog"	
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg []byte) error
}

type messageProcessor struct {
	transformerRegistry transform.TransformerRegistry
	contextBrokerClient domain.ContextBrokerClient
	log zerolog.Logger
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg []byte) error {
	ma := &iotcore.MessageAccepted{}
	err := json.Unmarshal(msg, ma)

	if err != nil {
		mp.log.Err(err).Msgf("unable to unmarshal MessageAccepted")
		return err
	}

	transformer := mp.transformerRegistry.DesignateTransformers(ctx, ma.Type)

	if transformer == nil {
		mp.log.Info().Msgf("no transformer found for type %s", ma.Type)
		return nil // hmm, detta blir inte bra.
	}

	entity, err := transformer(ctx, msg)

	if (err != nil){
		mp.log.Err(err).Msgf("unable to transform type %s", ma.Type)
		return err
	}

	err = mp.contextBrokerClient.Post(ctx, entity)

	if (err != nil){
		mp.log.Err(err).Msgf("unable to upload type %s", ma.Type)
		return err
	}

	return nil
}

func NewMessageProcessor(contextBrokerClient domain.ContextBrokerClient, log zerolog.Logger) MessageProcessor {
	tr := transform.NewTransformerRegistry()

	return &messageProcessor{
		transformerRegistry: tr,
		contextBrokerClient: contextBrokerClient,
		log: log,
	}
}
