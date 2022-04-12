package messageprocessor

import (
	"context"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/pkg/infrastructure/logging"
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error
}

type messageProcessor struct {
	transformerRegistry transform.TransformerRegistry
	contextBrokerClient domain.ContextBrokerClient
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error {

	transformer := mp.transformerRegistry.DesignateTransformers(ctx, msg.Type, msg.SensorType)

	log := logging.GetFromContext(ctx)

	if transformer == nil {
		log.Info().Msgf("no transformer found for type %s, sensorType %s", msg.Type, msg.SensorType)
		return nil //TODO: should this be an error?
	}

	entity, err := transformer(ctx, msg)

	if err != nil {
		log.Error().Err(err).Msgf("unable to transform type %s", msg.Type)
		return err
	}

	err = mp.contextBrokerClient.Post(ctx, entity)

	if err != nil {
		log.Error().Err(err).Msgf("unable to upload type %s", msg.Type)
		return err
	}

	return nil
}

func NewMessageProcessor(contextBrokerClient domain.ContextBrokerClient) MessageProcessor {
	tr := transform.NewTransformerRegistry()

	return &messageProcessor{
		transformerRegistry: tr,
		contextBrokerClient: contextBrokerClient,
	}
}
