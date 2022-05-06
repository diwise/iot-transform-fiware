package messageprocessor

import (
	"context"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"

	"github.com/rs/zerolog"
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error
}

type messageProcessor struct {
	transformerRegistry transform.TransformerRegistry
	contextBrokerClient domain.ContextBrokerClient
	log                 zerolog.Logger
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error {

	sensorType := msg.Pack[0].BaseName

	for _, m := range msg.Pack {
		if m.Name == "Env" && m.StringValue != "" {
			sensorType = sensorType + "/" + m.StringValue
		}
	}

	transformer := mp.transformerRegistry.DesignateTransformers(ctx, sensorType)

	if transformer == nil {
		mp.log.Info().Msgf("no transformer found for sensor %s, sensorType %s", msg.Sensor, msg.Pack[0].BaseName)
		return nil //TODO: should this be an error?
	}

	entity, err := transformer(ctx, msg)

	if err != nil {
		mp.log.Err(err).Msgf("unable to transform type %s", msg.Pack[0].BaseName)
		return err
	}

	err = mp.contextBrokerClient.Post(ctx, entity)

	if err != nil {
		mp.log.Err(err).Msgf("unable to upload type %s", msg.Pack[0].BaseName)
		return err
	}

	return nil
}

func NewMessageProcessor(contextBrokerClient domain.ContextBrokerClient, log zerolog.Logger) MessageProcessor {
	tr := transform.NewTransformerRegistry()

	return &messageProcessor{
		transformerRegistry: tr,
		contextBrokerClient: contextBrokerClient,
		log:                 log,
	}
}
