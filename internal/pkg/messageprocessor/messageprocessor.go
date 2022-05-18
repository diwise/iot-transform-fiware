package messageprocessor

import (
	"context"
	"fmt"

	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/domain"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error
}

type messageProcessor struct {
	transformerRegistry transform.TransformerRegistry
	contextBrokerClient domain.ContextBrokerClient
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted) error {

	sensorType := msg.BaseName()

	for _, m := range msg.Pack {
		if m.Name == "env" && m.StringValue != "" {
			sensorType = sensorType + "/" + m.StringValue
		}
	}

	transformer := mp.transformerRegistry.GetTransformerForSensorType(ctx, sensorType)

	log := logging.GetFromContext(ctx)

	if transformer == nil {
		return fmt.Errorf("no transformer found for sensorType %s", sensorType)		
	}

	entity, err := transformer(ctx, msg)

	if err != nil {
		log.Err(err).Msgf("unable to transform type %s", sensorType)
		return err
	}

	err = mp.contextBrokerClient.Post(ctx, entity)

	if err != nil {
		log.Err(err).Msgf("unable to upload type %s", sensorType)
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
