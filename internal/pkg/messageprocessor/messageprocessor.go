package messageprocessor

import (
	"context"
	"fmt"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	iotcore "github.com/diwise/iot-core/pkg/messaging/events"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type MessageProcessor interface {
	ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted, contextBrokerClient client.ContextBrokerClient) error
}

type messageProcessor struct {
	transformerRegistry transform.TransformerRegistry
}

func (mp *messageProcessor) ProcessMessage(ctx context.Context, msg iotcore.MessageAccepted, contextBrokerClient client.ContextBrokerClient) error {
	log := logging.GetFromContext(ctx)

	sensorType := msg.BaseName()

	for _, m := range msg.Pack {
		if m.Name == "env" && m.StringValue != "" {
			sensorType = sensorType + "/" + m.StringValue
		}
	}

	transformer := mp.transformerRegistry.GetTransformerForSensorType(ctx, sensorType)

	if transformer == nil {
		return fmt.Errorf("no transformer found for sensorType %s", sensorType)
	}

	err := transformer(ctx, msg, contextBrokerClient)

	if err != nil {
		log.Err(err).Msgf("unable to transform type %s", sensorType)
		return err
	}

	return nil
}

func NewMessageProcessor() MessageProcessor {
	tr := transform.NewTransformerRegistry()

	return &messageProcessor{
		transformerRegistry: tr,
	}
}
