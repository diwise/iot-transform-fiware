package features

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/diwise/context-broker/pkg/datamodels/fiware"
	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities/decorators"
	"github.com/diwise/iot-transform-fiware/internal/pkg/application/transform"
	"github.com/diwise/messaging-golang/pkg/messaging"
	"github.com/rs/zerolog"

	amqp "github.com/rabbitmq/amqp091-go"
)

type waterQuality struct {
	Temperature float64 `json:"temperature"`
}

type counter struct {
	Count int  `json:"count"`
	State bool `json:"state"`
}

type level struct {
	Current float64  `json:"current"`
	Percent *float64 `json:"percent,omitempty"`
	Offset  *float64 `json:"offset,omitempty"`
}

type presence struct {
	State bool `json:"state"`
}

type location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type feat struct {
	ID       string    `json:"id"`
	Type     string    `json:"type"`
	SubType  string    `json:"subtype"`
	Location *location `json:"location,omitempty"`
	Tenant   string    `json:"tenant,omitempty"`

	Counter      *counter      `json:"counter,omitempty"`
	Level        *level        `json:"level,omitempty"`
	Presence     *presence     `json:"presence,omitempty"`
	WaterQuality *waterQuality `json:"waterQuality"`
}

func TopicMessageHandler(messenger messaging.MsgContext, contextBrokerClientUrl string) messaging.TopicMessageHandler {
	return func(ctx context.Context, msg amqp.Delivery, logger zerolog.Logger) {
		feature := feat{}

		err := json.Unmarshal(msg.Body, &feature)
		if err != nil {
			logger.Error().Err(err).Msgf("failed to unmarshal message body")
			return
		}

		cbClient := client.NewContextBrokerClient(contextBrokerClientUrl, client.Tenant(feature.Tenant))

		switch feature.Type {
		case "waterQuality":
			WaterQualityObserved(ctx, feature, cbClient)
		default:
			logger.Debug().Msgf("unknown feature type: %s", feature.Type)
		}

	}
}

func WaterQualityObserved(ctx context.Context, feature feat, cbClient client.ContextBrokerClient) error {
	properties := make([]entities.EntityDecoratorFunc, 0, 5)

	id := fmt.Sprintf("%s%s:%s:%s", fiware.WaterQualityObservedIDPrefix, feature.SubType, feature.Type, feature.ID)

	properties = append(properties,
		decorators.DateObserved(time.Now().UTC().Format(time.RFC3339Nano)),
		transform.Temperature(feature.WaterQuality.Temperature, time.Now()),
	)

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err != nil {
		if !errors.Is(err, ngsierrors.ErrNotFound) {
			return fmt.Errorf("failed to merge entity %s, %w", id, err)
		}

		if feature.Location != nil {
			properties = append(properties, decorators.Location(feature.Location.Latitude, feature.Location.Longitude))
		}

		properties = append(properties, entities.DefaultContext())

		wqo, err := entities.New(id, fiware.WaterQualityObservedTypeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, wqo, headers)
		if err != nil {
			return fmt.Errorf("failed to create entity %s, %w", id, err)
		}
	}
	return nil
}
