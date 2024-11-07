package cip

import (
	"context"
	"errors"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	log := logging.GetFromContext(ctx)

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err == nil {
		return nil
	}

	if !errors.Is(err, ngsierrors.ErrNotFound) {
		log.Error("error merging entity", "err", err.Error())
		return err
	}

	properties = append(properties, entities.DefaultContext())

	entity, err := entities.New(id, typeName, properties...)
	if err != nil {
		log.Error("error creating new entity", "err", err.Error())
		return err
	}

	_, err = cbClient.CreateEntity(ctx, entity, headers)
	if err != nil {
		log.Error("error creating entity", "err", err.Error())
		return err
	}

	return nil
}
