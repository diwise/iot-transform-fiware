package cip

import (
	"context"
	"errors"
	"sync"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

var mu sync.Mutex

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	mu.Lock()
	defer mu.Unlock()

	headers := map[string][]string{"Content-Type": {"application/ld+json"}}

	log := logging.GetFromContext(ctx)

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return err
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, headers)
	if err == nil {
		log.Debug("entity merged")
		return nil
	}

	if !errors.Is(err, ngsierrors.ErrNotFound) {
		log.Error("failed to merge entity")
		return err
	}

	properties = append(properties, entities.DefaultContext())

	entity, err := entities.New(id, typeName, properties...)
	if err != nil {
		return err
	}

	_, err = cbClient.CreateEntity(ctx, entity, headers)
	if err != nil {
		log.Error("failed to create entity")
		return err
	}

	log.Debug("entity created")

	return nil
}
