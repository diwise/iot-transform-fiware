package cip

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

var mu sync.Mutex

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	mu.Lock()
	defer mu.Unlock()

	log := logging.GetFromContext(ctx)

	if err := mergeEntity(ctx, cbClient, id, properties); err != nil {
		err := createNewEntity(ctx, cbClient, id, typeName, properties)
		if err != nil {
			return err
		}

		log.Debug("entity created", "entity_id", id, "type_name", typeName)

		return nil
	}

	log.Debug("entity merged", "entity_id", id, "type_name", typeName)

	// Sleep for a short time to allow the context broker to process the merge before any subsequent operations on the same entity
	time.Sleep(100 * time.Millisecond)

	return nil
}

func createNewEntity(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)

	properties = append(properties, entities.DefaultContext())

	entity, err := entities.New(id, typeName, properties...)
	if err != nil {
		log.Error("failed to create new entity (entities.New)", "err", err.Error())
		return err
	}

	_, err = cbClient.CreateEntity(ctx, entity, map[string][]string{
		"Content-Type": {"application/ld+json"},
	})
	if err != nil {
		log.Error("failed to create entity (cbClient.CreateEntity)", "err", err.Error())
		return err
	}

	return nil
}

func mergeEntity(ctx context.Context, cbClient client.ContextBrokerClient, id string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		log.Error("failed to create entity fragment", "err", err.Error())
		return fmt.Errorf("failed to create entity fragment: %w", err)
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, map[string][]string{
		"Content-Type": {"application/ld+json"},
	})
	if err != nil {
		log.Error("failed to merge entity", "err", err.Error())
		return err
	}

	return nil
}
