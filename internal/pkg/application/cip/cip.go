package cip

import (
	"context"
	"fmt"
	"sync"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

var mu sync.Mutex
var known map[string]bool = make(map[string]bool)

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	mu.Lock()
	defer mu.Unlock()

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return fmt.Errorf("failed to create entity fragment: %w", err)
	}

	log := logging.GetFromContext(ctx).With("entity_id", id, "entity_type", typeName)

	var merge bool

	if _, ok := known[id]; ok {
		merge = true
	} else {
		_, err = cbClient.RetrieveEntity(ctx, id, map[string][]string{
			"Accept": {"application/ld+json"},
			"Link":   {entities.LinkHeader},
		})
		if err == nil {
			merge = true
			known[id] = true
		} else {
			log.Debug("entity not found, will create a new one")
		}
	}

	if merge {
		_, err = cbClient.MergeEntity(ctx, id, fragment, map[string][]string{
			"Content-Type": {"application/ld+json"},
		})
		if err != nil {
			log.Error("failed to merge entity", "error", err)
			return err
		}

		log.Debug("entity merged successfully")
		return nil
	}

	properties = append(properties, entities.DefaultContext())

	entity, err := entities.New(id, typeName, properties...)
	if err != nil {
		return fmt.Errorf("failed to create new entity: %w", err)
	}

	_, err = cbClient.CreateEntity(ctx, entity, map[string][]string{
		"Content-Type": {"application/ld+json"},
	})
	if err != nil {
		return fmt.Errorf("failed to create entity: %w", err)
	}

	log.Debug("entity created successfully")

	known[id] = true

	return nil
}
