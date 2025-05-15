package cip

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

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

	log := logging.GetFromContext(ctx)

	var merge bool

	if _, ok := known[id]; ok {
		merge = true
	} else {
		e, err := cbClient.RetrieveEntity(ctx, id, map[string][]string{
			"Accept": {"application/ld+json"},
			"Link":   {entities.LinkHeader},
		})
		if err != nil {
			log.Debug("could not retrieve entity", "err", err.Error())
			merge = false
			delete(known, id)
		}
		if err == nil && e != nil && e.ID() == id {
			merge = true
			known[id] = true
			log.Debug("retrieved entity from context-broker, add to cache", slog.Bool("merge", merge))
		}
		
		time.Sleep(100*time.Millisecond) // give the context-broker a break...
	}

	if merge {		
		_, err = cbClient.MergeEntity(ctx, id, fragment, map[string][]string{
			"Content-Type": {"application/ld+json"},
		})
		if err != nil {
			log.Error("failed to merge entity", "err", err.Error())
			return err
		}

		log.Debug("entity merged successfully")
		return nil
	}
	
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
		log.Error("failed to create new entity in context-broker", "err", err.Error())
		return err
	}

	log.Debug("entity created successfully")

	known[id] = true

	return nil
}
