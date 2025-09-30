package cip

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)

	messageCounter, err := otel.Meter("iot-transform-fiware/transform").Int64Counter(
		"diwise.transform.entities.total",
		metric.WithUnit("1"),
		metric.WithDescription("Total number of transformed entities"),
	)
	if err != nil {
		log.Error("failed to create otel message counter", "err", err.Error())
	}

	typeCounter, err := otel.Meter("iot-transform-fiware/transform").Int64Counter(
		fmt.Sprintf("diwise.transform.%s.total", strings.ToLower(typeName)),
		metric.WithUnit("1"),
		metric.WithDescription(fmt.Sprintf("Total number of transformed %s", strings.ToLower(typeName))),
	)
	if err != nil {
		log.Error("failed to create otel message counter", "err", err.Error())
	}

	ctx = logging.NewContextWithLogger(ctx, log, "entity_id", id, "type_name", typeName)

	return func() error {
		_, shouldMergeExistingEntity := retrieveEntity(ctx, cbClient, id)

		if shouldMergeExistingEntity {
			err := mergeEntity(ctx, cbClient, id, properties)
			if err != nil {
				log.Error("failed to merge existing entity", "err", err.Error())
				return err
			}

			messageCounter.Add(ctx, 1)
			typeCounter.Add(ctx, 1)

			log.Debug(fmt.Sprintf("merged existing %s", typeName))

			return nil
		}

		err := createNewEntity(ctx, cbClient, id, typeName, properties)
		if err != nil {
			log.Error("failed to create new entity", "err", err.Error())
			return err
		}

		messageCounter.Add(ctx, 1)
		typeCounter.Add(ctx, 1)

		log.Debug(fmt.Sprintf("created new %s", typeName), "entity_id", id)

		return nil
	}()
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

var mu sync.Mutex
var known map[string]bool = make(map[string]bool)

func retrieveEntity(ctx context.Context, cbClient client.ContextBrokerClient, id string) (string, bool) {
	mu.Lock()
	defer mu.Unlock()

	log := logging.GetFromContext(ctx)

	if _, ok := known[id]; ok {
		return id, true
	}

	headers := map[string][]string{
		"Accept": {"application/ld+json"},
		"Link":   {entities.LinkHeader},
	}

	_, err := cbClient.RetrieveEntity(ctx, id, headers)
	if err != nil {
		log.Debug("could not retrieve entity", "err", err.Error())
		delete(known, id)
		return id, false
	}

	known[id] = true

	return id, true
}
