package cip

import (
	"context"
	"errors"
	"fmt"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsierrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
)

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
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

		properties = append(properties, entities.DefaultContext())

		entity, err := entities.New(id, typeName, properties...)
		if err != nil {
			return err
		}

		_, err = cbClient.CreateEntity(ctx, entity, headers)
		if err != nil {
			return fmt.Errorf("failed to create entity %s, %w", id, err)
		}
	}

	return nil
}
