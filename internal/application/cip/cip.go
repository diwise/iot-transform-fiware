package cip

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/diwise/context-broker/pkg/ngsild/client"
	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type lockEntry struct {
	mu       sync.Mutex
	refCount int
}

type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

func (kl *keyedLocks) lock(key string) func() {
	kl.mu.Lock()
	entry, ok := kl.locks[key]
	if !ok {
		entry = &lockEntry{}
		kl.locks[key] = entry
	}
	entry.refCount++
	kl.mu.Unlock()

	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()

		kl.mu.Lock()
		entry.refCount--
		if entry.refCount == 0 {
			delete(kl.locks, key)
		}
		kl.mu.Unlock()
	}
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{
		locks: make(map[string]*lockEntry),
	}
}

var locks = newKeyedLocks()

func MergeOrCreate(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	unlock := locks.lock(id)
	defer unlock()

	log := logging.GetFromContext(ctx).With("entity_id", id, "type_name", typeName)
	ctx = logging.NewContextWithLogger(ctx, log)

	err := mergeEntity(ctx, cbClient, id, properties)
	if err != nil && !errors.Is(err, errEntityNotFound) {
		return err
	}

	if err == nil {
		log.Debug("entity merged")
		return nil
	}

	err = createNewEntity(ctx, cbClient, id, typeName, properties)
	if err != nil {
		if errors.Is(err, errEntityAlreadyExists) {
			log.Warn("entity already exists, try merging again...")
			return mergeEntity(ctx, cbClient, id, properties)
		}

		return err
	}

	log.Debug("entity created")

	return nil
}

func CreateIfNotExists(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	unlock := locks.lock(id)
	defer unlock()

	log := logging.GetFromContext(ctx).With("entity_id", id, "type_name", typeName)
	ctx = logging.NewContextWithLogger(ctx, log)

	exists := checkIfEntityExists(ctx, cbClient, id)
	if !exists {
		log.Info("entity does not exist")
		err := createNewEntity(ctx, cbClient, id, typeName, properties)
		if err != nil {
			return err
		}

		log.Info("entity created")
	}

	log.Info("entity already exists, will not attempt to create")

	return nil
}

var errEntityAlreadyExists = errors.New("entity already exists")

func createNewEntity(ctx context.Context, cbClient client.ContextBrokerClient, id string, typeName string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)

	properties = append(properties, entities.DefaultContext())

	entity, err := entities.New(id, typeName, properties...)
	if err != nil {
		return fmt.Errorf("failed to create new entity (entities.New): %w", err)
	}

	_, err = cbClient.CreateEntity(ctx, entity, map[string][]string{"Content-Type": {"application/ld+json"}})
	if err != nil {
		if errors.Is(err, ngsilderrors.ErrAlreadyExists) {
			return errEntityAlreadyExists
		}

		log.Error("failed to create entity", "err", err.Error())

		return err
	}

	return nil
}

var errEntityNotFound = errors.New("entity not found")

func mergeEntity(ctx context.Context, cbClient client.ContextBrokerClient, id string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)

	fragment, err := entities.NewFragment(properties...)
	if err != nil {
		return fmt.Errorf("failed to create entity fragment: %w", err)
	}

	_, err = cbClient.MergeEntity(ctx, id, fragment, map[string][]string{"Content-Type": {"application/ld+json"}})
	if err != nil {
		if errors.Is(err, ngsilderrors.ErrNotFound) {
			return errEntityNotFound
		}

		log.Error("failed to merge entity", "err", err.Error())

		return err
	}

	time.Sleep(100 * time.Millisecond) // give the context broker some time to process the merge before any subsequent operations

	return nil
}

func checkIfEntityExists(ctx context.Context, cbClient client.ContextBrokerClient, id string) bool {
	log := logging.GetFromContext(ctx)

	result, err := cbClient.RetrieveEntity(ctx, id, map[string][]string{"Content-Type": {"application/ld+json"}, "Link": {`<https://raw.githubusercontent.com/diwise/context-broker/main/assets/jsonldcontexts/default-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"`}})
	if err != nil && errors.Is(err, ngsilderrors.ErrNotFound) {
		log.Info(fmt.Sprintf("entity with id %s does not exist", id))
		return false
	} else if err != nil {
		log.Error("failed to retrieve entity", "err", err.Error())
		return true
	}

	if result != nil && result.ID() == id {
		log.Info(fmt.Sprintf("entity with id %s exists", id))
		return true
	}

	return false
}
