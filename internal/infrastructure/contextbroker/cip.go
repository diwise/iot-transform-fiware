package contextbroker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	ngsilderrors "github.com/diwise/context-broker/pkg/ngsild/errors"
	"github.com/diwise/context-broker/pkg/ngsild/types/entities"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

type lockEntry struct {
	gate     chan struct{}
	refCount int
}

type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

func (kl *keyedLocks) lock(ctx context.Context, key string) (func(), error) {
	kl.mu.Lock()
	entry, ok := kl.locks[key]
	if !ok {
		entry = &lockEntry{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		kl.locks[key] = entry
	}
	entry.refCount++
	kl.mu.Unlock()

	releaseRef := func() {
		kl.mu.Lock()
		entry.refCount--
		if entry.refCount == 0 {
			delete(kl.locks, key)
		}
		kl.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	case <-entry.gate:
	}
	return func() { entry.gate <- struct{}{}; releaseRef() }, nil
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{
		locks: make(map[string]*lockEntry),
	}
}

var locks = newKeyedLocks()

// MergeOrCreateWithContext skapar eller mergar en entitet med den angivna
// JSON-LD-contexten. Tom contextURL ger bibliotekets default-context.
func MergeOrCreateWithContext(ctx context.Context, cbClient BrokerClient, id string, typeName string, contextURL string, properties []entities.EntityDecoratorFunc) error {
	if _, err := transform.BuildEntity(transform.EntityWrite{EntityID: id, TypeName: typeName, Context: contextURL, Props: properties}, ""); err != nil {
		return err
	}
	unlock, err := locks.lock(ctx, id)
	if err != nil {
		return err
	}
	defer unlock()

	log := logging.GetFromContext(ctx).With("entity_id", id, "type_name", typeName)
	ctx = logging.NewContextWithLogger(ctx, log)

	err = mergeEntityWithContext(ctx, cbClient, id, contextURL, properties)
	if err != nil && !errors.Is(err, errEntityNotFound) {
		return err
	}

	if err == nil {
		log.Debug("entity merged")
		return nil
	}

	err = createNewEntity(ctx, cbClient, id, typeName, contextURL, properties)
	if err != nil {
		if errors.Is(err, ErrEntityAlreadyExists) {
			log.Warn("entity already exists, try merging again...")
			return mergeEntityWithContext(ctx, cbClient, id, contextURL, properties)
		}

		return err
	}

	log.Debug("entity created")

	return nil
}

var ErrEntityAlreadyExists = errors.New("entity already exists")

// CreateNewEntityWithContext skapar en entitet med den angivna JSON-LD-
// contexten. Tom contextURL ger bibliotekets default-context.
func CreateNewEntityWithContext(ctx context.Context, cbClient BrokerClient, id string, typeName string, contextURL string, properties []entities.EntityDecoratorFunc) error {
	return createNewEntity(ctx, cbClient, id, typeName, contextURL, properties)
}

func createNewEntity(ctx context.Context, cbClient BrokerClient, id string, typeName string, contextURL string, properties []entities.EntityDecoratorFunc) error {
	entity, err := transform.BuildEntity(transform.EntityWrite{EntityID: id, TypeName: typeName, Context: contextURL, Props: properties}, "")
	if err != nil {
		return fmt.Errorf("failed to create new entity (entities.New): %w", err)
	}

	_, err = cbClient.CreateEntity(ctx, entity, map[string][]string{"Content-Type": {"application/ld+json"}})
	if err != nil {
		if errors.Is(err, ngsilderrors.ErrAlreadyExists) {
			return ErrEntityAlreadyExists
		}

		return err
	}

	return nil
}

var errEntityNotFound = errors.New("entity not found")

func mergeEntityWithContext(ctx context.Context, cbClient BrokerClient, id, contextURL string, properties []entities.EntityDecoratorFunc) error {
	log := logging.GetFromContext(ctx)
	if _, err := transform.BuildEntity(transform.EntityWrite{EntityID: id, TypeName: "FragmentValidation", Context: contextURL, Props: properties}, ""); err != nil {
		return err
	}
	properties = append([]entities.EntityDecoratorFunc(nil), properties...)

	if contextURL != "" {
		properties = append(properties, entities.Context([]string{contextURL}))
	} else {
		properties = append(properties, entities.DefaultContext())
	}

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

	return nil
}
