package things

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/matryer/is"
)

const reloadRulesA = `
rules:
  - match: { kind: thing, type: RoomA }
    entities:
      - id: "urn:ngsi-ld:Room:{{nameOrID}}"
        type: Room
        properties:
          - { target: name, type: Text, field: name, observedAt: none }
`

const reloadRulesB = `
rules:
  - match: { kind: thing, type: RoomB }
    entities:
      - id: "urn:ngsi-ld:Room:{{nameOrID}}"
        type: Room
        properties:
          - { target: name, type: Text, field: name, observedAt: none }
`

func reloadThing(t *testing.T, id, typ string) []byte {
	t.Helper()
	return []byte(`{"thing":{"id":"` + id + `","type":"` + typ + `","name":"n","tenant":"t"},"tenant":"t","timestamp":"2024-01-01T00:00:00Z"}`)
}

// Reload byter reglmängd atomärt: gamla regler slutar matcha, nya börjar,
// utan fel eller partiellt tillstånd.
func TestReloadSwapsRuleSet(t *testing.T) {
	is := is.New(t)

	rulesA, err := config.ParseModel([]byte(reloadRulesA))
	is.NoErr(err)
	rulesB, err := config.ParseModel([]byte(reloadRulesB))
	is.NoErr(err)

	d := NewDynamicThingTransformer(rulesA)
	th, err := ThingFromJSON(reloadThing(t, "a-1", "RoomA"))
	is.NoErr(err)
	writes, err := d.Transform(th)
	is.NoErr(err)
	is.Equal(len(writes), 1)

	d.Reload(rulesB)

	writes, err = d.Transform(th)
	if !errors.Is(err, transform.ErrNoMatchingRule) {
		t.Fatalf("old rule must stop matching, got %d writes err %v", len(writes), err)
	}

	thB, err := ThingFromJSON(reloadThing(t, "b-1", "RoomB"))
	is.NoErr(err)
	writes, err = d.Transform(thB)
	is.NoErr(err)
	is.Equal(len(writes), 1)
}

// Samtidig Transform + Reload ska aldrig ge datarace eller partiellt
// tillstånd (körs med -race). Varje Transform ser en komplett gammal
// eller ny motor: exakt 0 eller 1 skrivningar, aldrig fel.
func TestReloadConcurrentWithTransform(t *testing.T) {
	rulesA, err := config.ParseModel([]byte(reloadRulesA))
	if err != nil {
		t.Fatalf("parse A: %v", err)
	}
	rulesB, err := config.ParseModel([]byte(reloadRulesB))
	if err != nil {
		t.Fatalf("parse B: %v", err)
	}

	d := NewDynamicThingTransformer(rulesA)
	th, err := ThingFromJSON(reloadThing(t, "a-1", "RoomA"))
	if err != nil {
		t.Fatalf("envelope: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				writes, err := d.Transform(th)
				if errors.Is(err, transform.ErrNoMatchingRule) {
					continue // omladdad motor utan match: giltigt skip
				}
				if err != nil {
					errs <- err
					return
				}
				if len(writes) != 1 {
					errs <- fmt.Errorf("unexpected writes count %d", len(writes))
					return
				}
			}
		}()
		go func(n int) {
			defer wg.Done()
			if n%2 == 0 {
				d.Reload(rulesB)
			} else {
				d.Reload(rulesA)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent transform: %v", err)
	}
}
