package api

import (
	"context"
	"log/slog"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/measurements"
	"github.com/diwise/iot-transform-fiware/internal/application/things"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
)

// RuleStore är den persistensyta Models behöver. *rules.PgStore uppfyller
// den; tester stoppar in stubbar för felinjicering.
type RuleStore interface {
	LoadRules(ctx context.Context) ([]rules.StoredRule, int64, error)
	ListRules() ([]rules.StoredRule, error)
	GetRule(id string) (rules.StoredRule, error)
	InsertRule(r transform.Rule) (rules.StoredRule, error)
	UpdateRule(id string, r transform.Rule, rev int64) (rules.StoredRule, error)
	DeleteRule(id string, rev int64) error
	RuleVersion() (int64, error)
}

// Snapshot är en oföränderlig regeluppsättning med förbyggda motorer.
// Meddelandehantering läser alltid en komplett snapshot: saker och
// mätvärden kan aldrig se olika regelversioner (TF-05).
type Snapshot struct {
	Version      int64
	Things       *things.DynamicThingTransformer
	Measurements *measurements.DynamicTransformer
}

// buildSnapshot bygger motorer från lagrade rader. Anropas med komplett
// radmängd; resultatet publiceras atomärt av anroparen.
func buildSnapshot(stored []rules.StoredRule, resolver measurements.SensorTypeResolver) *Snapshot {
	rs := make([]transform.Rule, 0, len(stored))
	for _, st := range stored {
		rs = append(rs, st.Rule)
	}
	return &Snapshot{
		Things:       things.NewDynamicThingTransformer(rs),
		Measurements: measurements.NewDynamicTransformer(rs, resolver),
	}
}

// SyncOnce laddar om snapshoten om lagrets version ändrats. Idempotent:
// utan ändring händer inget. Fel propageras; anroparen (mutate, RunSync)
// avgör återhämtning.
func (m *Models) SyncOnce() error {
	v, err := m.store.RuleVersion()
	if err != nil {
		return err
	}
	if snap := m.snap.Load(); snap != nil && snap.Version == v {
		return nil
	}
	return m.Refresh()
}

// Refresh bygger om snapshoten från lagret villkorslöst och byter den
// atomärt. Används vid start, efter egna mutationer och vid poll-träff.
func (m *Models) Refresh() error {
	stored, v, err := m.store.LoadRules(context.Background())
	if err != nil {
		return err
	}
	snap := buildSnapshot(stored, m.resolver)
	snap.Version = v
	for {
		current := m.snap.Load()
		if current != nil && current.Version >= v {
			return nil
		}
		if m.snap.CompareAndSwap(current, snap) {
			break
		}
	}
	return nil
}

// Snapshot returnerar aktuell snapshot (aldrig nil; före första Refresh
// en tom oladdad snapshot som inte matchar någon lagerversion).
func (m *Models) Snapshot() *Snapshot {
	return m.snap.Load()
}

// ThingsTransformer löser aktuell snapshot-motor per meddelande.
// Uppfyller messaging-things TransformerProvider.
func (m *Models) ThingsTransformer() *things.DynamicThingTransformer {
	return m.Snapshot().Things
}

// MeasurementsTransformer löser aktuell snapshot-motor per meddelande.
// Uppfyller messaging-measurements TransformerProvider.
func (m *Models) MeasurementsTransformer() *measurements.DynamicTransformer {
	return m.Snapshot().Measurements
}

// RunSync pollar lagrets version och laddar om vid ändring, så att alla
// instanser konvergerar mot samma regler utan omstart. Fel loggas och
// nästa tick försöker igen (återhämtning). Returnerar stoppfunktion.
func (m *Models) RunSync(ctx context.Context, interval time.Duration, log *slog.Logger) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				before := int64(-1)
				if snap := m.snap.Load(); snap != nil {
					before = snap.Version
				}
				if err := m.SyncOnce(); err != nil {
					log.Warn("rule sync failed, retrying", "err", err.Error())
					continue
				}
				if snap := m.snap.Load(); snap != nil && snap.Version != before {
					log.Info("rule snapshot reloaded", "version", snap.Version)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
