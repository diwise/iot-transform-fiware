// rebuild-relations is a local administrative tool, not an HTTP endpoint.
// It never calls the broker. -apply only enqueues work for the normal worker.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diwise/iot-transform-fiware/internal/application/relations"
	"github.com/diwise/iot-transform-fiware/internal/application/transform"
	modelconfig "github.com/diwise/iot-transform-fiware/internal/infrastructure/config"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/projection"
	"github.com/diwise/iot-transform-fiware/internal/infrastructure/rules"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	fs := flag.NewFlagSet("rebuild-relations", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "required: exactly one source tenant")
	relation := fs.String("relation", "", "optional source relation name (rebuild only)")
	importPath := fs.String("import", "", "complete authoritative Link[] JSON snapshot for ONE tenant")
	thingsExport := fs.String("things-export", "", "complete GET /api/v1/things/export ObjectSpec[] JSON snapshot")
	quiesced := fs.Bool("quiesced", false, "confirm source writers and consumers paused and old outbox drained (required for import)")
	allowEmpty := fs.Bool("allow-empty", false, "explicitly allow an authoritative import that removes ALL tenant links")
	apply := fs.Bool("apply", false, "enqueue/reconcile; default is read-only planning")
	rulesDir := fs.String("models-dir", "", "preview candidate YAML rules instead of DB snapshot (dry run only)")
	timeout := fs.Duration("timeout", time.Minute, "overall operation budget")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *thingsExport != "" && *importPath != "" {
		return errors.New("choose either -import or -things-export")
	}
	importing := *thingsExport != "" || *importPath != ""
	if fs.NArg() != 0 || *tenant == "" || *timeout <= 0 {
		return errors.New("explicit -tenant and positive -timeout required; no positional arguments")
	}
	if importing && *apply && !*quiesced {
		return errors.New("authoritative import requires -quiesced: stop writers/consumers and drain source outbox first")
	}
	if importing && *relation != "" {
		return errors.New("import replaces the complete tenant, not one relation; omit -relation")
	}
	if *apply && *rulesDir != "" {
		return errors.New("-apply uses installed DB rules only; -models-dir is dry-run only")
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	get := func(k, fallback string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return fallback
	}
	if os.Getenv("POSTGRES_HOST") == "" || os.Getenv("POSTGRES_USER") == "" {
		return errors.New("POSTGRES_HOST and POSTGRES_USER must be explicit")
	}
	cfg := rules.NewConfig(os.Getenv("POSTGRES_HOST"), os.Getenv("POSTGRES_USER"), os.Getenv("POSTGRES_PASSWORD"), get("POSTGRES_PORT", "5432"), get("POSTGRES_DBNAME", "diwise"), get("POSTGRES_SSLMODE", "disable"))
	pool, err := pgxpool.New(ctx, cfg.ConnStr())
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	store := projection.NewForPool(pool)
	if err := store.IsReady(ctx); err != nil {
		return fmt.Errorf("projection schema must be migrated by the service first: %w", err)
	}
	stored, version, err := rules.ReadSnapshot(ctx, pool)
	if err != nil {
		return fmt.Errorf("load rule snapshot: %w", err)
	}
	configured := make([]transform.Rule, 0, len(stored))
	for _, st := range stored {
		r := st.Rule
		r.ID, r.Revision, r.SnapshotVersion = st.ID, st.Revision, version
		configured = append(configured, r)
	}
	if *rulesDir != "" {
		configured, err = modelconfig.LoadModelsDir(*rulesDir)
		if err != nil {
			return err
		}
		version = -1 // explicitly candidate, never an installed snapshot
	}
	if err := transform.Validate(configured); err != nil {
		return err
	}
	engine := transform.NewEngine(configured)
	if !engine.HasRelationAggregates() {
		return errors.New("snapshot has no active relation aggregate declarations")
	}
	before, err := store.ExportTenant(ctx, *tenant)
	if err != nil {
		return err
	}
	after := before
	var groups []relations.Snapshot
	if importing {
		path := *importPath
		if *thingsExport != "" {
			path = *thingsExport
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if *thingsExport != "" {
			after, err = relations.LinksFromThingsExport(raw, *tenant)
		} else {
			after, err = relations.LinksFromJSON(raw)
		}
		if err != nil {
			return err
		}
		for _, l := range after {
			if l.Tenant != *tenant || l.FromKind == "" || l.ToKind == "" {
				return errors.New("import must contain only the selected tenant with both source kinds")
			}
		}
		if *apply && len(after) == 0 && !*allowEmpty {
			return errors.New("empty authoritative import requires -allow-empty (removes all tenant links)")
		}
		groups = relations.SnapshotsForReplacement(before, after, *tenant)
	} else {
		groups = relations.SnapshotsForRebuild(before, *tenant, *relation)
	}
	planner := engine.RelationPlanner(nil, get("DEFAULT_CONTEXT_URL", ""))
	preview, err := planner(relations.Snapshot{Tenant: *tenant, Groups: groups, Reconcile: importing})
	if err != nil {
		return fmt.Errorf("plan: %w", err)
	}
	summary := struct {
		Tenant              string `json:"tenant"`
		RuleSnapshotVersion int64  `json:"ruleSnapshotVersion"`
		Links               int    `json:"links"`
		Groups              int    `json:"groups"`
		Planned             int    `json:"plannedWrites"`
		Enqueued            int    `json:"enqueuedWrites"`
		Applied             bool   `json:"applied"`
	}{Tenant: *tenant, RuleSnapshotVersion: version, Links: len(after), Groups: len(groups), Planned: len(preview), Applied: *apply}
	if *apply {
		var res projection.RebuildResult
		if importing {
			res, err = store.ImportTenant(ctx, *tenant, after, projection.CaptureTrace(ctx), planner)
		} else {
			res, err = store.Rebuild(ctx, *tenant, *relation, projection.CaptureTrace(ctx), planner, 0)
		}
		if err != nil {
			return fmt.Errorf("reconcile: %w", err)
		}
		summary.Groups, summary.Enqueued = res.Groups, res.Pending
	}
	return json.NewEncoder(output).Encode(summary)
}
