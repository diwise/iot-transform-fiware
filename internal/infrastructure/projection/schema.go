package projection

// schemaSQL är den kanoniska DDL:n för projektionstabellerna i
// applikationskoden. Samma DDL finns som
// internal/infrastructure/rules/migrations/005_projection.sql för
// produktionsmigrering via regellagrets kedja (nummerförslag 005, A
// bekräftar). Båda är idempotenta (IF NOT EXISTS) och kan köras i valfri
// ordning; håll dem i synk tills A väljer kanonisk plats vid wiring.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS source_links(
  tenant            TEXT NOT NULL,
  relation          TEXT NOT NULL,
  from_id           TEXT NOT NULL,
  to_id             TEXT NOT NULL,
  revision          BIGINT NOT NULL DEFAULT 0,
  changed_at        TIMESTAMPTZ,
  from_kind         TEXT NOT NULL DEFAULT '',
  from_kind_version TEXT NOT NULL DEFAULT '',
  to_kind           TEXT NOT NULL DEFAULT '',
  to_kind_version   TEXT NOT NULL DEFAULT '',
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant, relation, from_id, to_id)
);
CREATE INDEX IF NOT EXISTS source_links_reverse ON source_links(tenant, relation, to_id, from_id);
CREATE INDEX IF NOT EXISTS source_links_from ON source_links(tenant, from_id);
CREATE INDEX IF NOT EXISTS source_links_to ON source_links(tenant, to_id);

CREATE TABLE IF NOT EXISTS source_link_specs(
  relation        TEXT PRIMARY KEY,
  allowed_targets JSONB NOT NULL DEFAULT '[]',
  min_links       INT NOT NULL DEFAULT 0,
  max_links       INT NOT NULL DEFAULT 0,
  hierarchical    BOOLEAN NOT NULL DEFAULT false,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS projection_seen_changes(
  relation_id TEXT PRIMARY KEY,
  tenant      TEXT NOT NULL,
  revision    BIGINT NOT NULL DEFAULT 0,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS projection_seen_tenant ON projection_seen_changes(tenant);

CREATE TABLE IF NOT EXISTS pending_broker_work(
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant          TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  operation       TEXT NOT NULL,
  entity_id       TEXT NOT NULL DEFAULT '',
  payload         JSONB NOT NULL DEFAULT '{}',
  trace_context   TEXT NOT NULL DEFAULT '',
  rule_id         TEXT NOT NULL DEFAULT '',
  rule_revision   BIGINT NOT NULL DEFAULT 0,
  attempts        INT NOT NULL DEFAULT 0,
  claimed_at      TIMESTAMPTZ,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant, idempotency_key)
);
CREATE INDEX IF NOT EXISTS pending_broker_work_claim ON pending_broker_work(tenant, created_at);
`

// schemaLock är advisory-låset för schemamigrering. Skilt från regellagrets
// 6193027748113459 (migrering) och 6193027748113460 (regeltransaktioner).
const schemaLock = 6193027748113461
