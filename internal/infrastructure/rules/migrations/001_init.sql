-- Regler för den konfigurationsstyrda transformeringen. YAML-filerna i
-- deployments/models är seed: vid start läggs saknade seed_key in, ändrade
-- seed-rader uppdateras, API-ändrade rader (source='api') rörs aldrig.
-- Körning sker mot en atomisk minnessnapshot som byggs om vid varje
-- mutation; tabellen är sanningen för CRUD via API.
CREATE TABLE IF NOT EXISTS transform_rules(
  id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  seed_key  TEXT UNIQUE,
  kind      TEXT NOT NULL,
  rule      JSONB NOT NULL,
  source    TEXT NOT NULL DEFAULT 'seed',
  revision  BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS transform_rules_kind ON transform_rules(kind);
