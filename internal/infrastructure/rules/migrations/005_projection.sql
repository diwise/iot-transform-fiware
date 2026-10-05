-- 005_projection.sql — F: beständig modellneutral relationsprojektion.
--
-- Ägare: paket F. Nummerförslag 005 (001-004 tagna i denna tjänst;
-- things-migrationerna 001-007 ligger i iot-things-v2 med separat databas
-- och kolliderar därför inte). A bekräftar numret vid wiring.
--
-- Lagringsbegrepp är källrelationer/tenant — aldrig standardmodellnamn.
-- tenant ingår i samtliga primärnycklar och WHERE-villkor.
-- revision är enbart metadata/diagnostik: ingen recency-filtrering sker
-- (sena event är en dokumenterad avgränsning enligt PLAN1050).
--
-- Mängdlagring per länkrad (inte blobbar): parallella workers kan aldrig
-- tappa länkar via read-modify-write; serialisering sker med
-- transaktionella advisory-lås i projektionslagret (inget lokalt mutex,
-- det ger ingen garanti mellan instanser).
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
-- Omvänd mängd per mål utan full tabellscan.
CREATE INDEX IF NOT EXISTS source_links_reverse ON source_links(tenant, relation, to_id, from_id);
-- Delete-policy skannar båda ändpunkterna för ett borttaget mål.
CREATE INDEX IF NOT EXISTS source_links_from ON source_links(tenant, from_id);
CREATE INDEX IF NOT EXISTS source_links_to ON source_links(tenant, to_id);

-- Generella kardinalitetsspecar per relationsnamn (min/max/allowedTargets).
-- Saknad spec = permissiv (endast tenant- och duplikatgrind); D äger
-- spec-författandet, F upprätthåller gränserna vid skrivning.
CREATE TABLE IF NOT EXISTS source_link_specs(
  relation      TEXT PRIMARY KEY,
  allowed_targets JSONB NOT NULL DEFAULT '[]',
  min_links     INT NOT NULL DEFAULT 0,
  max_links     INT NOT NULL DEFAULT 0,
  hierarchical  BOOLEAN NOT NULL DEFAULT false,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sedda ändringsidentiteter: exakt replay av samma relationId är
-- idempotent och ger inget nytt brokerarbete (separate från revision,
-- som aldrig filtrerar).
CREATE TABLE IF NOT EXISTS projection_seen_changes(
  relation_id TEXT PRIMARY KEY,
  tenant      TEXT NOT NULL,
  revision    BIGINT NOT NULL DEFAULT 0,
  applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS projection_seen_tenant ON projection_seen_changes(tenant);

-- Återställbart väntande brokerarbete. trace_context är W3C-carriern
-- separat från domänpayloaden (payload); tom sträng = spårlös händelse.
-- Rader lever tills MarkDone efter lyckad brokerleverans: DB-framgång
-- följd av brokerfel är replay-/omstartssäker. Idempotensnyckel per
-- tenant hindrar dubbelt arbete vid samtidig planering.
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
