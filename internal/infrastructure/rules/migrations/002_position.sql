-- TF-02: explicit stabil regelordning. Motorn bryter lika-poäng med lägst
-- index, så DB-ordningen måste vara deterministisk oberoende av UUID:
-- seed-regler i filordning (0..N), API-regler sist i skapandeordning.
-- Sekvensen ger samtidiga API-skapanden distinkta positioner utan
-- omnumrering (luckor vid DELETE är avsiktliga).
ALTER TABLE transform_rules ADD COLUMN IF NOT EXISTS position BIGINT;
CREATE SEQUENCE IF NOT EXISTS transform_rules_position_seq;
-- Backfill: seed-rader i filordning med numeriskt index (lexikografisk
-- sortering räcker inte för filer med 10+ regler), övriga efter
-- skapelsetid. Körs en gång; framtida seed-körningar rättar positioner.
-- Alla befintliga rader kommer från SeedDir (seed_key <fil>#<index>).
WITH ordered AS (
  SELECT id, ROW_NUMBER() OVER (
    ORDER BY (seed_key IS NULL),
      split_part(seed_key, '#', 1),
      (split_part(seed_key, '#', 2))::int,
      created_at, id
  ) - 1 AS pos
  FROM transform_rules WHERE position IS NULL
)
UPDATE transform_rules t SET position = o.pos FROM ordered o WHERE o.id = t.id;
SELECT setval('transform_rules_position_seq', GREATEST((SELECT COALESCE(MAX(position), -1) + 1 FROM transform_rules), 1), false);
ALTER TABLE transform_rules ALTER COLUMN position SET NOT NULL;
ALTER TABLE transform_rules ALTER COLUMN position SET DEFAULT nextval('transform_rules_position_seq');
