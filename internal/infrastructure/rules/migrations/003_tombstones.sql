-- TF-03: beständiga tombstones för borttagna seed-regler. DELETE tar bort
-- raden men registrerar seed_key här, så att nästa seed inte återupplivar
-- en uttryckligen borttagen regel. Tombstones för nycklar utanför aktuellt
-- seed-set beskärs av SeedRules (en regel som återinförs i seed behandlas
-- som ny).
CREATE TABLE IF NOT EXISTS seed_tombstones(
  seed_key   TEXT PRIMARY KEY,
  deleted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
