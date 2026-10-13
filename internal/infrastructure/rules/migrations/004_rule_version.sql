-- TF-05: global versionsräknare för regelregistret. Bumpas i samma
-- transaktion som varje mutation (insert/update/delete/seed-ändring) så
-- att andra tjänsteinstanser kan upptäcka ändringar via polling och
-- ladda om sina snapshots. Enradstabell; saknad rad betyder version 0.
CREATE TABLE IF NOT EXISTS rule_version(
  id      INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  version BIGINT NOT NULL DEFAULT 0
);
