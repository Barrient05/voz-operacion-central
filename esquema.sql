-- Ejecutar una sola vez en la base de datos PostgreSQL.
-- Cada folio es único. Los reintentos no reemplazan ni duplican registros existentes.
CREATE TABLE IF NOT EXISTS voz_operacion_cases (
  id TEXT PRIMARY KEY,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS voz_operacion_cases_created_at_idx
  ON voz_operacion_cases (created_at DESC);
