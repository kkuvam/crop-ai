-- 08__metadata_schema_registry.sql
CREATE TABLE IF NOT EXISTS metadata_schema_registry (
  table_name   VARCHAR,
  column_name  VARCHAR,
  data_type    VARCHAR,
  is_nullable  BOOLEAN,
  column_default VARCHAR,
  description  VARCHAR,
  schema_version VARCHAR,
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
