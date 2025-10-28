-- 07__metadata_table_manifest.sql
CREATE TABLE IF NOT EXISTS metadata_table_manifest (
  table_name VARCHAR PRIMARY KEY,
  purpose    VARCHAR,
  primary_keys VARCHAR,
  partitioning VARCHAR,
  last_verified TIMESTAMP
);
