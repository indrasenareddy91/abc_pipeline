-- Operational tables (generic for every project). Postgres 16 + Redshift compatible. Created once, never dropped.
-- Loaded per batch by gold.py (DELETE WHERE batch_id = <batch> then INSERT), so re-running a batch replaces its rows.

CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id VARCHAR(64), batch_id VARCHAR(64), step VARCHAR(64), status VARCHAR(16),
  started_at TIMESTAMP, finished_at TIMESTAMP, message VARCHAR(65535));

CREATE TABLE IF NOT EXISTS ingestion_log (
  batch_id VARCHAR(64), table_name VARCHAR(128), file_name VARCHAR(1024), rows_in BIGINT,
  columns_in VARCHAR(65535), sha256 VARCHAR(64), loaded_at TIMESTAMP, run_id VARCHAR(64));

CREATE TABLE IF NOT EXISTS profile_columns (
  batch_id VARCHAR(64), table_name VARCHAR(128), column_name VARCHAR(128), rows_total BIGINT, blank BIGINT,
  placeholder BIGINT, distinct_values BIGINT, untrimmed BIGINT, top_values VARCHAR(65535), shapes VARCHAR(65535));

CREATE TABLE IF NOT EXISTS rules_log (
  batch_id VARCHAR(64), table_name VARCHAR(128), rule_id VARCHAR(256), column_name VARCHAR(128),
  issue VARCHAR(65535), action VARCHAR(65535), rows_affected BIGINT);

CREATE TABLE IF NOT EXISTS quarantine (
  batch_id VARCHAR(64), table_name VARCHAR(128), record_key VARCHAR(1024), business_key VARCHAR(256),
  rule_id VARCHAR(256), reason VARCHAR(65535), raw_row VARCHAR(65535), created_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS row_counts (
  batch_id VARCHAR(64), table_name VARCHAR(128), raw_rows BIGINT, exact_duplicates BIGINT, key_duplicates BIGINT,
  quarantined BIGINT, accepted BIGINT, inserted BIGINT, updated BIGINT, unchanged BIGINT);

CREATE TABLE IF NOT EXISTS check_results (
  run_id VARCHAR(64), batch_id VARCHAR(64), check_name VARCHAR(256), status VARCHAR(8),
  detail VARCHAR(65535), checked_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS kpi_catalog (
  kpi_id VARCHAR(16), title VARCHAR(512), question VARCHAR(65535), logic VARCHAR(65535),
  assumptions VARCHAR(65535), table_name VARCHAR(1024));

-- SPEC v2 B: data journey (one row per step input -> output, with row counts) and expectation results.
-- Loaded per batch from ops/step_io and ops/expectations parquet; gold appends its own step_io rows.
CREATE TABLE IF NOT EXISTS step_io (
  batch_id VARCHAR(64), run_id VARCHAR(64), step VARCHAR(32), table_name VARCHAR(128),
  input_location VARCHAR(2048), rows_in BIGINT, output_location VARCHAR(2048), rows_out BIGINT,
  rows_rejected BIGINT, notes VARCHAR(65535), recorded_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS dq_expectations (
  batch_id VARCHAR(64), run_id VARCHAR(64), table_name VARCHAR(128), expectation VARCHAR(1024),
  column_name VARCHAR(256), severity VARCHAR(16), status VARCHAR(8), failing_rows BIGINT,
  detail VARCHAR(65535), checked_at TIMESTAMP);
