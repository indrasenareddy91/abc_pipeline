-- ABC Insurance curated star schema (gold). Postgres 16 + Redshift compatible. Maintained by 02_load/*.
-- dim_customer is SCD Type 2 on marital_status + customer_segment (history kept); valid_from <= d < valid_to.
-- Surrogate key -1 = UNKNOWN member. batch_id = batch that last wrote the row.

CREATE TABLE IF NOT EXISTS dim_date (
  date_key INT, full_date DATE, year INT, quarter INT, month INT, month_label VARCHAR(7));

CREATE TABLE IF NOT EXISTS dim_customer (
  customer_key INT, customer_id VARCHAR(64), title VARCHAR(16), full_name VARCHAR(256),
  customer_segment VARCHAR(64), marital_status VARCHAR(32), gender VARCHAR(16), dob DATE,
  effective_start_dt DATE, effective_end_dt DATE,
  valid_from DATE, valid_to DATE, is_current BOOLEAN, version INT, batch_id VARCHAR(64));

CREATE TABLE IF NOT EXISTS dim_policy_type (
  policy_type_key INT, policy_type_id VARCHAR(64), policy_type VARCHAR(128), policy_type_desc VARCHAR(512),
  policy_name VARCHAR(128), policy_term VARCHAR(32), batch_id VARCHAR(64));

CREATE TABLE IF NOT EXISTS dim_address (
  address_key INT, country VARCHAR(64), region VARCHAR(32), state VARCHAR(64), city VARCHAR(128),
  postal_code VARCHAR(16));

CREATE TABLE IF NOT EXISTS fact_policy (
  policy_key VARCHAR(160), customer_id VARCHAR(64), policy_id VARCHAR(64),
  customer_key INT, policy_type_key INT, address_key INT, start_date_key INT,
  premium_amt DECIMAL(18,2), total_policy_amt DECIMAL(18,2), premium_paid_till_date DECIMAL(18,2),
  policy_start_dt DATE, policy_end_dt DATE, next_premium_dt DATE, actual_premium_paid_dt DATE,
  quality_flags VARCHAR(512), first_batch VARCHAR(64), batch_id VARCHAR(64));
