-- title: Incremental load reconciliation (day over day)
-- question: What did each daily file add or change: new records, updated records, unchanged re-sends, rejected rows?
--   Which customers got a new version (marital status / segment change)?
-- logic: kpi04_incremental_counts from row_counts (per batch and table): new = keys never seen before, updated =
--   existing keys whose values changed (corrections and real changes), unchanged = identical re-sends.
--   kpi04_customer_changes: customers touched by each batch: new customer (first policy arrived in that batch),
--   changed (new SCD2 version: marital status / segment changed) or corrected (other fields fixed in place, SCD1).
-- assumptions: An existing key with different values replaces the old row in fact_policy (latest file wins).

DROP TABLE IF EXISTS kpi04_incremental_counts;

CREATE TABLE kpi04_incremental_counts AS
SELECT batch_id, table_name, raw_rows, inserted AS new_records, updated AS updated_records,
       unchanged AS unchanged_records, quarantined AS quarantined_rows,
       exact_duplicates + key_duplicates AS duplicate_rows
FROM row_counts;

DROP TABLE IF EXISTS kpi04_customer_changes;

CREATE TABLE kpi04_customer_changes AS
SELECT n.batch_id, n.customer_id, n.version,
       CASE WHEN n.version > 1 THEN 'changed (new version)'
            WHEN fb.first_batch = n.batch_id THEN 'new customer'
            ELSE 'corrected (in place)' END AS change_type,
       o.marital_status AS old_marital_status, n.marital_status AS new_marital_status,
       o.customer_segment AS old_segment, n.customer_segment AS new_segment, n.valid_from
FROM dim_customer n
LEFT JOIN dim_customer o ON o.customer_id = n.customer_id AND o.version = n.version - 1
LEFT JOIN (SELECT customer_id, MIN(first_batch) AS first_batch FROM fact_policy GROUP BY customer_id) fb
  ON fb.customer_id = n.customer_id
WHERE n.customer_key <> -1;
