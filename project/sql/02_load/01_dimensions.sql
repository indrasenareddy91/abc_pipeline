-- Dimensions from this batch's new/changed silver rows (stg_*). Idempotent: re-running a batch gives the same result.

-- ---------- dim_policy_type (SCD1: overwrite) ----------
INSERT INTO dim_policy_type (policy_type_key, policy_type_id, policy_type, policy_type_desc, policy_name, policy_term, batch_id)
SELECT -1, 'UNKNOWN', 'Unknown', NULL, NULL, NULL, 'system'
FROM (SELECT COUNT(*) AS n FROM dim_policy_type WHERE policy_type_key = -1) x WHERE x.n = 0;

DELETE FROM dim_policy_type WHERE policy_type_id IN (SELECT policy_type_id FROM stg_policy_types);

INSERT INTO dim_policy_type (policy_type_key, policy_type_id, policy_type, policy_type_desc, policy_name, policy_term, batch_id)
SELECT CAST(policy_type_id AS INT), policy_type_id, policy_type, policy_type_desc, policy_name, policy_term, '{batch_id}'
FROM stg_policy_types;

-- ---------- dim_address (insert-only, one row per distinct location) ----------
INSERT INTO dim_address (address_key, country, region, state, city, postal_code)
SELECT -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', NULL
FROM (SELECT COUNT(*) AS n FROM dim_address WHERE address_key = -1) x WHERE x.n = 0;

INSERT INTO dim_address (address_key, country, region, state, city, postal_code)
SELECT m.max_key + CAST(ROW_NUMBER() OVER (ORDER BY a.region, a.state, a.city, a.postal_code) AS INT),
       a.country, a.region, a.state, a.city, a.postal_code
FROM (SELECT DISTINCT country, region, state, city, postal_code FROM stg_customer_policies) a
CROSS JOIN (SELECT CAST(COALESCE(MAX(address_key), 0) AS INT) AS max_key FROM dim_address) m
LEFT JOIN dim_address d ON COALESCE(d.country, '~') = COALESCE(a.country, '~') AND COALESCE(d.region, '~') = COALESCE(a.region, '~')
  AND COALESCE(d.state, '~') = COALESCE(a.state, '~') AND COALESCE(d.city, '~') = COALESCE(a.city, '~')
  AND COALESCE(d.postal_code, '~') = COALESCE(a.postal_code, '~')
WHERE d.address_key IS NULL;

-- ---------- dim_date (dates policies start on) ----------
INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_label)
SELECT -1, NULL, NULL, NULL, NULL, 'UNKNOWN'
FROM (SELECT COUNT(*) AS n FROM dim_date WHERE date_key = -1) x WHERE x.n = 0;

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_label)
SELECT CAST(TO_CHAR(d.full_date, 'YYYYMMDD') AS INT), d.full_date,
       CAST(EXTRACT(YEAR FROM d.full_date) AS INT), CAST(EXTRACT(QUARTER FROM d.full_date) AS INT),
       CAST(EXTRACT(MONTH FROM d.full_date) AS INT), TO_CHAR(d.full_date, 'YYYY-MM')
FROM (SELECT DISTINCT policy_start_dt AS full_date FROM stg_customer_policies WHERE policy_start_dt IS NOT NULL) d
LEFT JOIN dim_date x ON x.full_date = d.full_date
WHERE x.date_key IS NULL;

-- ---------- dim_customer (SCD2 on marital_status + customer_segment) ----------
INSERT INTO dim_customer (customer_key, customer_id, title, full_name, customer_segment, marital_status, gender, dob,
  effective_start_dt, effective_end_dt, valid_from, valid_to, is_current, version, batch_id)
SELECT -1, 'UNKNOWN', NULL, 'Unknown', NULL, NULL, NULL, NULL, NULL, NULL,
       CAST('1900-01-01' AS DATE), CAST('9999-12-31' AS DATE), TRUE, 1, 'system'
FROM (SELECT COUNT(*) AS n FROM dim_customer WHERE customer_key = -1) x WHERE x.n = 0;

-- one row per customer from this batch (a customer with 2 policies appears twice: take the last row read)
DROP TABLE IF EXISTS wrk_cust;
CREATE TABLE wrk_cust AS
SELECT customer_id, title, full_name, customer_segment, marital_status, gender, dob, effective_start_dt, effective_end_dt
FROM (SELECT s.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _source_file DESC, _source_row_number DESC) AS rn
      FROM stg_customer_policies s) t
WHERE rn = 1;

DROP TABLE IF EXISTS wrk_dim_customer;
CREATE TABLE wrk_dim_customer AS
SELECT w.*, d.customer_key AS cur_key, d.version AS cur_version,
       CASE WHEN d.customer_key IS NULL THEN 'new'
            WHEN d.batch_id = '{batch_id}' THEN 'same_batch'
            WHEN COALESCE(w.marital_status, '~') <> COALESCE(d.marital_status, '~')
              OR COALESCE(w.customer_segment, '~') <> COALESCE(d.customer_segment, '~') THEN 'new_version'
            ELSE 'scd1' END AS action
FROM wrk_cust w
LEFT JOIN dim_customer d ON d.customer_id = w.customer_id AND d.is_current AND d.customer_key <> -1;

UPDATE dim_customer SET valid_to = CAST('{batch_date}' AS DATE), is_current = FALSE
FROM wrk_dim_customer w WHERE dim_customer.customer_key = w.cur_key AND w.action = 'new_version';

INSERT INTO dim_customer (customer_key, customer_id, title, full_name, customer_segment, marital_status, gender, dob,
  effective_start_dt, effective_end_dt, valid_from, valid_to, is_current, version, batch_id)
SELECT m.max_key + CAST(ROW_NUMBER() OVER (ORDER BY w.customer_id) AS INT),
       w.customer_id, w.title, w.full_name, w.customer_segment, w.marital_status, w.gender, w.dob,
       w.effective_start_dt, w.effective_end_dt,
       CASE WHEN w.action = 'new' THEN CAST('1900-01-01' AS DATE) ELSE CAST('{batch_date}' AS DATE) END,
       CAST('9999-12-31' AS DATE), TRUE,
       CASE WHEN w.action = 'new' THEN 1 ELSE w.cur_version + 1 END, '{batch_id}'
FROM wrk_dim_customer w
CROSS JOIN (SELECT CAST(COALESCE(MAX(customer_key), 0) AS INT) AS max_key FROM dim_customer) m
WHERE w.action IN ('new', 'new_version');

UPDATE dim_customer SET title = w.title, full_name = w.full_name, gender = w.gender, dob = w.dob,
       effective_start_dt = w.effective_start_dt, effective_end_dt = w.effective_end_dt,
       customer_segment = w.customer_segment, marital_status = w.marital_status, batch_id = '{batch_id}'
FROM wrk_dim_customer w WHERE dim_customer.customer_key = w.cur_key AND w.action IN ('same_batch', 'scd1');

DROP TABLE IF EXISTS wrk_dim_customer;
DROP TABLE IF EXISTS wrk_cust;
