-- fact_policy: grain = one row per customer policy (policy_key = customer_id|policy_id).
-- Incremental: rows of this batch replace the same policy_key (update / correction) or are added (new). No duplicates.

DROP TABLE IF EXISTS wrk_fact_first;
CREATE TABLE wrk_fact_first AS
SELECT policy_key, first_batch FROM fact_policy WHERE policy_key IN (SELECT policy_key FROM stg_customer_policies);

DELETE FROM fact_policy WHERE policy_key IN (SELECT policy_key FROM stg_customer_policies);

INSERT INTO fact_policy (policy_key, customer_id, policy_id, customer_key, policy_type_key, address_key, start_date_key,
  premium_amt, total_policy_amt, premium_paid_till_date, policy_start_dt, policy_end_dt, next_premium_dt,
  actual_premium_paid_dt, quality_flags, first_batch, batch_id)
SELECT s.policy_key, s.customer_id, s.policy_id,
       COALESCE(c.customer_key, -1), COALESCE(t.policy_type_key, -1), COALESCE(a.address_key, -1),
       COALESCE(dd.date_key, -1),
       s.premium_amt, s.total_policy_amt, s.premium_paid_till_date,
       s.policy_start_dt, s.policy_end_dt, s.next_premium_dt, s.actual_premium_paid_dt,
       s.quality_flags, COALESCE(f.first_batch, '{batch_id}'), '{batch_id}'
FROM stg_customer_policies s
LEFT JOIN dim_customer c ON c.customer_id = s.customer_id AND c.is_current
LEFT JOIN dim_policy_type t ON t.policy_type_id = s.policy_type_id
LEFT JOIN dim_address a ON COALESCE(a.country, '~') = COALESCE(s.country, '~') AND COALESCE(a.region, '~') = COALESCE(s.region, '~')
  AND COALESCE(a.state, '~') = COALESCE(s.state, '~') AND COALESCE(a.city, '~') = COALESCE(s.city, '~')
  AND COALESCE(a.postal_code, '~') = COALESCE(s.postal_code, '~') AND a.address_key <> -1
LEFT JOIN dim_date dd ON dd.full_date = s.policy_start_dt
LEFT JOIN wrk_fact_first f ON f.policy_key = s.policy_key;

DROP TABLE IF EXISTS wrk_fact_first;
