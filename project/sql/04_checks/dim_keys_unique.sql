-- check: surrogate keys are unique in every dimension
SELECT 'dim_customer' AS t, customer_key AS k FROM dim_customer GROUP BY customer_key HAVING COUNT(*) > 1
UNION ALL SELECT 'dim_policy_type', policy_type_key FROM dim_policy_type GROUP BY policy_type_key HAVING COUNT(*) > 1
UNION ALL SELECT 'dim_address', address_key FROM dim_address GROUP BY address_key HAVING COUNT(*) > 1
UNION ALL SELECT 'dim_date', date_key FROM dim_date GROUP BY date_key HAVING COUNT(*) > 1
