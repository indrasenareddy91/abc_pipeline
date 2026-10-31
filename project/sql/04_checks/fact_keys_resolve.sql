-- check: every fact foreign key points to a dimension row (or -1 UNKNOWN)
SELECT f.policy_key FROM fact_policy f
LEFT JOIN dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN dim_policy_type t ON t.policy_type_key = f.policy_type_key
LEFT JOIN dim_address a ON a.address_key = f.address_key
LEFT JOIN dim_date d ON d.date_key = f.start_date_key
WHERE c.customer_key IS NULL OR t.policy_type_key IS NULL OR a.address_key IS NULL OR d.date_key IS NULL
