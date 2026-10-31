-- check: every customer has exactly one current version in dim_customer
SELECT customer_id, SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
FROM dim_customer GROUP BY customer_id HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
