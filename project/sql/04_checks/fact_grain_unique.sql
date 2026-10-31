-- check: fact_policy has one row per policy_key
SELECT policy_key, COUNT(*) AS n FROM fact_policy GROUP BY policy_key HAVING COUNT(*) > 1 OR policy_key IS NULL
