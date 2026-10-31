-- check: kpi01 has exactly one row per fact_policy row (no join fan-out)
SELECT 'kpi01' AS t, (SELECT COUNT(*) FROM kpi01_policy_kpis) AS kpi_rows, (SELECT COUNT(*) FROM fact_policy) AS fact_rows
WHERE (SELECT COUNT(*) FROM kpi01_policy_kpis) <> (SELECT COUNT(*) FROM fact_policy)
