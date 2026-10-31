-- title: Policy status and late payment summary
-- question: How many policies are Active / Expired / Lapsed, and how late are premium payments, per region?
-- logic: Counts from kpi01 grouped by region, policy_status and late_category, with premium totals.
-- assumptions: Same definitions as kpi01.

DROP TABLE IF EXISTS kpi03_status_summary;

CREATE TABLE kpi03_status_summary AS
SELECT region, policy_status, COALESCE(late_category, 'Not due yet') AS late_category,
       COUNT(*) AS policies, CAST(SUM(premium_amt) AS DECIMAL(18,2)) AS premium_amt
FROM kpi01_policy_kpis
GROUP BY region, policy_status, COALESCE(late_category, 'Not due yet');
