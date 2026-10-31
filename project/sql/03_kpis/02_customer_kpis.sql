-- title: Customer KPIs (one row per customer)
-- question: For every customer: tenure with ABC Insurance, number of missed premium payments, last premium paid gap.
-- logic: customer_tenure_years = (as_of - Effective_Start_Dt) / 365.25. missed_payments_count = policies whose
--   due premium is unpaid and the due date has passed (from kpi01). last_premium_paid_dt = latest
--   Actual_Premium_Paid_Dt over the customer's policies; last_premium_paid_gap_days = as_of - that date.
-- assumptions: "Today" = as_of_date {as_of_date}. Uses the customer's current version (SCD2 is_current).
--   A customer with no payment at all has an empty gap.

DROP TABLE IF EXISTS kpi02_customer_kpis;

CREATE TABLE kpi02_customer_kpis AS
SELECT c.customer_id, c.title, c.full_name, c.customer_segment, c.marital_status, c.gender,
       c.effective_start_dt,
       CAST((CAST('{as_of_date}' AS DATE) - c.effective_start_dt) / 365.25 AS DECIMAL(8,2)) AS customer_tenure_years,
       p.policies, p.missed_payments_count, p.last_premium_paid_dt,
       CAST('{as_of_date}' AS DATE) - p.last_premium_paid_dt AS last_premium_paid_gap_days
FROM dim_customer c
JOIN (SELECT customer_id, COUNT(*) AS policies, SUM(is_missed_payment) AS missed_payments_count,
             MAX(actual_premium_paid_dt) AS last_premium_paid_dt
      FROM kpi01_policy_kpis GROUP BY customer_id) p ON p.customer_id = c.customer_id
WHERE c.is_current AND c.customer_key <> -1;
