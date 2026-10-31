-- title: Policy KPIs (one row per customer policy)
-- question: For every policy: customer age at issuance, policy type name, term in years, duration so far, days late and
--   late category, % of total premium paid, status (Active / Expired / Lapsed) and active flag.
-- logic: age_at_issuance = whole years between DOB and Policy_Start_Dt. policy_term_years = (Policy_End_Dt -
--   Policy_Start_Dt) / 365.25. policy_duration_years = (as_of or end date if earlier - start) / 365.25.
--   days_late = Actual_Premium_Paid_Dt - Next_Premium_Dt when paid; when not paid and the due date has passed,
--   as_of - Next_Premium_Dt. late_category: <=0 On time, 1-30 Slightly late, 31-90 Moderately late, 91+ Severely late.
--   total_premium_paid_pct = Premium_Amt_Paid_TillDate / Total_Policy_Amt * 100.
--   policy_status: Expired if Policy_End_Dt < as_of; Lapsed if the due premium is unpaid and its due date has passed;
--   else Active. policy_active_flag = 1 only for Active.
-- assumptions: "Today" = as_of_date {as_of_date} (fixed, so results are reproducible). Blank paid date = not paid.
--   Policies whose type id is not in the policy type files show 'Unknown'.

DROP TABLE IF EXISTS kpi01_policy_kpis;

CREATE TABLE kpi01_policy_kpis AS
SELECT x.*,
       CASE WHEN x.days_late IS NULL THEN NULL
            WHEN x.days_late <= 0 THEN 'On time'
            WHEN x.days_late <= 30 THEN 'Slightly late'
            WHEN x.days_late <= 90 THEN 'Moderately late'
            ELSE 'Severely late' END AS late_category,
       CASE WHEN x.policy_status = 'Active' THEN 1 ELSE 0 END AS policy_active_flag
FROM (
  SELECT f.policy_key, f.customer_id, c.full_name, f.policy_id, t.policy_type_id, t.policy_type,
         t.policy_name AS policy_type_name, t.policy_term, a.region,
         f.policy_start_dt, f.policy_end_dt, f.next_premium_dt, f.actual_premium_paid_dt,
         f.premium_amt, f.total_policy_amt, f.premium_paid_till_date,
         CAST(FLOOR((f.policy_start_dt - c.dob) / 365.25) AS INT) AS age_at_issuance,
         CAST((f.policy_end_dt - f.policy_start_dt) / 365.25 AS DECIMAL(8,2)) AS policy_term_years,
         CAST((LEAST(f.policy_end_dt, CAST('{as_of_date}' AS DATE)) - f.policy_start_dt) / 365.25 AS DECIMAL(8,2))
           AS policy_duration_years,
         CASE WHEN f.actual_premium_paid_dt IS NOT NULL THEN f.actual_premium_paid_dt - f.next_premium_dt
              WHEN f.next_premium_dt < CAST('{as_of_date}' AS DATE) THEN CAST('{as_of_date}' AS DATE) - f.next_premium_dt
         END AS days_late,
         CASE WHEN f.actual_premium_paid_dt IS NULL AND f.next_premium_dt < CAST('{as_of_date}' AS DATE) THEN 1 ELSE 0 END
           AS is_missed_payment,
         CAST(CASE WHEN f.total_policy_amt > 0 THEN f.premium_paid_till_date * 100.0 / f.total_policy_amt END
              AS DECIMAL(8,2)) AS total_premium_paid_pct,
         CASE WHEN f.policy_end_dt < CAST('{as_of_date}' AS DATE) THEN 'Expired'
              WHEN f.actual_premium_paid_dt IS NULL AND f.next_premium_dt < CAST('{as_of_date}' AS DATE) THEN 'Lapsed'
              ELSE 'Active' END AS policy_status
  FROM fact_policy f
  JOIN dim_customer c ON c.customer_key = f.customer_key
  JOIN dim_policy_type t ON t.policy_type_key = f.policy_type_key
  JOIN dim_address a ON a.address_key = f.address_key
) x;
