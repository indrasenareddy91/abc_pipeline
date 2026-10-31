-- check: status and late category only take the defined values; active flag matches status
SELECT policy_key, policy_status, late_category, policy_active_flag FROM kpi01_policy_kpis
WHERE policy_status NOT IN ('Active', 'Expired', 'Lapsed')
   OR (late_category IS NOT NULL AND late_category NOT IN ('On time', 'Slightly late', 'Moderately late', 'Severely late'))
   OR (policy_active_flag = 1) <> (policy_status = 'Active')
