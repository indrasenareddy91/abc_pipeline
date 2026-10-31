# KPI definitions (as of 2021-11-30)
- Age at issuance: years between DOB and policy start
- Customer tenure: years since customer effective start
- Late payment: paid date - due date -> 1-30 Slightly, 31-90 Moderately, 91+ Severely late
- Missed payments: premiums due before today and not paid
- Last premium paid gap: days since the latest payment
- Policy status: Expired (end < today), Lapsed (due and unpaid), else Active; active flag = 1 for Active
- Policy duration: years from start to today; policy term: years from start to end
- Total premium paid %: paid till date / total policy amount x 100
- Day over day: new / updated / quarantined rows per batch
