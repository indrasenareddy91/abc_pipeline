# Assumptions
- Dates are US month-first; 2-digit years are 20xx unless that is in the future, then 19xx
- Key = Customer ID + Policy ID (each alone is not unique)
- Blank region is taken from the regional source file (South / West)
- Blank paid date = premium not paid (missed payment)
- Amounts are USD; latest file wins for the same policy
