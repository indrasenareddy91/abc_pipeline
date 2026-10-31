# ABC Insurance — Data Warehouse on AWS

Multi-region insurance data (CSV, TXT, JSON) → cleaned → star schema in Redshift → policy and premium KPIs, loaded daily.

```
S3 → Lambda → Step Functions → Glue (bronze → silver → gold) → Redshift → Streamlit dashboard
```

## How to run

**Locally** (Docker Desktop running):
```
bash scripts/hack.sh local day0 day1      # dashboard: http://127.0.0.1:8501
python -m pytest -q tests/test_project.py
```

**On AWS** (CloudShell, region ap-south-1):
```
bash scripts/hack.sh cloud-up             # create infrastructure (once)
bash scripts/hack.sh deploy               # push code + dashboard
bash scripts/hack.sh run day0 day1        # run the pipeline
```
