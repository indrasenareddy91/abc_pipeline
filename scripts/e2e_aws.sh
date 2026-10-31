#!/bin/bash
# End-to-end test on AWS. Run from the repo root (CloudShell) after terraform apply + scripts/deploy.sh:
#   bash scripts/e2e_aws.sh hack-dev [day1 day2 ...]
# For each batch: upload project/sample_data/raw/<batch>/ to s3 raw/<batch>/ (transactions* last, so the
# files-arrived trigger fires once), wait for the Step Functions execution (Glue bronze -> silver -> gold),
# then query Redshift through the Data API: check_results, row_counts, warehouse table counts, KPI 1.
set -uo pipefail
NAME="${1:?usage: bash scripts/e2e_aws.sh <project>-<env> [batches...]}"; shift
BATCHES="${*:-day1 day2}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
export AWS_REGION="${AWS_REGION:-ap-south-1}" AWS_DEFAULT_REGION="${AWS_REGION:-ap-south-1}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text) || exit 1
BUCKET="${BUCKET:-${NAME}-data-${ACCOUNT}}"
SM="arn:aws:states:${AWS_REGION}:${ACCOUNT}:stateMachine:${NAME}-pipeline"
WG="${NAME}-wg"
DB=$(aws redshift-serverless get-namespace --namespace-name "${NAME}-ns" --query namespace.dbName --output text)
SECRET=$(aws redshift-serverless get-namespace --namespace-name "${NAME}-ns" --query namespace.adminPasswordSecretArn --output text)
SRC="${SAMPLE_RAW:-project/sample_data/raw}"
ok=0; bad=0
pass() { echo "  PASS  $1"; ok=$((ok+1)); }
fail() { echo "  FAIL  $1"; bad=$((bad+1)); }

sql() {  # run one SQL statement through the Redshift Data API and print the result as a table
  local id st
  id=$(aws redshift-data execute-statement --workgroup-name "$WG" --database "$DB" --secret-arn "$SECRET" \
       --sql "$1" --query Id --output text) || return 1
  for _ in $(seq 1 120); do
    st=$(aws redshift-data describe-statement --id "$id" --query Status --output text)
    case $st in FINISHED|FAILED|ABORTED) break;; esac; sleep 2
  done
  if [ "$st" != FINISHED ]; then
    echo "    SQL $st: $(aws redshift-data describe-statement --id "$id" --query Error --output text)"; return 1
  fi
  aws redshift-data get-statement-result --id "$id" --output json | python3 -c '
import json, sys
r = json.load(sys.stdin)
cols = [c["name"] for c in r["ColumnMetadata"]]
rows = [[("" if v.get("isNull") else str(next(iter(v.values())))) for v in rec] for rec in r["Records"]]
w = [max([len(c)] + [len(x[i]) for x in rows]) for i, c in enumerate(cols)]
line = lambda vals: "    " + "  ".join(v.ljust(w[i]) for i, v in enumerate(vals))
print(line(cols)); print(line(["-" * n for n in w]))
for x in rows: print(line(x))
print(f"    ({len(rows)} rows)")'
}

executions() { aws stepfunctions list-executions --state-machine-arn "$SM" --query 'executions[].executionArn' --output text; }

wait_run() {  # $1 = execution ARNs that existed before the upload; prints "<STATUS> <execution arn>"
  local E="" st
  for _ in $(seq 1 18); do  # up to 90 s for the S3 -> Lambda trigger
    for a in $(executions); do grep -qF "$a" <<< "$1" || { E=$a; break; }; done
    [ -n "$E" ] && break; sleep 5
  done
  if [ -z "$E" ]; then  # trigger off / required_files not matched -> start by hand
    E=$(aws stepfunctions start-execution --state-machine-arn "$SM" --name "e2e-$2-$(date +%s)" \
        --input "{\"batch\": \"$2\"}" --query executionArn --output text) || { echo "NOSTART -"; return; }
    echo "    (no trigger run seen; started by hand)" >&2
  fi
  echo "    execution ${E##*:} running..." >&2
  for _ in $(seq 1 360); do
    st=$(aws stepfunctions describe-execution --execution-arn "$E" --query status --output text)
    [ "$st" != RUNNING ] && break; sleep 10
  done
  echo "$st $E"
}

echo "== stack $NAME  bucket=$BUCKET  workgroup=$WG/$DB"
aws s3 ls "s3://$BUCKET/glue/jobs/" >/dev/null 2>&1 && pass "glue code deployed" || fail "glue code missing - run scripts/deploy.sh $NAME first"

for BATCH in $BATCHES; do
  echo "== batch $BATCH: upload -> trigger -> Glue bronze/silver/gold"
  [ -d "$SRC/$BATCH" ] || { fail "no sample files in $SRC/$BATCH"; continue; }
  T0=$(executions)
  for f in $(ls "$SRC/$BATCH" | grep -iv '^transactions') $(ls "$SRC/$BATCH" | grep -i '^transactions'); do
    aws s3 cp "$SRC/$BATCH/$f" "s3://$BUCKET/raw/$BATCH/$f" --only-show-errors && echo "    uploaded raw/$BATCH/$f"
  done
  aws s3 cp - "s3://$BUCKET/raw/$BATCH/_READY" --only-show-errors <<< "$(date -u +%FT%TZ)" && echo "    uploaded raw/$BATCH/_READY (starts the run)"
  R=$(wait_run "$T0" "$BATCH"); ST=${R%% *}; E=${R#* }
  if [ "$ST" = SUCCEEDED ]; then
    pass "workflow $BATCH: SUCCEEDED (${E##*:})"
    aws stepfunctions describe-execution --execution-arn "$E" --query output --output text | python3 -c '
import json, sys
o = json.load(sys.stdin)
for s in ("bronze", "silver", "gold"):
    if s in o:
        r = o[s]
        print("    %-7s %s  %s s  %s" % (s, r.get("State"), r.get("ExecutionTime"), r.get("JobRunId")))'
  else
    fail "workflow $BATCH: $ST (${E##*:})"
    [ "$E" != "-" ] && aws stepfunctions describe-execution --execution-arn "$E" --query '[error,cause]' --output text | cut -c1-600
    echo "    logs: aws logs tail /aws-glue/jobs/${NAME}/output --since 1h"
  fi
done

echo "== warehouse (Redshift Serverless, Data API)"
echo "  check_results per batch:"
sql "SELECT batch_id, status, COUNT(*) AS checks FROM check_results GROUP BY batch_id, status ORDER BY batch_id, status" || fail "check_results query"
echo "  checks not passed (if any):"
sql "SELECT batch_id, check_name, status, LEFT(detail, 80) AS detail FROM check_results WHERE UPPER(status) NOT IN ('PASS', 'OK') ORDER BY batch_id, check_name LIMIT 20"
echo "  row_counts:"
sql "SELECT batch_id, table_name, raw_rows, exact_duplicates, key_duplicates, quarantined, accepted, inserted, updated, unchanged FROM row_counts ORDER BY batch_id, table_name" || fail "row_counts query"
echo "  table counts:"
sql "SELECT 'fact_transaction' AS t, COUNT(*) AS n FROM fact_transaction UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer UNION ALL SELECT 'dim_branch', COUNT(*) FROM dim_branch UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product UNION ALL SELECT 'quarantine', COUNT(*) FROM quarantine UNION ALL SELECT 'rules_log', COUNT(*) FROM rules_log" || fail "table counts"
echo "  KPI 1 top customers:"
sql "SELECT * FROM kpi01_top_customers LIMIT 10" || fail "kpi01 query"

echo "== alarms"
aws cloudwatch describe-alarms --alarm-name-prefix "$NAME" --query 'MetricAlarms[].[AlarmName,StateValue]' --output text | sed 's/^/    /'
echo "== $ok passed, $bad failed"
[ "$bad" -eq 0 ]
