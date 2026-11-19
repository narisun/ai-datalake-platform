#!/bin/bash
# Real-world transformation scenarios, run against the live dev stack after
# scenarios.sh. Run inside the airflow container:
#
#   bash /opt/datalake/dev/scenarios_transform.sh
#
# It drops awkward files into the landing container, then runs ingest and transform.
set -uo pipefail

PY=/opt/datalake/venv/bin/python
DBT=/opt/datalake/venv/bin/dbt
LANDING="$PY /opt/datalake/dev/landing.py"
TMP=$(mktemp -d)

# dbt run directly (outside `dl transform`) needs the same sign-in settings.
export TRINO_HOST=trino TRINO_PORT=8443 TRINO_SCHEME=https TRINO_USER=svc_pipeline
export TRINO_PASSWORD="$DL_SECRET_TRINO_PIPELINE_PASSWORD" TRINO_CERT=/etc/trino-tls/ca.crt

sql() { "$PY" /opt/datalake/dev/trino_dev.py "$1"; }

results() {
  "$PY" - <<'PYEOF'
import collections, json
results = json.load(open("/opt/datalake/work/target/run_results.json"))["results"]
counts = collections.Counter((r["unique_id"].split(".")[0], r["status"]) for r in results)
print("    results:", ", ".join(f"{kind} {status}={n}" for (kind, status), n in sorted(counts.items())))
PYEOF
}

transform() {
  dl transform 2>&1 | grep -av "INFO:trino" | tail -4 | cut -c1-600
  echo "    exit code: ${PIPESTATUS[0]}"
  results
}

echo "== T1. What the first build ran"
results
"$PY" -c "
import json
for r in json.load(open('/opt/datalake/work/target/sources.json'))['results']:
    print('    freshness:', r['unique_id'].split('.')[-1], r['status'])
"

echo "== T2. A file with bad values, a UK-format date, an unknown customer, a duplicate and a correction"
cat > "$TMP/orders_2026-11-06.csv" <<'EOF'
Order ID,Customer ID,Ordered At,Amount
201,1,2026-11-06 09:00:00,abc
202,2,06/11/2026 10:30,25.00
203,,2026-11-06 11:00:00,15.00
204,99,2026-11-06 12:00:00,60.00
205,3,2026-11-06 13:00:00,-5.00
206,1,2026-11-06 14:00:00,10.00
206,1,2026-11-06 14:00:00,11.00
13,3,2026-11-02 09:05:00,300.00
EOF
$LANDING put "$TMP/orders_2026-11-06.csv" shop/orders/orders_2026-11-06.csv > /dev/null
dl ingest | tail -1
transform
echo "  rejected rows:"
sql "select entity, business_key, reject_reason, source_file from lake.analytics_staging.rejected_rows order by 2"
echo "  canonical orders from that file (204 unknown customer, 205 negative, 206 last row wins, 13 corrected):"
sql "select order_id, customer_id, amount from lake.analytics_canonical.sales_order where order_id in ('13','201','202','203','204','205','206') order by cast(order_id as integer)"
echo "  mart row for the unknown customer:"
sql "select order_id, customer_id, country, amount from lake.analytics_marts.fct_orders where order_id = '204'"

echo "== T3. A customer file with a duplicated id: the build must stop and say why"
sql "select count(*) as customers_before from lake.analytics_marts.dim_customer"
"$PY" - "$TMP/customers_2026-11-03.xlsx" <<'PYEOF'
import sys, openpyxl
workbook = openpyxl.Workbook()
sheet = workbook.active
for row in [
    ["Customer ID", "Full Name", "Email", "Country"],
    [1, "Ada Lovelace", "ada@example.com", "GB"],
    [2, "Alan Turing", "alan@example.com", "GB"],
    [2, "Alan M. Turing", "alan.turing@example.com", "GB"],
    [3, "Grace O'Brien-Hopper", "grace@example.com", "US"],
    [4, "Katherine Johnson", "katherine-at-example.com", "US"],
]:
    sheet.append(row)
workbook.save(sys.argv[1])
PYEOF
$LANDING put "$TMP/customers_2026-11-03.xlsx" shop/customers/customers_2026-11-03.xlsx > /dev/null
dl ingest | tail -1
transform
echo "  the marts still hold the last good build:"
sql "select count(*) as customers_after from lake.analytics_marts.dim_customer"
dl runs --process transform --limit 1 | cut -c1-330

echo "== T4. The sender fixes the customer file (one bad email remains)"
"$PY" - "$TMP/customers_2026-11-04.xlsx" <<'PYEOF'
import sys, openpyxl
workbook = openpyxl.Workbook()
sheet = workbook.active
for row in [
    ["Customer ID", "Full Name", "Email", "Country"],
    [1, "Ada Lovelace", "ada@example.com", "GB"],
    [2, "Alan M. Turing", "alan.turing@example.com", "gb"],
    [3, "Grace O'Brien-Hopper", "grace@example.com", "US"],
    [4, "Katherine Johnson", "katherine-at-example.com", "US"],
    [99, "Margaret Hamilton", "margaret@example.com", "US"],
]:
    sheet.append(row)
workbook.save(sys.argv[1])
PYEOF
$LANDING put "$TMP/customers_2026-11-04.xlsx" shop/customers/customers_2026-11-04.xlsx > /dev/null
dl ingest | tail -1
transform
sql "select customer_id, full_name, country from lake.analytics_canonical.customer order by cast(customer_id as integer)"
sql "select entity, business_key, reject_reason from lake.analytics_staging.rejected_rows where entity = 'customer'"
echo "  order 204 now finds its customer:"
sql "select order_id, country from lake.analytics_marts.fct_orders where order_id = '204'"

echo "== T5. A model change that breaks a contract is refused"
rm -rf /tmp/contract && cp -r /opt/datalake/dbt /tmp/contract
# \r\? because the project may be checked out with Windows line endings.
sed -i 's/^    amount\r\?$/    cast(amount as varchar) as amount/' /tmp/contract/models/canonical/sales_order.sql
grep -q 'cast(amount as varchar)' /tmp/contract/models/canonical/sales_order.sql || echo '    (the test edit did not apply)'
$DBT build --select sales_order --project-dir /tmp/contract --profiles-dir /tmp/contract \
  --target-path /tmp/contract-target --log-path /tmp/contract-logs --no-use-colors 2>&1 \
  | grep -aE "contract|mismatch|amount|ERROR=|PASS=" | head -8
sql "select typeof(amount) as type_still_in_the_lake from lake.analytics_canonical.sales_order limit 1"

echo "== T6. Can this stack run incremental models? (not used by the project; a capability check)"
rm -rf /tmp/incr && cp -r /opt/datalake/dbt /tmp/incr
cat > /tmp/incr/models/marts/zz_incremental_check.sql <<'EOF'
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge',
          tags=['internal'], meta={'owner': 'sales-data', 'openmetadata': {'domain': 'sales'}}) }}
select order_id, amount, loaded_at
from {{ ref('stg_orders') }}
where reject_reason is null
{% if is_incremental() %}
  and loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
EOF
for attempt in first second; do
  $DBT run --select zz_incremental_check --project-dir /tmp/incr --profiles-dir /tmp/incr \
    --target-path /tmp/incr-target --log-path /tmp/incr-logs --no-use-colors 2>&1 \
    | grep -aE "OK created|ERROR|rows\)" | sed "s/^/    $attempt run: /" | cut -c1-150
done
sql "select count(*) as rows_in_incremental_table from lake.analytics_marts.zz_incremental_check"
sql "drop table if exists lake.analytics_marts.zz_incremental_check"

echo "== Final state"
sql "select order_date, country, orders, revenue from lake.analytics_marts.agg_daily_revenue where order_date >= date '2026-11-06' order by 1, 2"
dl check-tags --manifest /opt/datalake/work/target/manifest.json
