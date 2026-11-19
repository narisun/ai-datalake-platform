#!/bin/bash
# Real-world ingestion scenarios, run against the live dev stack after `bootstrap.py`
# and a first `dl ingest`. Run inside the airflow container:
#
#   bash /opt/datalake/dev/scenarios.sh
#
# Each scenario drops files into the landing container and runs `dl ingest`.
# It changes the sample data; `landing.py seed` plus a fresh ingest restores it.
set -uo pipefail

PY=/opt/datalake/venv/bin/python
LANDING="$PY /opt/datalake/dev/landing.py"
TMP=$(mktemp -d)

sql() { "$PY" /opt/datalake/dev/trino_dev.py "$1"; }

run() { dl ingest 2>&1 | tail -1; echo "    exit code: ${PIPESTATUS[0]}"; }

echo "== 2. Run again with nothing new"
run

echo "== 3. A new file with a new column and messy text; and a corrected re-upload"
printf '\xef\xbb\xbfOrder ID,Customer ID,Ordered At,Amount,Channel,Note\r\n15,3,2026-11-03 08:00:00,45.00,web,"Leave with neighbour, No. 4"\r\n16,1,2026-11-03 09:30:00,12.50,store,"Customer said: ""it'"'"'s a gift"""\r\n' > "$TMP/orders_2026-11-03.csv"
$LANDING put "$TMP/orders_2026-11-03.csv" shop/orders/orders_2026-11-03.csv
# The sender corrects order 10's amount and withdraws order 12.
printf 'Order ID,Customer ID,Ordered At,Amount\n10,1,2026-11-01 10:00:00,99.00\n11,2,2026-11-01 11:15:00,80.00\n' > "$TMP/orders_2026-11-01.csv"
$LANDING put "$TMP/orders_2026-11-01.csv" shop/orders/orders_2026-11-01.csv
run
sql "select order_id, amount, channel, note, _source_file from lake.raw.orders order by order_id"

echo "== 4. A bad file (no Order ID column) beside a good customer file"
printf 'Customer ID,Ordered At,Amount\n2,2026-11-04 10:00:00,20.00\n' > "$TMP/orders_2026-11-04.csv"
$LANDING put "$TMP/orders_2026-11-04.csv" shop/orders/orders_2026-11-04.csv
"$PY" - "$TMP/customers_2026-11-02.xlsx" <<'PYEOF'
import sys, openpyxl
workbook = openpyxl.Workbook()
sheet = workbook.active
for row in [
    ["Customer ID", "Full Name", "Email", "Country"],
    [1, "Ada Lovelace", "ada@example.com", "GB"],
    [2, "Alan Turing", "alan@example.com", "GB"],
    [3, "Grace O'Brien-Hopper", "grace@example.com", "US"],
    [4, "Katherine Johnson", "katherine@example.com", "US"],
]:
    sheet.append(row)
workbook.save(sys.argv[1])
PYEOF
$LANDING put "$TMP/customers_2026-11-02.xlsx" shop/customers/customers_2026-11-02.xlsx
run
sql "select count(*) as customers, count(distinct _source_file) as files from lake.raw.customers"
dl runs --process ingest --limit 1 | cut -c1-230

echo "== 5. Run again: the same bad file is not reported twice"
run

echo "== 6. The sender fixes the bad file"
printf 'Order ID,Customer ID,Ordered At,Amount\n17,2,2026-11-04 10:00:00,20.00\n' > "$TMP/orders_2026-11-04.csv"
$LANDING put "$TMP/orders_2026-11-04.csv" shop/orders/orders_2026-11-04.csv
run

echo "== 7. A larger file: 20,000 rows"
"$PY" - "$TMP/orders_2026-11-05.csv" <<'PYEOF'
import sys
with open(sys.argv[1], "w", newline="") as handle:
    handle.write("Order ID,Customer ID,Ordered At,Amount\n")
    for n in range(20000):
        handle.write(f"{100000 + n},{1 + n % 4},2026-11-05 12:00:00,{10 + n % 90}.50\n")
PYEOF
$LANDING put "$TMP/orders_2026-11-05.csv" shop/orders/orders_2026-11-05.csv
SECONDS=0
run
echo "    took ${SECONDS}s"
sql "select count(*) as orders, count(distinct _source_file) as files from lake.raw.orders"

echo "== Ledger"
dl files
