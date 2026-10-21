#!/usr/bin/env bash
# Run one numbered query from lab_queries.sql:   ./run_lab_sql.sh 1
set -euo pipefail
source env.sh
N="${1:?Usage: ./run_lab_sql.sh QUERY_NUMBER   (1, 2 or 3)}"
python3 - "$PROJECT_ID" "$N" <<'PY'
import re, subprocess, sys
project, n = sys.argv[1], sys.argv[2]
sql = open("lab_queries.sql").read().replace("YOUR_PROJECT", project)
blocks = re.split(r"^-- ## ", sql, flags=re.M)[1:]
match = [b for b in blocks if b.startswith(n + ".")]
if not match:
    sys.exit(f"No query number {n} in lab_queries.sql")
title, _, query = match[0].partition("\n")
if "___" in query:
    sys.exit("Query " + n + " still has ___ blanks. Open lab_queries.sql and fill them in first.")
print("=== " + title.strip())
subprocess.run(["bq", "query", "--use_legacy_sql=false", "--format=pretty", query], check=False)
PY
