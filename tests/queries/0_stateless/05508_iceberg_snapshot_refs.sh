#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-replicated-database: ALTER EXECUTE returns the command result, not replicated DDL status

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_experimental_iceberg_snapshot_refs=1 --use_iceberg_metadata_files_cache=0 --async_insert=0"

TABLE="t_iceberg_refs_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
rm -rf "${TABLE_PATH}"
trap "rm -rf '${TABLE_PATH}'" EXIT

$CH --query "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS allow_experimental_iceberg_compaction = 0"

echo "phase1_python_branch"
for i in 1 2 3; do
    $CH --query "INSERT INTO ${TABLE} VALUES (${i})"
done

echo "default_count"
$CH --query "SELECT count() FROM ${TABLE}"

# Point branch `b` at the oldest snapshot (one row).
python3 - "${TABLE_PATH}metadata" <<'PY'
import json, os, re, sys
d = sys.argv[1]
latest = max(int(m.group(1)) for f in os.listdir(d) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
meta = json.load(open(os.path.join(d, f"v{latest}.metadata.json")))
parent = {s["snapshot-id"]: s.get("parent-snapshot-id") for s in meta["snapshots"]}
chain = [meta["current-snapshot-id"]]
while parent.get(chain[-1]) in parent:
    chain.append(parent[chain[-1]])
chain.reverse()
meta.setdefault("refs", {})["b"] = {"snapshot-id": chain[0], "type": "branch"}
json.dump(meta, open(os.path.join(d, f"v{latest + 1}.metadata.json"), "w"))
PY

echo "ref_b_count"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'b'"

echo "ref_main_count"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'main'"

echo "unknown_ref"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'missing'" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "ref_and_snapshot_id"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'b', iceberg_snapshot_id = 1" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "ref_and_timestamp"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'b', iceberg_timestamp_ms = 1" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"

echo "phase2_create_drop"
$CH --query "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS allow_experimental_iceberg_compaction = 0"
$CH --query "INSERT INTO ${TABLE} VALUES (1)"
$CH --query "INSERT INTO ${TABLE} VALUES (2)"

echo "create_branch"
$CH --query "ALTER TABLE ${TABLE} EXECUTE create_snapshot_ref(name = 'probe')" | cut -f1,2,4

echo "create_tag"
$CH --query "ALTER TABLE ${TABLE} EXECUTE create_snapshot_ref(name = 'v1', type = 'tag')" | cut -f1,2,4

$CH --query "INSERT INTO ${TABLE} VALUES (3)"

echo "main_after_insert"
$CH --query "SELECT count() FROM ${TABLE}"
echo "branch_after_insert"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'probe'"
echo "tag_after_insert"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'v1'"

echo "if_not_exists"
$CH --query "ALTER TABLE ${TABLE} EXECUTE create_snapshot_ref(name = 'probe', if_not_exists = 1)" | cut -f1,2,4

echo "duplicate_ref"
$CH --query "ALTER TABLE ${TABLE} EXECUTE create_snapshot_ref(name = 'probe')" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "drop_main"
$CH --query "ALTER TABLE ${TABLE} EXECUTE drop_snapshot_ref(name = 'main')" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "insert_with_ref"
$CH --query "INSERT INTO ${TABLE} VALUES (9) SETTINGS iceberg_ref = 'probe'" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "execute_without_gate"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --allow_experimental_iceberg_snapshot_refs=0 --query \
    "ALTER TABLE ${TABLE} EXECUTE create_snapshot_ref(name = 'other')" 2>&1 | grep -o 'SUPPORT_IS_DISABLED' | head -1

echo "drop_probe"
$CH --query "ALTER TABLE ${TABLE} EXECUTE drop_snapshot_ref(name = 'probe')" | cut -f1,2,4

echo "read_dropped"
$CH --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_ref = 'probe'" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

echo "if_exists"
$CH --query "ALTER TABLE ${TABLE} EXECUTE drop_snapshot_ref(name = 'probe', if_exists = 1)" | cut -f1,2,4

echo "drop_missing"
$CH --query "ALTER TABLE ${TABLE} EXECUTE drop_snapshot_ref(name = 'probe')" 2>&1 | grep -o 'BAD_ARGUMENTS' | head -1

$CH --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
