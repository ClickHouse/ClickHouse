#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-replicated-database: there the ALTER runs as replicated DDL, which returns the DDL status instead of the result

# `expire_snapshots` and `remove_orphan_files` keep a live file that the metadata names through a symlinked directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --session_timezone=UTC --allow_insert_into_iceberg=1 --allow_expire_snapshots=1 --allow_iceberg_remove_orphan_files=1 --iceberg_orphan_files_min_retention_seconds=0 --use_iceberg_metadata_files_cache=0"
ROOT="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
trap 'rm -rf "${ROOT}"' EXIT

# Creates table $1 with two snapshots and the symlink `metadata_alias -> metadata` in its directory.
populate()
{
    TABLE="t_$1_${CLICKHOUSE_DATABASE}"
    DIR="${ROOT}/${TABLE}"
    mkdir -p "${DIR}/metadata"
    ln -s metadata "${DIR}/metadata_alias"
    $CH -q "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${DIR}/') SETTINGS allow_experimental_iceberg_compaction = 0"
    $CH -q "INSERT INTO ${TABLE} VALUES (1)"
    $CH -q "INSERT INTO ${TABLE} VALUES (2)"
}

# Writes the next metadata version where the manifest list of snapshot $1 (`current` or `parent`) is the current
# snapshot's manifest list spelled through `metadata_alias`, and prints the real path of that manifest list.
respell_manifest_list()
{
    python3 - "${DIR}/metadata" "$1" <<'PY'
import json, os, re, sys
d, which = sys.argv[1], sys.argv[2]
latest = max(int(m.group(1)) for f in os.listdir(d) if (m := re.fullmatch(r"v(\d+)\.metadata\.json", f)))
meta = json.load(open(os.path.join(d, f"v{latest}.metadata.json")))
snapshots = {s["snapshot-id"]: s for s in meta["snapshots"]}
current = snapshots[meta["current-snapshot-id"]]
target = current if which == "current" else snapshots[current["parent-snapshot-id"]]
manifest_list = current["manifest-list"]
target["manifest-list"] = manifest_list.replace("/metadata/", "/metadata_alias/")
json.dump(meta, open(os.path.join(d, f"v{latest + 1}.metadata.json"), "w"))
print("metadata/" + os.path.basename(manifest_list))
PY
}

exists()
{
    if [ -f "${DIR}/$1" ]; then echo "manifest list present"; else echo "manifest list absent"; fi
}

data()
{
    $CH -q "SELECT groupArray(x) FROM (SELECT x FROM ${TABLE} ORDER BY x)"
}

echo "--- expire_snapshots: an expired snapshot names the retained manifest list through the symlink"
populate expire
manifest_list=$(respell_manifest_list parent)
$CH -q "ALTER TABLE ${TABLE} EXECUTE expire_snapshots(retention_period = '1ms')" | grep -E "deleted_manifest_lists_count"
exists "${manifest_list}"
data
$CH -q "DROP TABLE ${TABLE}"

echo "--- remove_orphan_files: the current snapshot names its manifest list through the symlink"
populate orphans
manifest_list=$(respell_manifest_list current)
sleep 1.1
older_than=$($CH -q "SELECT toString(now())")
$CH -q "ALTER TABLE ${TABLE} EXECUTE remove_orphan_files(older_than = '${older_than}')" | grep -E "deleted_manifest_lists_count"
exists "${manifest_list}"
data
$CH -q "DROP TABLE ${TABLE}"
