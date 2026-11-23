#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)
# - no-replicated-database: the replica of the other shard applies each `ALTER` again to the same table directory

# Iceberg format version 1 metadata written before `partition-specs` existed has only the deprecated
# `partition-spec` field: the fields of the only partition spec, whose id is 0. It must be read as that spec.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_V2="t2_${CLICKHOUSE_DATABASE}"
TABLE_DIR="${USER_FILES_PATH}/${TABLE}"
TABLE_V2_DIR="${USER_FILES_PATH}/${TABLE_V2}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${TABLE};
        DROP TABLE IF EXISTS ${TABLE_V2}"
    rm -rf "${TABLE_DIR}" "${TABLE_V2_DIR}"
}
cleanup
trap cleanup EXIT

query()
{
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "$1"
}

# Writes the latest metadata again as the next version, with only `partition-spec` (the fields of the default spec).
drop_partition_specs()
{
    python3 - "$1/metadata" <<'EOF'
import json, os, re, sys
directory = sys.argv[1]
versions = [int(m.group(1)) for m in (re.fullmatch(r"v(\d+)\.metadata\.json", f) for f in os.listdir(directory)) if m]
latest = max(versions)
m = json.load(open(os.path.join(directory, "v%d.metadata.json" % latest)))
default = [s for s in m["partition-specs"] if s["spec-id"] == m["default-spec-id"]]
m["partition-spec"] = default[0]["fields"]
del m["partition-specs"]
del m["default-spec-id"]
json.dump(m, open(os.path.join(directory, "v%d.metadata.json" % (latest + 1)), "w"), indent=4)
EOF
}

# Inspects the latest metadata file of the table.
check()
{
    local label=$1
    local table_dir=$2
    local latest
    latest=$(find "${table_dir}/metadata" -name 'v*.metadata.json' | sort -V | tail -1)
    python3 - "${label}" "${latest}" <<'EOF'
import json, sys
label, path = sys.argv[1], sys.argv[2]
m = json.load(open(path))
default = [s for s in m.get("partition-specs", []) if s.get("spec-id") == m.get("default-spec-id")]
spec = m.get("partition-spec")
print(label,
      "has_partition_specs=%d" % ("partition-specs" in m),
      "default_spec_id=%s" % m.get("default-spec-id", "none"),
      "spec_in_sync=%d" % (spec is not None and len(default) == 1 and spec == default[0].get("fields")),
      "spec_fields=%s" % (",".join(f["name"] for f in spec) if spec is not None else "none"))
EOF
}

query "CREATE TABLE ${TABLE} (id Int32, p String) ENGINE = IcebergLocal('${TABLE_DIR}/') PARTITION BY p SETTINGS iceberg_format_version = 1"
query "INSERT INTO ${TABLE} VALUES (1, 'a')"
query "INSERT INTO ${TABLE} VALUES (2, 'b')"
drop_partition_specs "${TABLE_DIR}"
check legacy "${TABLE_DIR}"

# The manifest list is pruned by `partition-spec`: the manifest of partition 'a' is skipped.
query "
    SELECT id FROM ${TABLE} WHERE p = 'b'
        SETTINGS use_iceberg_partition_pruning = 1, use_iceberg_manifest_list_partition_pruning = 1,
            iceberg_metadata_log_level = 'none', log_comment = '${CLICKHOUSE_DATABASE}_legacy_pruning';
    SYSTEM FLUSH LOGS query_log;"
query "
    SELECT ProfileEvents['IcebergPartitionPrunedManifestFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment = '${CLICKHOUSE_DATABASE}_legacy_pruning'
    SETTINGS enable_parallel_replicas = 0"

# ALTER resolves the default spec from `partition-spec` and writes `partition-specs` and `default-spec-id` too.
query "ALTER TABLE ${TABLE} ADD COLUMN x Nullable(Int64)"
check add_column "${TABLE_DIR}"

# INSERT partitions the rows by `partition-spec`: one data file per partition.
drop_partition_specs "${TABLE_DIR}"
query "INSERT INTO ${TABLE} VALUES (3, 'a', 30), (4, 'c', 40)"
check insert "${TABLE_DIR}"
query "SELECT uniqExact(_path) FROM ${TABLE} WHERE id IN (3, 4)"
query "SELECT * FROM ${TABLE} ORDER BY id"

# Format version 2 requires `partition-specs`, `partition-spec` alone is not read.
query "CREATE TABLE ${TABLE_V2} (id Int32, p String) ENGINE = IcebergLocal('${TABLE_V2_DIR}/') PARTITION BY p SETTINGS iceberg_format_version = 2"
drop_partition_specs "${TABLE_V2_DIR}"
query "INSERT INTO ${TABLE_V2} VALUES (1, 'a')" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
