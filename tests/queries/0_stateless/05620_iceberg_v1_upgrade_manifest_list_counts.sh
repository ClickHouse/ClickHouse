#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# The first commit after an external upgrade of an Iceberg table from format version 1 carries the manifests of the
# v1 manifest list over into a v2 or v3 one, which must keep their counts in the new list's shape. Iceberg Java before
# 1.5 names the file counts `added_data_files_count` and so on.
# https://github.com/ClickHouse/ClickHouse/issues/123045

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_v1_upgrade_counts"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# What an external engine writes on an upgrade: a new metadata version with the new format version, the snapshots
# left untouched.
upgrade()
{
    python3 - "$1/metadata" "$2" <<'PY'
import glob, json, os, re, sys
metadata_dir, version = sys.argv[1], int(sys.argv[2])
latest = max(glob.glob(os.path.join(metadata_dir, "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
number = int(re.search(r"/v(\d+)\.metadata\.json$", latest).group(1))
with open(latest) as f:
    metadata = json.load(f)
metadata["format-version"] = version
metadata.setdefault("last-sequence-number", 0)
if version == 3:
    metadata["next-row-id"] = 0
with open(os.path.join(metadata_dir, f"v{number + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY
}

# The local path of the manifest list of the current snapshot.
current_list()
{
    python3 - "$1/metadata" <<'PY'
import glob, json, os, re, sys
metadata_dir = sys.argv[1]
latest = max(glob.glob(os.path.join(metadata_dir, "v*.metadata.json")),
             key=lambda path: int(re.search(r"/v(\d+)\.metadata\.json$", path).group(1)))
with open(latest) as f:
    metadata = json.load(f)
snapshot = next(s for s in metadata["snapshots"] if s["snapshot-id"] == metadata["current-snapshot-id"])
print(os.path.join(metadata_dir, os.path.basename(snapshot["manifest-list"])))
PY
}

COUNTS="content Int32, added_files_count Int32, existing_files_count Int32, added_rows_count Int64, existing_rows_count Int64, deleted_rows_count Int64"

print_counts()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(added_rows_count + existing_rows_count), sum(deleted_rows_count), countIf(added_files_count + existing_files_count = 0)
        FROM file('$(current_list "$1")', Avro, '${COUNTS}') WHERE content = 0"
}

echo "v1 -> v2"
T1="${LAKE}/v2"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_v1_v2 (x Int32, p Int32) ENGINE = IcebergLocal('${T1}') PARTITION BY p SETTINGS iceberg_format_version = 1;
    INSERT INTO t_v1_v2 VALUES (1, 1), (2, 2);
    DROP TABLE t_v1_v2;
"
upgrade "${T1}" 2
for row in "(3, 3)" "(4, 1)"; do
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_insert_into_iceberg = 1;
        INSERT INTO TABLE FUNCTION icebergLocal('${T1}', 'Parquet', 'x Int32, p Int32') VALUES ${row};
    "
    ${CLICKHOUSE_CLIENT} --query "SELECT x, p FROM icebergLocal('${T1}') ORDER BY x"
    ${CLICKHOUSE_CLIENT} --query "SELECT x FROM icebergLocal('${T1}') WHERE p = 2"
    print_counts "${T1}"
done

echo "v1 -> v3"
T2="${LAKE}/v3"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_v1_v3 (x Int32) ENGINE = IcebergLocal('${T2}') SETTINGS iceberg_format_version = 1;
    INSERT INTO t_v1_v3 VALUES (1), (2);
    DROP TABLE t_v1_v3;
"
upgrade "${T2}" 3
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${T2}', 'Parquet', 'x Int32') VALUES (3);
"
${CLICKHOUSE_CLIENT} --query "SELECT x, _row_id FROM icebergLocal('${T2}') ORDER BY x"
print_counts "${T2}"

echo "Iceberg Java 1.4 names"
T3="${LAKE}/java"
cp -r "${CUR_DIR}/data_lake_corpus/iceberg/ice143_v2_position_deletes" "${T3}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${T3}')"
OLD=$(current_list "${T3}")
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${T3}') VALUES (100, 'x', 1, '2024-01-01 00:00:00');
"
NEW=$(current_list "${T3}")
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf((n.added_files_count, n.existing_files_count, n.deleted_files_count)
        = (o.added_data_files_count, o.existing_data_files_count, o.deleted_data_files_count))
    FROM file('${NEW}', Avro, 'manifest_path String, added_files_count Int32, existing_files_count Int32, deleted_files_count Int32') AS n
    INNER JOIN file('${OLD}', Avro, 'manifest_path String, added_data_files_count Int32, existing_data_files_count Int32, deleted_data_files_count Int32') AS o
    ON n.manifest_path = o.manifest_path"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${T3}')"

rm -rf "${LAKE}"
