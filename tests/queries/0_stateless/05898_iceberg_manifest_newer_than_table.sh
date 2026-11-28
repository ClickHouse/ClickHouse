#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Iceberg writes.

# A table upgrade keeps the older manifests, but a manifest is never newer than its table.
# https://github.com/ClickHouse/ClickHouse/issues/130002

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_manifest_newer_than_table"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# A new metadata version that declares the given format version, the snapshots left untouched.
set_format_version()
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
with open(os.path.join(metadata_dir, f"v{number + 1}.metadata.json"), "w") as f:
    json.dump(metadata, f, indent=4)
PY
}

# A table opened as v1 and upgraded to v2 by another writer reads its v1 and v2 manifests.
echo "upgraded"
T1="${LAKE}/upgraded"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_upgraded (x Int32) ENGINE = IcebergLocal('${T1}') SETTINGS iceberg_format_version = 1;
    INSERT INTO t_upgraded VALUES (1), (2);
"
set_format_version "${T1}" 2
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    INSERT INTO TABLE FUNCTION icebergLocal('${T1}', 'Parquet', 'x Int32') VALUES (3);
"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_upgraded ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_upgraded"

# v1 metadata over the v2 manifests of a v2 table.
echo "manifest newer than the table"
T2="${LAKE}/downgraded"
${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_downgraded (x Int32) ENGINE = IcebergLocal('${T2}');
    INSERT INTO t_downgraded VALUES (1), (2);
    DROP TABLE t_downgraded;
"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM icebergLocal('${T2}') ORDER BY x"
set_format_version "${T2}" 1
error=$(${CLICKHOUSE_CLIENT} --query "SELECT x FROM icebergLocal('${T2}') ORDER BY x" 2>&1)
if grep -q -F 'ICEBERG_SPECIFICATION_VIOLATION' <<< "$error" \
    && grep -q -F "has format-version 2, which is newer than the table's format-version 1" <<< "$error"; then
    echo 'rejected'
else
    echo 'not rejected'
fi

rm -rf "${LAKE}"
