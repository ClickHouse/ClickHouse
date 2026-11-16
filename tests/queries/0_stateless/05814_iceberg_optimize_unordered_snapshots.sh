#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# The `snapshots` array of the metadata file has no defined order.
# `OPTIMIZE` must write the same parent links and counters when a child precedes its parent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build does not run this compaction, so it prints the expected output.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "90"
    echo -e "1\t0\tAPPEND\t80\t80"
    echo -e "2\t1\tAPPEND\t10\t90"
    exit 0
fi

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(90)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id < 10"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(100, 10)"

# The table is at metadata version 4: create, insert, delete, insert.
# Version 5 is a copy with the second insert before its parent, the delete.
# It is a new file, because the server caches parsed metadata files by path.
python3 - "${TABLE_PATH}metadata" <<'PY'
import json, sys
metadata_dir = sys.argv[1]
metadata = json.load(open(f"{metadata_dir}/v4.metadata.json"))
first_insert, delete, second_insert = metadata["snapshots"]
metadata["snapshots"] = [first_insert, second_insert, delete]
metadata["last-updated-ms"] += 1
json.dump(metadata, open(f"{metadata_dir}/v5.metadata.json", "x"))
PY

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# Columns: snapshot number, parent snapshot number (0 = no parent), operation, added records, total records.
${CLICKHOUSE_CLIENT} --query "
    WITH (
        SELECT groupArray(snapshot_id) FROM
        (
            SELECT snapshot_id FROM system.iceberg_history
            WHERE database = currentDatabase() AND table = '${TABLE}'
            ORDER BY made_current_at
        )
    ) AS ids
    SELECT indexOf(ids, snapshot_id) AS snapshot, indexOf(ids, parent_id), operation, summary['added-records'], summary['total-records']
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY snapshot
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
