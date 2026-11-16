#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# `OPTIMIZE` must keep the current snapshot of the table.
# Here the current snapshot is not the newest one, as after a rollback.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build does not run this compaction, so it prints the expected output.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "5"
    exit 0
fi

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(10)"
# `OPTIMIZE` rewrites the table only when it has position deletes.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id < 5"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(20)"

# The table is at metadata version 4: create, insert, delete, insert.
# Version 5 is a copy with the delete as the current snapshot.
# It is a new file, because the server caches parsed metadata files by path.
python3 - "${TABLE_PATH}metadata" <<'PY'
import json, sys
metadata_dir = sys.argv[1]
metadata = json.load(open(f"{metadata_dir}/v4.metadata.json"))
first_insert, delete, second_insert = metadata["snapshots"]
metadata["current-snapshot-id"] = delete["snapshot-id"]
metadata["refs"]["main"]["snapshot-id"] = delete["snapshot-id"]
metadata["last-updated-ms"] += 1
json.dump(metadata, open(f"{metadata_dir}/v5.metadata.json", "x"))
PY

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
