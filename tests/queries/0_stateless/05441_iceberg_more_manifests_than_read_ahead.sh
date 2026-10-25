#!/usr/bin/env bash
# A table with more manifest files than are read ahead at once is read whole, with one and with several reading threads, with and without the metadata cache.
#
# Tags: no-fasttest, no-msan
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-msan: under MSan each single-threaded read of the 73 manifests that misses the metadata cache takes 2-3 minutes, so the test runs close to the 600 s timeout

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"
TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"
trap 'rm -rf "${TABLE_PATH}"' EXIT

$CH --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
# One data manifest per INSERT.
$CH --query "$(for i in $(seq 70); do echo "INSERT INTO ${TABLE} VALUES ($i);"; done)"

$CH --query "ALTER TABLE ${TABLE} DELETE WHERE id = 5; ALTER TABLE ${TABLE} DELETE WHERE id = 40; ALTER TABLE ${TABLE} DELETE WHERE id = 70;"
# The trivial count reads the 70 data manifests before it meets a delete manifest and falls back to reading the table.
$CH --query "SELECT count() FROM ${TABLE} SETTINGS optimize_trivial_count_query = 1, use_iceberg_metadata_files_cache = 0, max_threads = 1"

# More than 64 data manifests and at least one delete manifest.
PREMISE_ID="${CLICKHOUSE_DATABASE}_premise_${RANDOM}${RANDOM}"
$CH --query_id "$PREMISE_ID" --query "SELECT * FROM ${TABLE} SETTINGS iceberg_metadata_log_level = 'manifest_list_entry', use_iceberg_metadata_files_cache = 0, max_threads = 1 FORMAT Null"
$CH --query "SYSTEM FLUSH LOGS iceberg_metadata_log"
$CH --query "
    SELECT countIf(c = 0) > 64, countIf(c = 1) > 0
    FROM (
        SELECT JSONExtractString(content, 'manifest_path') AS path, any(JSONExtractInt(content, 'content')) AS c
        FROM system.iceberg_metadata_log
        WHERE query_id = '$PREMISE_ID' AND content_type = 'ManifestListEntry'
        GROUP BY path)"

# The table keeps the metadata cache it was created with, so the uncached reads go through the table function.
$CH --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}/') SETTINGS max_threads = 1, use_iceberg_metadata_files_cache = 0"
$CH --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}/') SETTINGS max_threads = 8, use_iceberg_metadata_files_cache = 0"
$CH --query "SELECT count(), sum(id) FROM ${TABLE} SETTINGS max_threads = 8, use_iceberg_metadata_files_cache = 1"
$CH --query "SELECT count(), sum(id) FROM ${TABLE} SETTINGS max_threads = 8, use_iceberg_metadata_files_cache = 1"

$CH --query "DROP TABLE ${TABLE}"
