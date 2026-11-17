#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro and Iceberg integrations are not available in the fast test build.

# Under parallel parsing, the Iceberg field-id column mapper must reach every per-segment Avro parser.
# An INSERT planned before `ADD COLUMN` commits a data file under the widened schema that lacks `d`. Only
# the mapper tells, by field id, that `d` is absent from that file and must read as NULL
# (`input_format_avro_allow_missing_fields` is disabled, so a parser without the mapper throws).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
rm -rf "${TABLE_PATH}"
mkdir -p "${TABLE_PATH}"

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --input_format_avro_allow_missing_fields=0"
TABLE="CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro')"

# A small sync interval writes many Avro blocks per data file, so a tiny `min_chunk_bytes_for_parallel_parsing`
# cuts every file into many segments.
${LOCAL} --query "
    CREATE TABLE t (a Int32, c Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro');
    INSERT INTO t SELECT number, number * 10 FROM numbers(2000) SETTINGS output_format_avro_sync_interval = 256;
"
PIN="metadata/$(basename "$(ls "${TABLE_PATH}metadata"/*.metadata.json | sort -V | tail -1)")"
${LOCAL} --query "
    ${TABLE};
    ALTER TABLE t ADD COLUMN d Nullable(Int64);
"
# The structure comes from the pinned version, the commit goes to the widened table.
${LOCAL} --query "
    ${TABLE} SETTINGS iceberg_metadata_file_path = '${PIN}';
    INSERT INTO t SELECT number + 2000, (number + 2000) * 10 FROM numbers(2000) SETTINGS output_format_avro_sync_interval = 256;
"
${LOCAL} --query "
    ${TABLE};
    INSERT INTO t SELECT number + 4000, (number + 4000) * 10, number FROM numbers(2000) SETTINGS output_format_avro_sync_interval = 256;
"

PAR="input_format_parallel_parsing=1, max_threads=8, max_parsing_threads=8, min_chunk_bytes_for_parallel_parsing=64"

# Every segment is parsed into its own blocks, so more blocks than data files (3) proves several segments.
echo -n "several segments: "; ${LOCAL} --query "
    ${TABLE};
    SELECT count() > 3 FROM (SELECT blockSize() FROM t) SETTINGS ${PAR}, max_block_size = 100000"

echo "parallel:"
${LOCAL} --query "
    ${TABLE};
    SELECT count(), countIf(d IS NULL), sum(a), sum(c), sum(d) FROM t SETTINGS ${PAR}"

# The same rows as the sequential read, compared as a multiset: the parallel output is not ordered.
seq=$(${LOCAL} --query "${TABLE}; SELECT * FROM t SETTINGS input_format_parallel_parsing = 0 FORMAT TSV")
par=$(${LOCAL} --query "${TABLE}; SELECT * FROM t SETTINGS ${PAR} FORMAT TSV")
if [ -z "$seq" ]; then
    echo "sequential read returned nothing"
else
    [ "$(printf '%s' "$seq" | sort | md5sum)" = "$(printf '%s' "$par" | sort | md5sum)" ] && echo "same as sequential: OK" || echo "same as sequential: MISMATCH"
fi

rm -rf "${TABLE_PATH}"
