#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Iceberg needs Avro for its metadata
# Tag no-replicated-database: a full-definition `ATTACH TABLE ... UUID` is not supported there

# An insert into an Iceberg table with a text data file format used `ParallelFormattingOutputFormat`, whose thread
# writes to the file while the insert reads the file size to decide where to cut it (a data race under TSan).
#
# `CREATE` of a data lake table rejects a data file format that the table format cannot store (such as `JSONEachRow`),
# but a table defined before that check still loads through `ATTACH`, so the code path stays reachable. Reproduce it:
# create the Iceberg metadata with `Parquet`, drop the table keeping its files, and attach a full definition that
# uses `JSONEachRow`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_PATH="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}/05632_iceberg_insert_text_format_file_cut/"
TABLE_UUID=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --send_logs_level=error --query "
SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
SET output_format_parallel_formatting = 1, max_threads = 4, max_insert_threads = 1, max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0;
SET iceberg_insert_max_bytes_in_data_file = 100000;

CREATE TABLE t_init (k Int64, s String) ENGINE = IcebergLocal('${DATA_PATH}', 'Parquet');
DROP TABLE t_init SYNC;

ATTACH TABLE t UUID '${TABLE_UUID}' (k Int64, s String) ENGINE = IcebergLocal('${DATA_PATH}', 'JSONEachRow');
INSERT INTO t SELECT number, repeat('x', number % 100) FROM numbers(50000);

SELECT count(), sum(k), sum(length(s)) FROM t;
-- The data was cut into several files, all of them in the text format.
SELECT uniqExact(_path) > 1, countIf(NOT endsWith(_path, '.jsoneachrow')) FROM t;

DROP TABLE t SYNC;
"

rm -rf "${DATA_PATH:?}"
