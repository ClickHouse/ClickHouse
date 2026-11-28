#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# Checkpoints and file skipping over the tables of delta-kernel-rs `kernel/tests/data` (see
# `data_delta_lake/kernel_data/README.md`). Expected values are those of the kernel tests
# `kernel/tests/integration/log/v2_checkpoints.rs`, `kernel/tests/integration/read.rs` and
# `kernel/src/scan/data_skipping/tests.rs`:
# - V2 checkpoints (JSON and Parquet manifests, with and without sidecar files, with and without
#   `_last_checkpoint`) and V2 "classic" checkpoints.
# - File skipping by statistics stored only as structs (`stats_parsed`, `partitionValues_parsed`) in
#   single-part, multi-part, V2 sidecar and V2 classic checkpoints.
# - File skipping by partition values and by statistics, compared with the number of files that
#   the kernel selects, and the absence of over-pruning with millisecond-truncated timestamp
#   statistics.
# `DeltaLakeScannedFiles` counts the data files a query reads. The checks run in one
# clickhouse-local launch, so the per-query value is the difference of consecutive reads of the
# cumulative counter, computed by the `awk` filter at the bottom. A `bounded` check is used where
# ClickHouse skips fewer files than the kernel: it prints 1 when the number of scanned files is in
# [kernel selection, all files], so that the result rows are checked and better pruning is allowed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

QUERIES="SET session_timezone = 'UTC';"
COUNTER="sumIf(value, event = 'DeltaLakeScannedFiles')"

# exact <table> <expected ids query>: the table contains exactly the expected multiset of `id`.
exact() {
    local table=$1 expected=$2
    QUERIES+="
SELECT '--- ${table}';
DROP TABLE IF EXISTS ids;
CREATE TABLE ids ENGINE = Memory AS SELECT toInt64(id) AS id FROM deltaLakeLocal('${DATA}/${table}');
SELECT count(), sum(id) FROM ids;
SELECT 'missing', count() FROM (SELECT toInt64(id) FROM (${expected}) EXCEPT ALL SELECT id FROM ids);
SELECT 'unexpected', count() FROM (SELECT id FROM ids EXCEPT ALL SELECT toInt64(id) FROM (${expected}));"
}

# skip <table> <columns> <where> <scanned files>: rows and the exact number of scanned files.
skip() {
    local table=$1 columns=$2 where=$3 files=$4
    QUERIES+="
SELECT '--- ${table} WHERE ${where//\'/\"}';
SELECT 'counter', ${COUNTER} FROM system.events;
SELECT ${columns} FROM deltaLakeLocal('${DATA}/${table}') WHERE ${where} ORDER BY ALL;
SELECT 'scanned', ${COUNTER}, ${files} FROM system.events;"
}

# bounded <table> <columns> <where> <kernel selection> <all files>
bounded() {
    local table=$1 columns=$2 where=$3 low=$4 high=$5
    QUERIES+="
SELECT '--- ${table} WHERE ${where//\'/\"}';
SELECT 'counter', ${COUNTER} FROM system.events;
SELECT ${columns} FROM deltaLakeLocal('${DATA}/${table}') WHERE ${where} ORDER BY ALL;
SELECT 'bounded', ${COUNTER}, ${low}, ${high} FROM system.events;"
}

ids() { echo "SELECT number AS id FROM numbers($1)"; }

SIDECAR_IDS="SELECT 0 AS id FROM numbers(3) UNION ALL $(ids 30) UNION ALL $(ids 100) UNION ALL $(ids 100) UNION ALL $(ids 1000)"

QUERIES+="SELECT '=== V2 checkpoints';"
exact v2-checkpoints-json-with-sidecars "${SIDECAR_IDS}"
exact v2-checkpoints-parquet-with-sidecars "${SIDECAR_IDS}"
exact v2-checkpoints-json-without-sidecars "$(ids 10) UNION ALL SELECT 2718 AS id"
exact v2-checkpoints-parquet-without-sidecars "$(ids 10) UNION ALL SELECT 2718 AS id"
exact v2-classic-checkpoint-json "$(ids 20)"
exact v2-classic-checkpoint-parquet "$(ids 20)"
exact v2-checkpoints-json-with-last-checkpoint "$(ids 10)"
exact v2-checkpoints-parquet-with-last-checkpoint "$(ids 10)"

QUERIES+="SELECT '=== statistics only as structs in checkpoints';"
for table in v1-single-part v1-multi-part v2-parquet-sidecars v2-json-sidecars v2-classic-parquet; do
    skip "${table}-struct-stats-only" 'id, value' 'id > 3' 2
done
skip v1-multi-part-partitioned-struct-stats-only 'id, value, part' 'part = 0' 1

QUERIES+="SELECT '=== partition values and statistics';"
skip app-txn-checkpoint 'count()' "modified = '2021-02-01'" 2
skip app-txn-checkpoint 'count()' "modified = '2099-01-01'" 0
skip app-txn-checkpoint 'count()' "modified != '2021-02-01'" 2
skip app-txn-checkpoint 'count()' "modified > '2021-02-01'" 2
skip app-txn-checkpoint 'count()' "modified < '2021-02-02'" 2
skip app-txn-checkpoint 'count()' "modified >= '2021-02-01'" 4
skip app-txn-checkpoint 'count()' "modified >= '2021-02-01' AND modified <= '2021-02-01'" 2
skip app-txn-checkpoint 'value' 'value > 9' 2
skip app-txn-checkpoint 'value' 'value < 4' 2
skip app-txn-checkpoint 'count()' 'value > 11' 0
skip app-txn-checkpoint 'count()' 'value <= 11' 4
skip app-txn-checkpoint 'value' 'value >= 1 AND value <= 3' 2
skip app-txn-checkpoint 'value' "modified = '2021-02-01' AND value > 9" 2
skip app-txn-checkpoint 'count()' "modified = '2021-02-02' OR value > 9" 4
bounded app-txn-checkpoint 'value' "modified >= '2021-02-01' AND (value < 2 OR value > 11)" 2 4
skip parsed-stats 'count(), min(id), max(id)' 'id > 400' 2
skip parsed-stats 'count(), min(id), max(id)' 'id BETWEEN 150 AND 250' 2

QUERIES+="SELECT '=== timestamp statistics truncated to milliseconds';"
# The file with ids 3 and 4 has the maximum 4.000500 s, written as 4.000 s in its JSON statistics.
# The kernel selects 2 files for > and >=, 2 for < and <=, and 1 for =.
for op in '>' '>=' '<' '<='; do
    bounded timestamp-truncation-stats 'id, ts_col' "ts_col ${op} toDateTime64('1970-01-01 00:00:04.000400', 6, 'UTC')" 2 3
done
bounded timestamp-truncation-stats 'id, ts_col' "ts_col = toDateTime64('1970-01-01 00:00:04.000400', 6, 'UTC')" 1 3
bounded timestamp-truncation-stats 'id, ts_col' "ts_col = toDateTime64('1970-01-01 00:00:04.000500', 6, 'UTC')" 1 3

QUERIES+="SELECT '=== null counts';"
# The files of partitions 0, 1 and 2 have only NULL, only non-NULL and mixed values of `n`.
bounded mixed-nulls 'part, n' 'n IS NULL' 2 3
bounded mixed-nulls 'part, n' 'n IS NOT NULL' 2 3
skip mixed-nulls 'part, n' "n = 'non-null'" 1

$CLICKHOUSE_LOCAL -q "${QUERIES}" | awk -F'\t' '
    $1 == "counter" { previous = $2; next }
    $1 == "scanned" { scanned = $2 - previous; previous = $2;
                      if (scanned == $3) print "scanned files: ok"; else print "scanned files: " scanned ", expected " $3; next }
    $1 == "bounded" { scanned = $2 - previous; previous = $2;
                      if (scanned >= $3 && scanned <= $4) print "scanned files: ok";
                      else print "scanned files: " scanned ", expected in [" $3 ", " $4 "]"; next }
    { print }'
