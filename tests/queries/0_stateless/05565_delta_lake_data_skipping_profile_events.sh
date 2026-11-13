#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# File pruning of Delta Lake tables written by Spark, observed through `DeltaLakeScannedFiles` (files returned by the
# scan of delta-kernel-rs, which skips files by statistics and partition values) and `DeltaLakePartitionPrunedFiles`
# (files skipped by ClickHouse by partition values): by the statistics of the files (also when they come from a v2 checkpoint with
# sidecars, or from the checkpoint of a table with deletion vectors), by partition values (including NULL and
# special characters), and by a widened column, whose old files have statistics of the narrow type.
# The tables are copies of corpus tables (see `05560_delta_lake_corpus`) in the user files of the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_USER_FILES_UNIQUE}_skipping"
rm -rf "$BASE"
mkdir -p "$BASE"
for table in v2_checkpoint_parquet deletion_vectors partitioned type_widening liquid_clustering
do
    cp -r "$CUR_DIR/data_lake_corpus/delta/$table" "$BASE/$table"
done

n=0
check()
{
    local table=$1 condition=$2
    n=$((n + 1))
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${n}"
    local result
    # `optimize_empty_string_comparisons` is randomized in CI, so it is pinned to the default: it rewrites `p_str = ''`
    # into `empty(p_str)`, which is not converted into a predicate of delta-kernel-rs, so the files are pruned by
    # ClickHouse; without the rewrite, delta-kernel-rs prunes them itself. `delta_lake_enable_engine_predicate` is
    # randomized too, and without it delta-kernel-rs skips no files.
    result=$(${CLICKHOUSE_CLIENT} --session_timezone UTC --query_id "$query_id" --use_query_condition_cache 0 \
        --optimize_empty_string_comparisons 1 --delta_lake_enable_engine_predicate 1 \
        --query "SELECT count(), sum(id) FROM deltaLakeLocal('$BASE/$table') WHERE $condition" | tr '\t' ' ')
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --param_condition "$condition" --param_result "$result" --query "
        SELECT '$table', {condition:String}, {result:String}, ProfileEvents['DeltaLakeScannedFiles'], ProfileEvents['DeltaLakePartitionPrunedFiles']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish'"
}

echo '-- statistics from a v2 checkpoint (5 files of 10 ids each, one deleted row)'
check v2_checkpoint_parquet "1 = 1"
check v2_checkpoint_parquet "id >= 40"
check v2_checkpoint_parquet "id < 10"
check v2_checkpoint_parquet "id = 25"
check v2_checkpoint_parquet "id > 1000"

echo '-- deletion vectors'
check deletion_vectors "1 = 1"
check deletion_vectors "id >= 100"
check deletion_vectors "id < 100"

echo '-- partition values'
check partitioned "1 = 1"
check partitioned "p_int = 1"
check partitioned "p_int IS NULL"
check partitioned "p_str = 'x/y=z'"
check partitioned "p_str = ''"
check partitioned "p_bool"
check partitioned "p_date = '2000-01-02'"

echo '-- widened column'
check type_widening "i > 5000000000"
check type_widening "sh = -32768"

echo '-- clustered'
check liquid_clustering "1 = 1"
check liquid_clustering "id >= 30"

rm -rf "$BASE"
