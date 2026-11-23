#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake"

# With the cache, the deletion vectors are read by ClickHouse, without it by the kernel. The first cached read
# lists the files, the second one takes them from the cache.
QUERIES="SET session_timezone = 'UTC';"
for table in \
    kernel_golden/dv-partitioned-with-checkpoint/delta \
    kernel_golden/dv-with-columnmapping/delta \
    kernel_golden/log-replay-dv-key-cases/delta \
    kernel_data/table-with-dv-small \
    kernel_data/cdf-table-with-dv \
    kernel_data/v2-checkpoints-parquet-with-sidecars \
    trino/databricks122/deletion_vectors
do
    DELTA="deltaLakeLocal('$DATA/$table')"
    QUERIES+="
SELECT '$table', count(), sum(cityHash64(*)) FROM $DELTA SETTINGS use_delta_lake_metadata_cache = 0;
SELECT '$table', count(), sum(cityHash64(*)) FROM $DELTA SETTINGS use_delta_lake_metadata_cache = 1;
SELECT '$table', count(), sum(cityHash64(*)) FROM $DELTA SETTINGS use_delta_lake_metadata_cache = 1;"
done

PRUNED="(SELECT sum(value) FROM system.events WHERE event = 'DeltaLakeStatsPrunedFiles')"

check()
{
    local table=$1 condition=$2
    local delta="deltaLakeLocal('$DATA/$table')"
    QUERIES+="
SELECT '$table', \$\$$condition\$\$, count() FROM $delta WHERE $condition SETTINGS use_delta_lake_metadata_cache = 0;
SELECT '$table', \$\$$condition\$\$, count(), $PRUNED FROM $delta WHERE $condition SETTINGS use_delta_lake_metadata_cache = 1;"
}

check kernel_data/cdf-table-non-partitioned "id = 3"
check kernel_data/cdf-table-non-partitioned "id > 8"
check kernel_data/cdf-table-non-partitioned "id IS NULL"
check kernel_data/cdf-table-non-partitioned "name = 'Ada'"
check kernel_data/cdf-table-non-partitioned "birthday = '2024-04-15'"
check kernel_data/cdf-table-non-partitioned "double_field < 0"
check kernel_data/cdf-table-non-partitioned "NOT (id >= 2)"
check delta_rs/delta-1.2.1-only-struct-stats "integer = 5"
check delta_rs/delta-1.2.1-only-struct-stats "integer IN (1, 7)"
check delta_rs/delta-1.2.1-only-struct-stats "string = 'string'"
check delta_rs/delta-1.2.1-only-struct-stats "timestamp >= '2022-10-24 22:59:44'"
check delta_rs/delta-1.2.1-only-struct-stats "timestamp = '2022-10-24 22:59:46.083211'"
check kernel_golden/dv-with-columnmapping/delta "col1 > 25"
check kernel_data/v2-checkpoints-parquet-with-sidecars "id < 10"

$CLICKHOUSE_LOCAL -q "$QUERIES"
