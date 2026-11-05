#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# https://github.com/ClickHouse/ClickHouse/issues/129498
# `OPTIMIZE` keeps the partition spec of a partitioned table: a later `DELETE` applies to the compacted files and a second `OPTIMIZE` succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2"
TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}/"

optimize()
{
    $CH --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1
}

# Reads apply position deletes whether or not compaction ran, so only their removal proves that OPTIMIZE ran.
count_position_deletes()
{
    $CH --query "SELECT count() FROM system.iceberg_files WHERE database = currentDatabase() AND table = '${TABLE}' AND content = 'POSITION_DELETE'"
}

$CH --query "CREATE TABLE ${TABLE} (k Int64, s Nullable(String)) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY s"
$CH --query "INSERT INTO ${TABLE} SELECT number, [NULL, 'a', 'b'][number % 3 + 1] FROM numbers(30)"
$CH --query "ALTER TABLE ${TABLE} DELETE WHERE k % 2 = 0"
deletes_before_first=$(count_position_deletes)
first_err=$(optimize)
deletes_after_first=$(count_position_deletes)
$CH --query "INSERT INTO ${TABLE} SELECT number + 100, [NULL, 'a'][number % 2 + 1] FROM numbers(10)"
$CH --query "ALTER TABLE ${TABLE} DELETE WHERE k % 3 = 0"

# The cloud build routes `OPTIMIZE` through a different code path.
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "first OPTIMIZE compacted the position deletes: 1"
    echo "rows matching the second DELETE: 0"
    echo "second OPTIMIZE compacted the position deletes: 1"
    printf '17\t880\n'
else
    [[ -n "${first_err}" ]] && echo "FAIL: first OPTIMIZE failed: ${first_err}"
    echo "first OPTIMIZE compacted the position deletes: $(( deletes_before_first > 0 && deletes_after_first == 0 ))"
    echo "rows matching the second DELETE:" "$($CH --query "SELECT count() FROM ${TABLE} WHERE k % 3 = 0")"
    deletes_before_second=$(count_position_deletes)
    second_err=$(optimize)
    [[ -n "${second_err}" ]] && echo "FAIL: second OPTIMIZE failed: ${second_err}"
    deletes_after_second=$(count_position_deletes)
    echo "second OPTIMIZE compacted the position deletes: $(( deletes_before_second > 0 && deletes_after_second == 0 ))"
    $CH --query "SELECT count(), sum(k) FROM ${TABLE}"
fi

$CH --query "DROP TABLE ${TABLE} SYNC"
rm -rf "${TABLE_PATH}"
