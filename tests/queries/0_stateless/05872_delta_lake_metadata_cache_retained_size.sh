#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table partitioned by `p` whose every data file has a partition value of 100000 bytes. The kernel keeps
# the value in the transform of each file, so a cached listing retains more than its paths.
WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
cp -r "$CUR_DIR/data_delta_lake/kernel_golden/basic-with-inserts-updates/delta" "$WORK_DIR/"
LOG="$WORK_DIR/delta/_delta_log"
VALUE=$(printf 'v%.0s' {1..100000})
sed -i 's/\\"fields\\":\[/\\"fields\\":[{\\"name\\":\\"p\\",\\"type\\":\\"string\\",\\"nullable\\":true,\\"metadata\\":{}},/; s/"partitionColumns":\[\]/"partitionColumns":["p"]/' "$LOG/00000000000000000000.json"
sed -i "s/\"partitionValues\":{}/\"partitionValues\":{\"p\":\"$VALUE\"}/" "$LOG"/*.json
DELTA="deltaLakeLocal('$WORK_DIR/delta')"

run()
{
    local budget=$1
    $CLICKHOUSE_LOCAL -q "
    SET use_delta_lake_metadata_cache = 1;
    SELECT count(), sum(id), uniqExact(p), min(length(p)) FROM $DELTA;
    SELECT count(), sum(id), uniqExact(p), min(length(p)) FROM $DELTA;
    WITH
        (SELECT sum(value) FROM system.events WHERE event = 'DeltaLakeMetadataCacheHits') AS hits,
        (SELECT sum(value) FROM system.metrics WHERE metric = 'DeltaLakeMetadataCacheBytes') AS bytes
    SELECT hits > 0 ? 'cached' : 'not cached', bytes >= 200000, bytes <= $budget;
    " -- --delta_lake_metadata_cache_size="$budget"
}

echo "small cache"
run 150000
echo "large cache"
run 10000000

rm -rf "$WORK_DIR"
