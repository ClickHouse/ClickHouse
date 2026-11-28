#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="$CUR_DIR/data_delta_lake/kernel_golden/dv-partitioned-with-checkpoint"
DELTA="deltaLakeLocal('$TABLE/delta')"
EXPECTED="file('$TABLE/expected/*.parquet', Parquet)"

$CLICKHOUSE_LOCAL -q "
CREATE TABLE counters (step UInt32, name String, hits UInt64, misses UInt64) ENGINE = Memory;
CREATE VIEW current_counters AS
    SELECT sumIf(value, event = 'DeltaLakeMetadataCacheHits') AS hits, sumIf(value, event = 'DeltaLakeMetadataCacheMisses') AS misses
    FROM system.events;

INSERT INTO counters SELECT 0, 'start', * FROM current_counters;

SELECT 'without cache', count(), sum(col1) FROM $DELTA;
INSERT INTO counters SELECT 1, 'without cache', * FROM current_counters;

SET use_delta_lake_metadata_cache = 1;

SELECT 'first', count(), sum(col1) FROM $DELTA;
INSERT INTO counters SELECT 2, 'first', * FROM current_counters;

SELECT 'second', count(), sum(col1) FROM $DELTA;
INSERT INTO counters SELECT 3, 'second', * FROM current_counters;

SELECT 'missing rows', count() FROM (SELECT * FROM $EXPECTED EXCEPT ALL SELECT * FROM $DELTA);
SELECT 'unexpected rows', count() FROM (SELECT * FROM $DELTA EXCEPT ALL SELECT * FROM $EXPECTED);
SELECT 'partition', count(), sum(col1) FROM $DELTA WHERE part = 3;
SELECT 'expected partition', count(), sum(col1) FROM $EXPECTED WHERE part = 3;
INSERT INTO counters SELECT 4, 'cached reads', * FROM current_counters;

SYSTEM CLEAR DELTA LAKE METADATA CACHE;

SELECT 'after clear', count(), sum(col1) FROM $DELTA;
INSERT INTO counters SELECT 5, 'after clear', * FROM current_counters;

SELECT name, hits > lagInFrame(hits) OVER w AS hit, misses > lagInFrame(misses) OVER w AS miss
FROM counters
WINDOW w AS (ORDER BY step ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY step
OFFSET 1;
"
