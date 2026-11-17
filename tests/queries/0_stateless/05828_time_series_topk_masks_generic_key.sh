#!/usr/bin/env bash
# Tags: no-fasttest
# Tests the `key` argument of the timeSeries{TopK,BottomK,LimitK}Masks aggregate functions: UInt64 keys (group numbers) keep working,
# and keys of other types (e.g. an array of tags) are supported with the same selection, deterministic tie-breaking and mergeable states.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_aggregate_functions=1"

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE IF EXISTS topk_generic_input;
DROP TABLE IF EXISTS topk_generic_states;

-- 200 series, 6 time steps, many equal values (ties) and some missing values. 'instance' is zero-padded, so the order of the serialized tags is the order of n.
CREATE TABLE topk_generic_input (n UInt64, tags Array(Tuple(String, String)), v Array(Nullable(Float64))) ENGINE = MergeTree ORDER BY n;
INSERT INTO topk_generic_input SELECT n, [('instance', lpad(toString(n), 3, '0')), ('job', concat('j', toString(n % 5)))],
    arrayMap(t -> if((n + t) % 11 = 0, NULL, toFloat64((n * 7 + t * 13) % 50)), range(6))
FROM (SELECT number AS n FROM numbers(0, 100));
INSERT INTO topk_generic_input SELECT n, [('instance', lpad(toString(n), 3, '0')), ('job', concat('j', toString(n % 5)))],
    arrayMap(t -> if((n + t) % 11 = 0, NULL, toFloat64((n * 7 + t * 13) % 50)), range(6))
FROM (SELECT number AS n FROM numbers(100, 100));

SELECT '-- UInt64 keys';
SELECT timeSeriesTopKMasks(2, n, v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesBottomKMasks(2, n, v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesLimitKMasks(2, n, cityHash64(n), v) FROM topk_generic_input WHERE n < 6;
SELECT toTypeName(timeSeriesTopKMasks(2, n, v)) FROM topk_generic_input;

SELECT '-- String and tags keys';
SELECT timeSeriesTopKMasks(2, toString(n), v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesTopKMasks(2, tags, v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesBottomKMasks(2, tags, v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesLimitKMasks(2, tags, cityHash64(tags), v) FROM topk_generic_input WHERE n < 6;
SELECT toTypeName(timeSeriesTopKMasks(2, tags, v)) FROM topk_generic_input;

SELECT '-- Other key types';
SELECT timeSeriesTopKMasks(1, toDate(n), v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesTopKMasks(1, toLowCardinality(toString(n)), v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesTopKMasks(1, toNullable(toString(n)), v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesTopKMasks(1, (n, toString(n)), v) FROM topk_generic_input WHERE n < 6;
SELECT timeSeriesTopKMasks(1, n, v) FROM topk_generic_input WHERE n < 0;
SELECT timeSeriesTopKMasks(1, tags, v) FROM topk_generic_input WHERE n < 0;

SELECT '-- Tags keys select the same series as UInt64 keys';
SELECT
    arrayMap(x -> (toUInt64(x.1[1].2), x.2), timeSeriesTopKMasks(3, tags, v)) = timeSeriesTopKMasks(3, n, v),
    arrayMap(x -> (toUInt64(x.1[1].2), x.2), timeSeriesBottomKMasks(3, tags, v)) = timeSeriesBottomKMasks(3, n, v),
    arrayMap(x -> (toUInt64(x.1[1].2), x.2), timeSeriesLimitKMasks(3, tags, cityHash64(n), v)) = timeSeriesLimitKMasks(3, n, cityHash64(n), v),
    length(timeSeriesTopKMasks(3, n, v)) > 3
FROM topk_generic_input;
-- Grouped by a part of the tags.
SELECT job, arraySum(arrayMap(x -> arraySum(x.2), r1)), arraySum(arrayMap(x -> arraySum(x.2), r2)), arrayMap(x -> x.1[2].2, r1) = arrayMap(x -> concat('j', toString(x.1 % 5)), r2)
FROM (SELECT tags[2].2 AS job, timeSeriesTopKMasks(2, tags, v) AS r1, timeSeriesTopKMasks(2, n, v) AS r2 FROM topk_generic_input GROUP BY job)
ORDER BY job;

SELECT '-- States of tags keys are mergeable';
CREATE TABLE topk_generic_states (part UInt8, st AggregateFunction(timeSeriesTopKMasks, UInt8, Array(Tuple(String, String)), Array(Nullable(Float64)))) ENGINE = MergeTree ORDER BY part;
INSERT INTO topk_generic_states SELECT n % 7, timeSeriesTopKMasksState(3, tags, v) FROM topk_generic_input GROUP BY n % 7;
SELECT timeSeriesTopKMasksMerge(st) = (SELECT timeSeriesTopKMasks(3, tags, v) FROM topk_generic_input) FROM topk_generic_states;
SELECT finalizeAggregation(timeSeriesTopKMasksState(3, tags, v)) = timeSeriesTopKMasks(3, tags, v) FROM topk_generic_input;

SELECT '-- Wrong argument types';
SELECT timeSeriesTopKMasks(1, n, v, v) FROM topk_generic_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT timeSeriesTopKMasks(1, toUInt32(n), v) FROM topk_generic_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesTopKMasks(1, tags, 1) FROM topk_generic_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesLimitKMasks(1, tags, tags, v) FROM topk_generic_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
"

# All series have equal values, so only the tie-breaking decides what is selected: it must not depend on the number of threads or on the run.
echo '-- Ties are broken deterministically, the number of distinct results over 10 runs for each max_threads'
for max_threads in 1 8
do
    for _ in $(seq 1 10)
    do
        $CLICKHOUSE_CLIENT --max_threads=$max_threads -q "
            SELECT cityHash64(timeSeriesTopKMasks(3, tags, [1.0, 1.0, 1.0]::Array(Float64))),
                   cityHash64(timeSeriesBottomKMasks(3, tags, [1.0, 1.0, 1.0]::Array(Float64))),
                   cityHash64(timeSeriesLimitKMasks(3, tags, toUInt64(cityHash64(tags) % 3), [1.0, 1.0, 1.0]::Array(Float64)))
            FROM topk_generic_input"
    done | sort | uniq | wc -l | tr -d " "
done
$CLICKHOUSE_CLIENT -q "SELECT timeSeriesTopKMasks(2, tags, [1.0, 1.0]::Array(Float64)) FROM topk_generic_input"

$CLICKHOUSE_CLIENT -n -q "
DROP TABLE topk_generic_input;
DROP TABLE topk_generic_states;
"
