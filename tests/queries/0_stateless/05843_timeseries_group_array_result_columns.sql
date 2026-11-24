-- `timeSeriesGroupArray` writes its result straight into the timestamp and value columns of the output block.

-- Many groups are finished into one output block: the samples of each group follow the samples of the groups before it.
-- The reference is the same samples gathered with `groupArray` and sorted by timestamp.
SELECT 'array of pairs, DateTime64 and Float64';
SELECT count(), countIf(s != r), sum(length(s))
FROM
(
    SELECT id, timeSeriesGroupArray(samples) AS s, arraySort(x -> x.1, groupArrayArray(samples)) AS r
    FROM
    (
        SELECT number % 1000 AS id, [(toDateTime64(1700000000 + intDiv(number, 1000) * 15, 3, 'UTC'), toFloat64(number) / 8)] AS samples
        FROM numbers(30000)
    )
    GROUP BY id
);

-- Out-of-order samples: the rows of each group arrive in descending timestamp order, so each state is sorted before its result is written.
SELECT 'two arguments, DateTime and Float32, out of order';
SELECT count(), countIf(s != r), sum(length(s))
FROM
(
    SELECT id, timeSeriesGroupArray(t, v) AS s, arraySort(x -> x.1, groupArray((t, v))) AS r
    FROM
    (
        SELECT number % 700 AS id, toDateTime(1700000000 + (30000 - number) * 15, 'UTC') AS t, toFloat32(number / 8) AS v
        FROM numbers(30000)
    )
    GROUP BY id
);

-- The bits of the values are kept: the stale marker, NaN and -0 as `Float64`, and a signaling NaN and -0 as `Float32`.
SELECT 'bit patterns';
SELECT arrayMap(x -> hex(reinterpretAsUInt64(x.2)), timeSeriesGroupArray(samples))
FROM
(
    SELECT [(toDateTime64(1700000000 + number, 3, 'UTC'), [reinterpretAsFloat64(toUInt64(9218868437227405314)), nan, -0.][number + 1])] AS samples
    FROM numbers(3)
);
SELECT arrayMap(x -> hex(reinterpretAsUInt32(x.2)), timeSeriesGroupArray(t, v))
FROM
(
    SELECT toDateTime(1700000000 + number, 'UTC') AS t, [reinterpretAsFloat32(toUInt32(2139095041)), toFloat32(-0.)][number + 1] AS v
    FROM numbers(2)
);

-- The merge of an `AggregatingMergeTree` table, as in the samples table of a `TimeSeries` table: three parts, one sample
-- per row and part, merged into rows of three samples each.
DROP TABLE IF EXISTS ts_group_array_samples;
CREATE TABLE ts_group_array_samples
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY id;

SYSTEM STOP MERGES ts_group_array_samples;
INSERT INTO ts_group_array_samples SELECT number, [(toDateTime64(1700000030, 3, 'UTC'), toFloat64(number * 10 + 2))] FROM numbers(500);
INSERT INTO ts_group_array_samples SELECT number, [(toDateTime64(1700000000, 3, 'UTC'), toFloat64(number * 10))] FROM numbers(500);
INSERT INTO ts_group_array_samples SELECT number, [(toDateTime64(1700000015, 3, 'UTC'), if(number = 7, reinterpretAsFloat64(toUInt64(9218868437227405314)), toFloat64(number * 10 + 1)))] FROM numbers(500);
SYSTEM START MERGES ts_group_array_samples;
OPTIMIZE TABLE ts_group_array_samples FINAL;

SELECT 'after the merge';
SELECT count(), countIf(id != 7 AND samples != arrayMap(k -> (toDateTime64(1700000000 + k * 15, 3, 'UTC'), toFloat64(id * 10 + k)), range(3)))
FROM ts_group_array_samples;
SELECT arrayMap(x -> (toUnixTimestamp64Milli(x.1), hex(reinterpretAsUInt64(x.2))), samples) FROM ts_group_array_samples WHERE id = 7;

DROP TABLE ts_group_array_samples;
