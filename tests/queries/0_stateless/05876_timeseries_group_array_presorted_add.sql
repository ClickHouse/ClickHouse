SET allow_experimental_time_series_aggregate_functions = 1;

SELECT 'empty';
SELECT timeSeriesGroupArray(samples)
FROM (SELECT []::Array(Tuple(UInt32, Float64)) AS samples FROM numbers(3));

SELECT timeSeriesGroupArray(samples)
FROM
(
    SELECT [[], [(10::UInt32, 1.::Float64), (20::UInt32, 2.::Float64)], [], [(30::UInt32, 3.::Float64)], []][number + 1] AS samples
    FROM numbers(5)
);

SELECT 'disjoint append and prepend across arena threshold';
DROP TABLE IF EXISTS ts_presorted_parts;
CREATE TABLE ts_presorted_parts
(
    id UInt64,
    b DateTime64(3, 'UTC'),
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY (id, b);

SYSTEM STOP MERGES ts_presorted_parts;

INSERT INTO ts_presorted_parts
SELECT
    1,
    toDateTime64(1700000000, 3, 'UTC'),
    arrayMap(k -> (toDateTime64(1700000000 + k * 15, 3, 'UTC'), toFloat64(k)), range(30));

INSERT INTO ts_presorted_parts
SELECT
    2,
    toDateTime64(1700000000, 3, 'UTC'),
    arrayMap(k -> (toDateTime64(1700000000 + (11 * 30 + k) * 15, 3, 'UTC'), toFloat64(11 * 100 + k)), range(30));

INSERT INTO ts_presorted_parts VALUES (3, '2023-11-14 22:13:20.000', []);

INSERT INTO ts_presorted_parts
SELECT
    id,
    toDateTime64(1700000000, 3, 'UTC'),
    samples
FROM
(
    SELECT
        1 AS id,
        p,
        arrayMap(k -> (toDateTime64(1700000000 + (p * 30 + k) * 15, 3, 'UTC'), toFloat64(p * 100 + k)), range(30)) AS samples
    FROM (SELECT number + 1 AS p FROM numbers(11))
    UNION ALL
    SELECT
        2 AS id,
        p,
        arrayMap(k -> (toDateTime64(1700000000 + ((11 - p) * 30 + k) * 15, 3, 'UTC'), toFloat64((11 - p) * 100 + k)), range(30)) AS samples
    FROM (SELECT number + 1 AS p FROM numbers(11))
)
ORDER BY p
SETTINGS max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, optimize_on_insert = 0;

SELECT
    id,
    length(samples),
    samples = arrayMap(k -> (toDateTime64(1700000000 + k * 15, 3, 'UTC'), toFloat64(if(id = 1 AND k < 30, k, intDiv(k, 30) * 100 + k % 30))), range(360))
FROM ts_presorted_parts FINAL
WHERE id IN (1, 2)
ORDER BY id;

SELECT id, samples FROM ts_presorted_parts FINAL WHERE id = 3;

SYSTEM START MERGES ts_presorted_parts;
OPTIMIZE TABLE ts_presorted_parts FINAL;

SELECT
    id,
    length(samples),
    samples = arrayMap(k -> (toDateTime64(1700000000 + k * 15, 3, 'UTC'), toFloat64(if(id = 1 AND k < 30, k, intDiv(k, 30) * 100 + k % 30))), range(360))
FROM ts_presorted_parts
WHERE id IN (1, 2)
ORDER BY id;

DROP TABLE ts_presorted_parts;

SELECT 'many reverse singletons';
SELECT length(s), s = arrayMap(k -> (toUInt32(1000 + k), toFloat64(k)), range(400))
FROM
(
    SELECT timeSeriesGroupArray([(toUInt32(1000 + 399 - number), toFloat64(399 - number))]) AS s
    FROM numbers(400)
);

SELECT 'overlap duplicate and nan';
DROP TABLE IF EXISTS ts_overlap;
CREATE TABLE ts_overlap
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(UInt32, Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY id;

SYSTEM STOP MERGES ts_overlap;

INSERT INTO ts_overlap VALUES
    (1, [(10, 1.), (20, 5.)]),
    (2, [(10, 1.), (20, 5.), (30, 3.)]),
    (3, [(20, 5.), (30, 3.), (40, 6.)]),
    (4, [(10, 1.), (30, 3.), (50, 5.)]),
    (5, [(10, nan), (20, -5.), (30, nan), (40, -10.)]),
    (6, [(10, reinterpretAsFloat64(toUInt64(9218868437227405314))), (20, reinterpretAsFloat64(toUInt64(9218868437227405314)))]);

INSERT INTO ts_overlap VALUES
    (1, [(20, 3.), (30, 6.)]),
    (2, [(20, 2.), (30, 8.), (40, 6.)]),
    (3, [(10, 9.), (20, 2.), (30, 8.)]),
    (4, [(20, 2.), (30, 30.), (40, 4.)]),
    (5, [(15, 2.), (20, nan), (30, nan), (40, nan), (50, nan)]),
    (6, [(20, -1.), (30, 4.)]);

INSERT INTO ts_overlap VALUES
    (1, [(30, 2.), (30, 9.), (40, 7.)]),
    (4, [(5, 0.5), (10, 0.), (30, 10.), (50, 50.), (60, 6.)]),
    (5, [(10, -100.), (30, nan), (50, -0.5)]);

SELECT id, samples FROM ts_overlap FINAL WHERE id <= 5 ORDER BY id;
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), samples) FROM ts_overlap FINAL WHERE id = 6;

SYSTEM START MERGES ts_overlap;
OPTIMIZE TABLE ts_overlap FINAL;

SELECT id, samples FROM ts_overlap WHERE id <= 5 ORDER BY id;
SELECT arrayMap(x -> (x.1, hex(reinterpretAsUInt64(x.2))), samples) FROM ts_overlap WHERE id = 6;

DROP TABLE ts_overlap;

SELECT 'unsorted positions inside single array';
SELECT count(), countIf(a != b)
FROM
(
    SELECT
        pos,
        timeSeriesGroupArray(arr) AS a,
        arrayReduce('timeSeriesGroupArray', arrayMap(x -> x.1, arr), arrayMap(x -> x.2, arr)) AS b
    FROM
    (
        SELECT
            pos,
            arrayMap(k -> (toUInt32(if(k = pos, 5, 10 + k * 2)), toFloat64(k + 1)), range(20)) AS arr
        FROM (SELECT arrayJoin([0, 1, 7, 8, 9, 15, 16, 19]) AS pos)
    )
    GROUP BY pos, arr
);

SELECT 'two-array and group by parity';
SELECT
    timeSeriesGroupArray([10, 20]::Array(UInt32), [1., 2.]::Array(Float64)),
    timeSeriesGroupArrayIf(ts, vals, cond)
FROM
(
    SELECT
        [[30, 40], [99, 100], [10, 20]]::Array(Array(UInt32))[number + 1] AS ts,
        [[3., 4.], [9., 9.], [1., 2.]]::Array(Array(Float64))[number + 1] AS vals,
        [1, 0, 1][number + 1] AS cond
    FROM numbers(3)
);

DROP TABLE IF EXISTS ts_mixed_parts;
CREATE TABLE ts_mixed_parts
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(UInt32, Float64)))
)
ENGINE = AggregatingMergeTree ORDER BY id;

SYSTEM STOP MERGES ts_mixed_parts;

INSERT INTO ts_mixed_parts
SELECT
    id,
    if(p % 5 = 4,
        []::Array(Tuple(UInt32, Float64)),
        arrayMap(
            k -> (
                toUInt32(1000 + (if(id % 3 = 0, p, if(id % 3 = 1, 7 - p, (p * 3) % 8)) * 10 + k) * 5),
                if((id + p + k) % 17 = 0, nan, toFloat64((sipHash64(id, p, k) % 10000) - 5000))
            ),
            range(16)
        )
    )
FROM (SELECT number % 200 AS id, intDiv(number, 200) AS p FROM numbers(200 * 8))
ORDER BY p, id
SETTINGS max_block_size = 200, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, optimize_on_insert = 0;

CREATE TEMPORARY TABLE ts_mixed_raw AS
SELECT id, arrayJoin(samples) AS s FROM ts_mixed_parts;

SELECT
    count(),
    countIf(
        arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), f.samples)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), r.ref_samples)
    )
FROM (SELECT id, samples FROM ts_mixed_parts FINAL) AS f
INNER JOIN (SELECT id, timeSeriesGroupArray(s.1, s.2) AS ref_samples FROM ts_mixed_raw GROUP BY id) AS r
USING (id);

SYSTEM START MERGES ts_mixed_parts;
OPTIMIZE TABLE ts_mixed_parts FINAL;

SELECT
    count(),
    countIf(
        arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), m.samples)
        != arrayMap(x -> (x.1, reinterpretAsUInt64(x.2)), r.ref_samples)
    )
FROM ts_mixed_parts AS m
INNER JOIN (SELECT id, timeSeriesGroupArray(s.1, s.2) AS ref_samples FROM ts_mixed_raw GROUP BY id) AS r
USING (id);

DROP TABLE ts_mixed_parts;
