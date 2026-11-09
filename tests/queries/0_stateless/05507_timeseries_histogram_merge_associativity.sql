-- `timeSeriesGroupArrayHistograms` is associative, as `SimpleAggregateFunction` requires: the counter reset hint of a sample depends on
-- its final predecessor only, not on the order of the merges. Where a merge changes the predecessor, the hint is detected from the data
-- whatever hint the sample had, as an explicit or an earlier detected reset was about another predecessor. A (count 10), C (count 2)
-- and B (count 5) arrive with unknown hints: merging A with B first detects a reset for B, and merging C in afterwards must find that
-- B doesn't reset after C, as merging all three at once does.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS histogram_arrays;
CREATE TABLE histogram_arrays
(
    name String,
    h Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64)))
) ENGINE = Memory;

INSERT INTO histogram_arrays
SELECT name, arrayMap(s -> (s.1, 0, s.3, 0, 0., toFloat64(s.2), s.2, s.2, 0., 0., [], [], [], [], []), samples)
FROM values('name String, samples Array(Tuple(UInt32, Int64, UInt8))',
    ('a', [(100, 10, 0)]),
    ('c', [(200, 2, 0)]),
    ('b', [(300, 5, 0)]),
    ('pair', [(100, 10, 2), (300, 15, 1)]),
    ('middle', [(200, 12, 0)]));

SELECT '-- the hint of B is detected against C, whichever merge made C its predecessor';
WITH (SELECT h FROM histogram_arrays WHERE name = 'a') AS a,
     (SELECT h FROM histogram_arrays WHERE name = 'c') AS c,
     (SELECT h FROM histogram_arrays WHERE name = 'b') AS b,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints,
     (x, y) -> arrayReduce('timeSeriesGroupArrayHistograms', [x, y]) AS merge
SELECT hints(arrayReduce('timeSeriesGroupArrayHistograms', [a, c, b])) AS at_once,
       hints(merge(merge(a, b), c)) = at_once,
       hints(merge(merge(a, c), b)) = at_once,
       hints(merge(merge(c, b), a)) = at_once,
       hints(merge(c, merge(a, b))) = at_once,
       hints(merge(b, merge(a, c))) = at_once,
       hints(merge(a, merge(c, b))) = at_once;

SELECT '-- an explicit reset is kept while the predecessor stays, and detected again when a sample lands in between';
WITH (SELECT h FROM histogram_arrays WHERE name = 'pair') AS pair,
     (SELECT h FROM histogram_arrays WHERE name = 'middle') AS middle,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints
SELECT hints(arrayReduce('timeSeriesGroupArrayHistograms', [pair])),
       hints(arrayReduce('timeSeriesGroupArrayHistograms', [pair, middle])),
       hints(arrayReduce('timeSeriesGroupArrayHistograms', [arraySlice(pair, 1, 1), middle, arraySlice(pair, 2, 1)]));

DROP TABLE histogram_arrays;
