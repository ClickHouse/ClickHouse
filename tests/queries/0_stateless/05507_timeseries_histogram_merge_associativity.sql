-- `timeSeriesGroupArray` over histogram samples is associative, as `SimpleAggregateFunction` requires: the result doesn't depend on the order of
-- the merges. Every sample whose predecessor may have changed gets its counter reset hint detected from the data against its
-- predecessor in the result, whatever hint it had: an explicit or an earlier detected hint was about another predecessor. The hints
-- of the other samples are kept: the sink detected them, so they are what detection would give. The counter hints don't take part in
-- choosing between samples with the same timestamp, and a winner whose equal rivals had other counter hints gets an unknown hint, so
-- the first sample of the result doesn't depend on which of them was kept either.
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

-- Samples as (timestamp, count, hint); the hints are 0 - unknown, 1 - reset, 2 - no reset, 3 - gauge.
INSERT INTO histogram_arrays
SELECT name, arrayMap(s -> (s.1, 0, s.3, 0, 0., toFloat64(s.2), s.2, s.2, 0., 0., [], [], [], [], []), samples)
FROM values('name String, samples Array(Tuple(UInt32, Int64, UInt8))',
    ('a', [(100, 10, 0)]),
    ('c', [(200, 2, 0)]),
    ('b', [(300, 5, 0)]),
    ('pair', [(100, 10, 2), (300, 15, 1)]),
    ('middle', [(200, 12, 0)]),
    ('later', [(400, 20, 0)]),
    ('reported_reset', [(100, 1, 0), (200, 2, 1)]),
    ('no_reset', [(100, 1, 2)]),
    ('with_reset', [(100, 1, 1)]),
    ('gauge', [(100, 1, 3)]),
    ('before', [(50, 5, 0)]),
    ('two', [(50, 5, 0), (100, 1, 0)]));

SELECT '-- the hint of B is detected against C, whichever merge made C its predecessor';
WITH (SELECT h FROM histogram_arrays WHERE name = 'a') AS a,
     (SELECT h FROM histogram_arrays WHERE name = 'c') AS c,
     (SELECT h FROM histogram_arrays WHERE name = 'b') AS b,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints,
     (x, y) -> arrayReduce('timeSeriesGroupArray', [x, y]) AS merge
SELECT hints(arrayReduce('timeSeriesGroupArray', [a, c, b])) AS at_once,
       hints(merge(merge(a, b), c)) = at_once,
       hints(merge(merge(a, c), b)) = at_once,
       hints(merge(merge(c, b), a)) = at_once,
       hints(merge(c, merge(a, b))) = at_once,
       hints(merge(b, merge(a, c))) = at_once,
       hints(merge(a, merge(c, b))) = at_once;

SELECT '-- a reset the data does not show is kept while its predecessor stays (a single array, appended arrays) and detected again when a sample lands in between';
WITH (SELECT h FROM histogram_arrays WHERE name = 'pair') AS pair,
     (SELECT h FROM histogram_arrays WHERE name = 'middle') AS middle,
     (SELECT h FROM histogram_arrays WHERE name = 'later') AS later,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints
SELECT hints(arrayReduce('timeSeriesGroupArray', [pair])),
       hints(arrayReduce('timeSeriesGroupArray', [pair, later])),
       hints(arrayReduce('timeSeriesGroupArray', [pair, middle]));

SELECT '-- which of two equal samples wins does not change the hint of the sample after them';
WITH (SELECT h FROM histogram_arrays WHERE name = 'reported_reset') AS reported,
     (SELECT h FROM histogram_arrays WHERE name = 'no_reset') AS equal,
     (SELECT h FROM histogram_arrays WHERE name = 'before') AS earlier,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints,
     (x, y) -> arrayReduce('timeSeriesGroupArray', [x, y]) AS merge
SELECT hints(arrayReduce('timeSeriesGroupArray', [reported, equal, earlier])) AS at_once,
       hints(merge(merge(reported, earlier), equal)) = at_once,
       hints(merge(merge(reported, equal), earlier)) = at_once,
       hints(merge(merge(equal, earlier), reported)) = at_once,
       hints(merge(reported, merge(equal, earlier))) = at_once;

SELECT '-- an equal duplicate of the predecessor does not change the hint of the sample after it, whichever is kept';
WITH (SELECT h FROM histogram_arrays WHERE name = 'two') AS two,
     (SELECT h FROM histogram_arrays WHERE name = 'before') AS before,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints
SELECT hints(arrayReduce('timeSeriesGroupArray', [two, before])),
       hints(arrayReduce('timeSeriesGroupArray', [before, two]));

SELECT '-- the first sample keeps its hint if its equal duplicates agree, gets an unknown one if they differ, and a gauge marker wins';
WITH (SELECT h FROM histogram_arrays WHERE name = 'no_reset') AS no_reset,
     (SELECT h FROM histogram_arrays WHERE name = 'with_reset') AS with_reset,
     (SELECT h FROM histogram_arrays WHERE name = 'gauge') AS gauge,
     x -> arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), x) AS hints,
     (x, y) -> arrayReduce('timeSeriesGroupArray', [x, y]) AS merge
SELECT hints(merge(no_reset, no_reset)),
       hints(merge(no_reset, with_reset)),
       hints(merge(with_reset, no_reset)),
       hints(merge(merge(no_reset, no_reset), with_reset)),
       hints(merge(no_reset, merge(no_reset, with_reset))),
       hints(merge(no_reset, gauge)),
       hints(merge(gauge, no_reset));

DROP TABLE histogram_arrays;
