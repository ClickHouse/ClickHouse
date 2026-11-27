-- The order of `TimeSeriesDuplicateTimestampRule` over histogram samples with the same timestamp: a stale marker loses, then the
-- greatest count wins, then the greatest sum, then the other elements decide in the stored order. Of each pair `timeSeriesGroupArray`
-- keeps `a`, whichever array comes first and when one array holds both. A pair is named after the key deciding it, and a later key
-- that differs favours `b`, so a key compared too late or not at all shows up as `b` or as a winner depending on the input order.
-- The sink applies the same rule to the duplicates of one INSERT (see 05458_timeseries_histograms_insert).

DROP TABLE IF EXISTS duplicate_pairs;

CREATE TABLE duplicate_pairs
(
    decider String,
    a Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                  schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64))),
    b Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                  schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64)))
) ENGINE = Memory;

INSERT INTO duplicate_pairs VALUES
    ('count',
        [(100, false, 'unknown', 0, 0., 1., 5, 0, 0., 0., [(0, 5)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0., 5., 4, 0, 0., 0., [(0, 4)], [], [], [], [])]),
    -- The counts are compared exactly: rounded to `Float64`, the integer count 2^53 + 1 would be equal to the float count 2^53.
    ('count_across_flavours',
        [(100, false, 'unknown', 0, 0., 1., 9007199254740993, 9007199254740993, 0., 0., [], [], [], [], [])],
        [(100, true, 'unknown', 0, 0., 2., 0, 0, 9007199254740992., 9007199254740992., [], [], [], [], [])]),
    ('count_nan',
        [(100, false, 'unknown', 0, 0., 7., 1, 0, 0., 0., [(0, 1)], [], [], [], [])],
        [(100, true, 'unknown', 0, 0., 8., 0, 0, nan, 0., [], [], [], [], [])]),
    ('sum',
        [(100, false, 'unknown', 0, 0., 6., 7, 0, 0., 0., [(0, 7)], [], [], [], [])],
        [(100, false, 'unknown', 1, 0., 3., 7, 0, 0., 0., [(0, 7)], [], [], [], [])]),
    ('is_float',
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 5., 0., [], [], [(0, 5.)], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 5, 0, 0., 0., [(0, 5)], [], [], [], [])]),
    -- Of the counter reset hints only the gauge marker is compared: merging changes the other ones.
    ('gauge_marker',
        [(100, false, 'gauge', 0, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])],
        [(100, false, 'unknown', 1, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])]),
    ('schema',
        [(100, false, 'unknown', 1, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0.5, 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])]),
    ('zero_threshold',
        [(100, false, 'unknown', 0, 0.5, 1., 2, 0, 0., 0., [(0, 2)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0.25, 1., 2, 1, 0., 0., [(0, 1)], [], [], [], [])]),
    ('zero_count_int',
        [(100, false, 'unknown', 0, 0., 1., 3, 2, 0., 0., [(0, 1)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 3, 1, 0., 0., [(0, 2)], [], [], [], [])]),
    ('zero_count_float',
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 3., 2., [], [], [(0, 1.)], [], [])],
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 3., 1., [], [], [(0, 2.)], [], [])]),
    ('positive_int_index',
        [(100, false, 'unknown', 0, 0., 1., 2, 0, 0., 0., [(1, 1)], [(0, 1)], [], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 2, 0, 0., 0., [(0, 2)], [], [], [], [])]),
    ('positive_int_count',
        [(100, false, 'unknown', 0, 0., 1., 2, 0, 0., 0., [(0, 2)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 2, 0, 0., 0., [(0, 1)], [(0, 1)], [], [], [])]),
    ('positive_int_size',
        [(100, false, 'unknown', 0, 0., 1., 1, 0, 0., 0., [(0, 1), (1, 0)], [], [], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 1, 0, 0., 0., [(0, 1)], [(0, 0)], [], [], [])]),
    ('negative_int',
        [(100, false, 'unknown', 0, 0., 1., 1, 0, 0., 0., [], [(1, 1)], [], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 1, 0, 0., 0., [], [(0, 1)], [], [], [])]),
    ('positive_float',
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 2., 0., [], [], [(0, 2.)], [], [])],
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 2., 0., [], [], [(0, 1.)], [(0, 1.)], [])]),
    ('negative_float',
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 1., 0., [], [], [], [(1, 1.)], [])],
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 1., 0., [], [], [], [(0, 1.)], [])]),
    ('custom_values',
        [(100, false, 'unknown', -53, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [1., 3.])],
        [(100, false, 'unknown', -53, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [1., 2.])]);

-- `b` is a stale marker, a histogram whose `sum` is the NaN with the bits 0x7FF0000000000002. Its other elements are ignored, so its
-- greater count must not let it win, and it loses to an ordinary NaN `sum` (0x7FF8000000000001, Go's `math.NaN()`) too.
-- 0x3FF0000000000000 is 1.
INSERT INTO duplicate_pairs
SELECT decider,
       [(100, 0, 0, 0, 0., reinterpret(a_sum, 'Float64'), a_count, 0, 0., 0.,
         if(a_count = 0, [], [(0, a_count)]), [], [], [], [])],
       [(100, 0, 0, 0, 0., reinterpret(0x7FF0000000000002, 'Float64'), stale_count, 0, 0., 0.,
         if(stale_count = 0, [], [(0, stale_count)]), [], [], [], [])]
FROM values('decider String, stale_count Int64, a_sum UInt64, a_count Int64',
    ('stale_marker', 5, 0x3FF0000000000000, 3),
    ('stale_marker_vs_nan', 0, 0x7FF8000000000001, 0));

-- The winner is found by a hash of its bits: the NaN `sum` of the winner of `stale_marker_vs_nan` isn't equal to itself.
SELECT decider,
       multiIf(a_first = a_hash, 'a', a_first = b_hash, 'b', 'neither'),
       multiIf(b_first = a_hash, 'a', b_first = b_hash, 'b', 'neither'),
       multiIf(one_array = a_hash, 'a', one_array = b_hash, 'b', 'neither')
FROM
(
    SELECT decider, sipHash64(a) AS a_hash, sipHash64(b) AS b_hash,
           sipHash64(arrayReduce('timeSeriesGroupArray', [a, b])) AS a_first,
           sipHash64(arrayReduce('timeSeriesGroupArray', [b, a])) AS b_first,
           sipHash64(arrayReduce('timeSeriesGroupArray', [arrayConcat(b, a)])) AS one_array
    FROM duplicate_pairs
)
ORDER BY decider;

DROP TABLE duplicate_pairs;
