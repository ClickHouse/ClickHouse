-- The order of `TimeSeriesDuplicateTimestampRule` over the payload elements compared after the stale marker, the count and the sum:
-- of each pair of histogram samples with the same timestamp, `timeSeriesGroupArray` keeps `a` whichever array comes first. A pair is
-- named after the key deciding it, and a later key that differs favours `b`, so a key compared too late or not at all shows up as `b`
-- or as a winner depending on the input order.

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
    ('is_float',
        [(100, true, 'unknown', 0, 0., 1., 0, 0, 5., 0., [], [], [(0, 5.)], [], [])],
        [(100, false, 'unknown', 0, 0., 1., 5, 0, 0., 0., [(0, 5)], [], [], [], [])]),
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

SELECT decider,
       multiIf(a_first = a, 'a', a_first = b, 'b', 'neither'),
       multiIf(b_first = a, 'a', b_first = b, 'b', 'neither')
FROM
(
    SELECT decider, a, b,
           arrayReduce('timeSeriesGroupArray', [a, b]) AS a_first,
           arrayReduce('timeSeriesGroupArray', [b, a]) AS b_first
    FROM duplicate_pairs
)
ORDER BY decider;

DROP TABLE duplicate_pairs;
