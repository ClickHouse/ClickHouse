-- The counter reset hints after `timeSeriesGroupArray` merges arrays of histogram samples: every sample whose predecessor
-- may have changed (the first sample of every appended array but the first, every sample but the first of overlapping arrays) gets its
-- hint detected again from the data against its predecessor in the result, as the sink does within one insert (a count decrease is a
-- reset), whatever hint it had; the other hints and gauge markers are kept, a single array is returned as it is, the result doesn't
-- depend on the order of the inputs, and the hints don't take part in choosing which payload wins a conflict. The same happens when
-- the parts of the samples table are merged.
SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_threads = 1;

DROP TABLE IF EXISTS histogram_hint_inputs;
DROP TABLE IF EXISTS histogram_hint_states;
DROP TABLE IF EXISTS ts_histogram_hints;
CREATE TABLE histogram_hint_inputs
(
    scenario String,
    part UInt8,
    h Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64)))
) ENGINE = Memory;

INSERT INTO histogram_hint_inputs
SELECT scenario, part, arrayMap(s -> (s.1, 0, s.3, 0, 0., toFloat64(s.2), s.2, s.2, 0., 0., [], [], [], [], []), samples)
FROM values('scenario String, part UInt8, samples Array(Tuple(UInt32, Int64, UInt8))',
    ('conflict', 1, [(100, 100, 2), (101, 120, 2)]),
    ('conflict', 2, [(100, 1, 2), (101, 2, 2), (102, 3, 2)]),
    ('append', 1, [(100, 100, 2), (101, 120, 2)]),
    ('append', 2, [(102, 3, 2), (103, 4, 2)]),
    ('interleave', 1, [(100, 1, 2), (102, 3, 2)]),
    ('interleave', 2, [(101, 2, 1), (103, 4, 1)]),
    ('gauge', 1, [(100, 100, 3), (101, 120, 3)]),
    ('gauge', 2, [(100, 1, 3), (101, 2, 3), (102, 3, 3)]),
    ('single', 1, [(100, 1, 2), (101, 2, 1)]));

-- Appended arrays keep their hints but for their first sample (the sink detected them), sorted arrays get all of them detected;
-- gauge markers are kept; the single array of `single` is returned as it is.
SELECT scenario, arrayMap(s -> (s.timestamp, s.count_int, s.counter_reset_hint), merged),
       merged = arrayReduce('timeSeriesGroupArray', arrayReverse(arrays))
FROM
(
    SELECT scenario, arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((part, h)))) AS arrays,
           arrayReduce('timeSeriesGroupArray', arrays) AS merged
    FROM histogram_hint_inputs GROUP BY scenario
)
ORDER BY scenario;

-- Serialized states retain the input boundaries required for detecting hints after merging.
CREATE TABLE histogram_hint_states ENGINE = MergeTree ORDER BY part AS
SELECT part, timeSeriesGroupArrayState(h) AS state
FROM histogram_hint_inputs WHERE scenario = 'conflict' GROUP BY part;
SELECT arrayMap(s -> (s.count_int, s.counter_reset_hint), timeSeriesGroupArrayMerge(state)) FROM histogram_hint_states;
DROP TABLE histogram_hint_states;

-- The counter hints don't take part in choosing which payload wins a conflict: the inner merge of parts 1 and 3 makes the hint of
-- part 3 unknown (the schema changed), and the outer merge must still prefer its payload (schema 1) to part 2.
INSERT INTO histogram_hint_inputs
SELECT 'payload', part, arrayMap(s -> (s.1, 0, s.3, schema, 0., toFloat64(s.2), s.2, s.2, 0., 0., [], [], [], [], []), samples)
FROM values('part UInt8, schema Int8, samples Array(Tuple(UInt32, Int64, UInt8))',
    (1, 0, [(100, 1, 2)]), (2, 0, [(101, 2, 2)]), (3, 1, [(101, 2, 2)]));
SELECT arrayMap(s -> (s.schema, s.counter_reset_hint), arrayReduce('timeSeriesGroupArray', [arrays[2], arrays[3]])),
       arrayMap(s -> (s.schema, s.counter_reset_hint), arrayReduce('timeSeriesGroupArray',
           [arrayReduce('timeSeriesGroupArray', [arrays[1], arrays[3]]), arrays[2]]))
FROM
(
    SELECT arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((part, h)))) AS arrays
    FROM histogram_hint_inputs WHERE scenario = 'payload'
);

-- The hints are detected again the same way when the parts of the samples table are merged: the two inserts make two parts with a row of the same
-- bucket each, which stay unmerged (`max_bytes_to_merge_at_max_space_in_pool = 1` disables background merges) until
-- `OPTIMIZE TABLE ... FINAL` merges them. The hints are read as stored, without the aggregate, so the check sees the merge itself.
CREATE TABLE ts_histogram_hints ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO ts_histogram_hints (metric_name, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.sum, histograms.counter_reset_hint)
VALUES ('m', [toDateTime64(100, 3), toDateTime64(101, 3)], [100, 120], [100, 120], [100, 120], [2, 2]);
INSERT INTO ts_histogram_hints (metric_name, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.sum, histograms.counter_reset_hint)
VALUES ('m', [toDateTime64(100, 3), toDateTime64(101, 3), toDateTime64(102, 3)], [1, 2, 3], [1, 2, 3], [1, 2, 3], [2, 2, 2]);
SELECT arrayMap(s -> (s.count_int, s.counter_reset_hint), histograms) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_histogram_hints) ORDER BY ALL;
OPTIMIZE TABLE ts_histogram_hints FINAL;
SELECT arrayMap(s -> (s.count_int, s.counter_reset_hint), histograms) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_histogram_hints) ORDER BY ALL;

-- Sorting and deduplicating a single insert detects the reset against the new predecessor.
TRUNCATE TABLE ts_histogram_hints;
INSERT INTO ts_histogram_hints (metric_name, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.sum, histograms.counter_reset_hint)
VALUES ('m', [toDateTime64(100, 3), toDateTime64(101, 3), toDateTime64(100, 3), toDateTime64(101, 3), toDateTime64(102, 3)],
        [100, 120, 1, 2, 3], [100, 120, 1, 2, 3], [100, 120, 1, 2, 3], [2, 2, 2, 2, 2]);
SELECT arrayMap(s -> (s.count_int, s.counter_reset_hint), histograms) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_histogram_hints);

DROP TABLE ts_histogram_hints;
DROP TABLE histogram_hint_inputs;
