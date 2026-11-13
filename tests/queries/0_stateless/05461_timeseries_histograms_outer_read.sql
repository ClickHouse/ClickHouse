-- Reading native histogram samples from a TimeSeries table through the `histograms.*` outer columns (see makeASTSelectFromTimeSeries):
-- the stored tuples of the `histograms` column of the samples table are returned flattened into one array per element, one outer row
-- per row of the samples table (a series within a bucket), like `samples`. Without `FINAL` an outer column is a subcolumn of the
-- stored column; with `FINAL` the rows of a bucket which are not merged yet are merged by `timeSeriesGroupArray`.
-- Every case uses a metric name of its own, and background merges of the samples table are disabled, so the checks of the
-- unmerged rows don't depend on when a merge happens.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '-- an integer histogram and a float histogram: every element of the stored tuple is an outer array';
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold, histograms.sum,
                histograms.count_int, histograms.zero_count_int, histograms.positive_int, histograms.negative_int)
    VALUES ('int_hist', map('job', 'api'), [toDateTime64(100, 3)], [0], [2], [3], [0.001], [12.5], [10], [2], [[(-2, 3), (-1, 2), (1, 3)]], [[(0, 0)]]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
                histograms.count_float, histograms.zero_count_float, histograms.positive_float, histograms.negative_float)
    VALUES ('float_hist', map('job', 'api'), [toDateTime64(100, 3)], [1], [3], [0], [3.25], [5.75], [0.5], [[(0, 2.5), (1, 2.5)]], [[(1, 0.25)]]);

SELECT metric_name, tags, samples, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold, histograms.sum,
       histograms.count_int, histograms.zero_count_int, histograms.count_float, histograms.zero_count_float,
       histograms.positive_int, histograms.negative_int, histograms.positive_float, histograms.negative_float, histograms.custom_values
FROM ts WHERE metric_name IN ('int_hist', 'float_hist') ORDER BY metric_name;

SELECT '-- a float-only series has empty histograms.* arrays next to its samples';
INSERT INTO ts (metric_name, tags, samples) VALUES ('floats', map('job', 'api'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);
SELECT metric_name, samples, histograms.timestamp, histograms.count_int, histograms.custom_values FROM ts WHERE metric_name = 'floats';

SELECT '-- a series with float samples and histograms in two buckets: one outer row per bucket with both kinds of its bucket';
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('mixed', map('job', 'api'), [(toDateTime64(70, 3), 1.), (toDateTime64(130, 3), 2.)],
            [toDateTime64(80, 3), toDateTime64(125, 3), toDateTime64(140, 3)], [1, 2, 3], [0.5, 1.5, 2.5], [[(0, 1)], [(0, 2)], [(0, 3)]]);
SELECT metric_name, samples, histograms.timestamp, histograms.count_int, histograms.sum FROM ts WHERE metric_name = 'mixed' ORDER BY samples;

SELECT '-- subsets of the columns: the samples table alone, with the tags table, with the metric families table';
SELECT histograms.count_int FROM ts ORDER BY ALL;
SELECT metric_name, histograms.sum FROM ts ORDER BY ALL;
SELECT samples, histograms.timestamp FROM ts ORDER BY ALL;
INSERT INTO ts (metric_family, type, unit, help) VALUES
    ('mixed', 'histogram', 'seconds', 'Mixed samples'),
    ('float_hist', 'gaugehistogram', 'seconds', 'Float histogram');
SELECT histograms.count_int, type FROM ts ORDER BY ALL;

-- Native histograms match their unsuffixed family name, including when filtering by metadata with `FINAL`.
SELECT metric_name, metric_family, type, unit, help, histograms.count_int
FROM ts FINAL
WHERE type IN ('histogram', 'gaugehistogram') AND notEmpty(histograms.timestamp)
ORDER BY ALL;

SELECT '-- ARRAY JOIN over the arrays of the group';
SELECT metric_name, t, c FROM ts ARRAY JOIN histograms.timestamp AS t, histograms.count_int AS c ORDER BY metric_name, t;

SELECT '-- two inserts into one bucket make two rows; FINAL merges them: sorted, the greatest count wins for the duplicate timestamp 220';
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('parts', map('job', 'api'), [toDateTime64(200, 3), toDateTime64(220, 3)], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 2)]]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('parts', map('job', 'api'), [toDateTime64(210, 3), toDateTime64(220, 3)], [3, 5], [2.5, 4.5], [[(0, 3)], [(0, 5)]]);
SELECT histograms.timestamp, histograms.count_int FROM ts WHERE metric_name = 'parts' ORDER BY ALL;
SELECT histograms.timestamp, histograms.count_int, histograms.sum FROM ts FINAL WHERE metric_name = 'parts';
SELECT samples, histograms.count_int FROM ts FINAL WHERE metric_name = 'mixed' ORDER BY ALL;
-- The rows of the samples table: the two rows of `parts` are merged into one by FINAL (`count()` alone would count the tags rows).
SELECT count(samples) FROM ts;
SELECT count(samples) FROM ts FINAL;

SELECT '-- merging an aggregate state preserves ordering and deduplication, including after serialization';
SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), h.count_int), timeSeriesGroupArrayMerge(state))
FROM
(
    SELECT timeSeriesGroupArrayState(histograms) AS state
    FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts)
    WHERE id IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'parts')
)
SETTINGS max_threads = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_histogram_states.native', Native)
SELECT timeSeriesGroupArrayState(histograms) AS state
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts)
WHERE id IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'parts')
SETTINGS engine_file_truncate_on_insert = 1, max_threads = 1;

SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), h.count_int), timeSeriesGroupArrayMerge(state))
FROM file(currentDatabase() || '_histogram_states.native', Native);

SELECT '-- a single sorted input array is deduplicated, with the greatest count winning';
SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), h.count_int), timeSeriesGroupArray(histograms))
FROM
(
    SELECT arraySort(h -> h.timestamp, arrayFlatten(groupArray(histograms))) AS histograms
    FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts)
    WHERE id IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'parts')
);

SELECT '-- disjoint input arrays can each contain duplicate timestamps';
SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), h.count_int),
               timeSeriesGroupArray(arraySort(h -> h.timestamp, arrayConcat(histograms, histograms))))
FROM
(
    SELECT histograms FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts)
    WHERE id IN (SELECT id FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) WHERE metric_name = 'mixed')
    ORDER BY bucket
)
SETTINGS max_threads = 1;

DROP TABLE ts;
