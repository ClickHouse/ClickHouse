-- A TimeSeries table of version 12 stores native histogram samples in the `histograms` column of its samples tables, next to the
-- float samples: the column is generated with the codecs of its subcolumns, kept by `CREATE TABLE ... AS`, and mirrored by the
-- `histograms.*` outer columns. Tables of version 11 have neither. A histogram sample written through the outer columns is read back
-- through them (the details of the writes are in 05458_timeseries_histograms_insert, of the reads in 05461_timeseries_histograms_outer_read).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
-- The histogram tuple type is printed in one line.
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS ts_copy_v11;
DROP TABLE IF EXISTS ts_v11;
DROP TABLE IF EXISTS ts_limit;

SELECT '-- the default version stores histograms';
CREATE TABLE ts ENGINE = TimeSeries;
SELECT extract(create_table_query, 'version = (\d+)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';

SELECT '-- the samples tables have the histograms column with the codecs of its subcolumns';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') AS samples_columns,
       extract(create_table_query, 'RECENT SAMPLES INNER COLUMNS \((.*?)\) RECENT SAMPLES INNER ENGINE') = samples_columns AS same_recent_samples_columns
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT extract(table, '^\.inner_id\.(\w+)\.') AS inner_table, type, compression_codec, mapSort(subcolumn_codecs)
FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.%samples.%' AND name = 'histograms' ORDER BY inner_table;

SELECT '-- the outer table has the histograms.* columns';
DESCRIBE TABLE ts;

SELECT '-- a copy keeps the column and the codecs of its subcolumns once';
CREATE TABLE ts_copy AS ts;
SELECT (SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy')
     = (SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts');
SELECT countSubstrings(create_table_query, 'CODEC `histograms.timestamp`') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';

SELECT '-- a table of version 11 has neither the column nor the outer columns and rejects the settings of the histograms';
CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11;
SELECT countSubstrings(create_table_query, 'histograms') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v11';
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_v11' AND name LIKE 'histograms.%';
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS version = 11, histograms_max_buckets = 10; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- a copy pinned to version 11 drops the column, a table of the current version accepts the setting';
CREATE TABLE ts_copy_v11 AS ts ENGINE = TimeSeries SETTINGS version = 11;
SELECT countSubstrings(create_table_query, 'histograms') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy_v11';
CREATE TABLE ts_limit ENGINE = TimeSeries SETTINGS histograms_max_buckets = 10;
SELECT extract(create_table_query, 'histograms_max_buckets = (\d+)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_limit';

SELECT '-- float samples are written and read as before, histogram samples are written and read through the histograms.* columns';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.)]);
SELECT metric_name, samples FROM ts;
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold, histograms.sum,
                histograms.count_int, histograms.zero_count_int, histograms.count_float, histograms.zero_count_float,
                histograms.positive_int, histograms.negative_int, histograms.positive_float, histograms.negative_float, histograms.custom_values)
    VALUES ('h', map(), [toDateTime64(100, 3)], [0], [0], [0], [0.], [1.], [1], [0], [0.], [0.], [[(0, 1)]], [[]], [[]], [[]], [[]]);
SELECT length(samples), length(histograms), histograms.count_int FROM timeSeriesSamples(ts) ORDER BY ALL;
SELECT histograms.count_int FROM ts ORDER BY ALL;
SELECT metric_name, samples, histograms.timestamp, histograms.count_int, histograms.positive_int FROM ts ORDER BY metric_name;
SELECT count() FROM ts;

DROP TABLE ts_limit;
DROP TABLE ts_v11;
DROP TABLE ts_copy_v11;
DROP TABLE ts_copy;
DROP TABLE ts;
