-- The generated `samples` column of the samples tables declares the codecs for the elements of its tuples: `Delta, T64, ZSTD(3)`
-- for the timestamps and `ALP, ZSTD(3)` for the values, and the generated `bucket`, `min_time` and `max_time` columns get the codec
-- of the timestamps. A copy of the table (`CREATE TABLE ... AS`) gets the declarations once, and a declared `samples` column keeps
-- the codecs declared for it.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_copy;

SELECT '-- the generated samples column declares the codecs for its elements, in both samples tables';
CREATE TABLE ts ENGINE = TimeSeries;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') AS samples_columns,
       extract(create_table_query, 'RECENT SAMPLES INNER COLUMNS \((.*?)\) RECENT SAMPLES INNER ENGINE') = samples_columns AS same_recent_samples_columns
FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT extract(table, '^\.inner_id\.(\w+)\.') AS inner_table, compression_codec, subcolumn_codecs
FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.%samples.%' AND name = 'samples' ORDER BY inner_table;

SELECT '-- a copy gets the declarations once';
CREATE TABLE ts_copy AS ts;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
INSERT INTO ts_copy (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]);
SELECT samples FROM ts_copy;
DROP TABLE ts_copy;
DROP TABLE ts;

SELECT '-- a declared samples column keeps its own codec and gets no codecs for its elements';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))) CODEC(ZSTD(5)));
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT compression_codec, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.samples.%' AND name = 'samples';
DROP TABLE ts;

SELECT '-- the codecs declared for the elements are kept, also by a copy';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
                       CODEC samples.timestamp (DoubleDelta, ZSTD(1)), CODEC samples.value (Gorilla, ZSTD(1)));
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
CREATE TABLE ts_copy AS ts;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
SELECT compression_codec, subcolumn_codecs FROM system.columns
WHERE database = currentDatabase() AND table = '.inner_id.samples.' || (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy') AND name = 'samples';
INSERT INTO ts_copy (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 1.)]);
SELECT samples FROM ts_copy;
DROP TABLE ts_copy;
DROP TABLE ts;

SELECT '-- a declared samples column without codecs gets the default codec';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))));
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
SELECT compression_codec, subcolumn_codecs FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner\_id.samples.%' AND name = 'samples';
DROP TABLE ts;

SELECT '-- a copy pinned to a version storing one sample per row has no codecs for subcolumns';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
CREATE TABLE ts_copy AS ts ENGINE = TimeSeries SETTINGS version = 10;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts;
