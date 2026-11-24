-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` needs ANTLR4, which the fast-test build disables.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS squality_time_series_selector_internal_in;
CREATE TABLE squality_time_series_selector_internal_in
ENGINE = TimeSeries SETTINGS version = 10
TAGS INNER COLUMNS (id Tuple(UInt64, UInt64));
INSERT INTO squality_time_series_selector_internal_in (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]);

SELECT count()
FROM timeSeriesSelector(squality_time_series_selector_internal_in, 'foo', 0, 1000, 'timestamp_and_value')
SETTINGS in_null_as_unknown = 1;

DROP TABLE squality_time_series_selector_internal_in;
