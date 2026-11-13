#!/usr/bin/env bash
# The downsampling grid must not follow the server compatibility profile.
# `TTL` analysis reads the global settings, so the profile is a command-line setting of `clickhouse local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --allow_experimental_time_series_table=1 --compatibility=26.10 --multiquery <<'SQL'
SET session_timezone = 'UTC';

SELECT '-- sub-millisecond timestamps on either side of an interval boundary both survive';
CREATE TABLE ts (samples Array(Tuple(DateTime64(6, 'UTC'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '1s:20s';
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64('1969-12-31 23:59:39.999999', 6, 'UTC'), 1.), (toDateTime64('1969-12-31 23:59:40.000001', 6, 'UTC'), 2.)]);
OPTIMIZE TABLE ts FINAL;
SELECT length(samples), arrayMap(x -> x.value, samples) FROM timeSeriesSamples(ts);
DROP TABLE ts;

SELECT '-- a bucket before 1970 keeps the last sample of each interval';
CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '30d:20s';
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map(), arrayMap(i -> (toDateTime64(i - 60, 3, 'UTC'), toFloat64(i)), range(60));
OPTIMIZE TABLE ts FINAL;
SELECT arrayMap(x -> toUnixTimestamp64Second(x.timestamp), samples) FROM timeSeriesSamples(ts);
DROP TABLE ts;
SQL
