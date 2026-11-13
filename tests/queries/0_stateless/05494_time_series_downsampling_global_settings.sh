#!/usr/bin/env bash
# The downsampling TTL reads the global settings, so each case sets them on the command line of `clickhouse local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '-- the grid does not follow the compatibility profile'
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

echo '-- a dense expired bucket stays under 1 GiB of resident memory with lazy column replication off'
# A capture of the aggregated array uses several GiB for this bucket, and the query memory limit does not count those copies.
out="${CLICKHOUSE_TMP}/05494_time_series_downsampling_global_settings.out"
$CLICKHOUSE_LOCAL --allow_experimental_time_series_table=1 --enable_lazy_columns_replication=0 --multiquery >"$out" <<'SQL' &
SET session_timezone = 'UTC';

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, samples_downsampling_periods = '30d:5m';
INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map(), arrayMap(i -> (toDateTime64('2000-01-01 00:00:00', 3, 'UTC') + toIntervalMillisecond(i * 100), toFloat64(i)), range(10000));
OPTIMIZE TABLE ts FINAL;
SELECT length(samples) FROM timeSeriesSamples(ts);
SQL
pid=$!
peak=0
while kill -0 "$pid" 2>/dev/null; do
    hwm=$(awk '/VmHWM:/ {print $2}' "/proc/${pid}/status" 2>/dev/null || true)
    if [[ -n "${hwm}" && "${hwm}" -gt "${peak}" ]]; then
        peak=${hwm}
    fi
    sleep 0.02
done
wait "$pid"
status=$?
if [[ "$status" -ne 0 || "$peak" -gt 1048576 ]]; then
    echo "status ${status} peak_kib ${peak}"
    exit 1
fi
cat "$out"
rm -f "$out"
