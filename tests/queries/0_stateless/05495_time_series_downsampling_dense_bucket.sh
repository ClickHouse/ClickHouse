#!/usr/bin/env bash
# A dense expired bucket must stay under 1 GiB of resident memory when lazy column replication is off.
# A capture of the aggregated array uses several GiB for this bucket. The query memory limit does not count those copies.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

out="${CLICKHOUSE_TMP}/05495_time_series_downsampling_dense_bucket.out"
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
