#!/usr/bin/env bash
# Tags: no-fasttest
# Arrow interval types: `YEAR_MONTH` is read as `IntervalMonth`, `DAY_TIME` as `Tuple(days IntervalDay, milliseconds
# IntervalMillisecond)` and `MONTH_DAY_NANO` as `Tuple(months IntervalMonth, days IntervalDay, nanoseconds
# IntervalNanosecond)`, so the values can be added to dates. Each column has a null row. The tuples are wrapped in
# `Nullable` unless `enable_nullable_tuple_type` is disabled, in which case a null row reads as zero intervals.
# The files were written with nanoarrow and pyarrow (pyarrow cannot build the first two types from Python).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for FILE in "$CURDIR/data_arrow/interval_types.arrow" "$CURDIR/data_arrow/interval_types.arrows"; do
    FMT=Arrow
    [[ "$FILE" == *.arrows ]] && FMT=ArrowStream
    echo "--- ${FMT} ---"
    $CLICKHOUSE_LOCAL -q "
        DESC file('$FILE', $FMT);
        SELECT * FROM file('$FILE', $FMT) ORDER BY id;
        SELECT id, toDate('2026-01-31') + ym, toDateTime64('2026-01-31 00:00:00', 3, 'UTC') + dt.days + dt.milliseconds,
            toDateTime64('2026-01-31 00:00:00', 9, 'UTC') + mdn.months + mdn.days + mdn.nanoseconds
        FROM file('$FILE', $FMT) ORDER BY id;
    "
    # A separate process, so that the schema is not taken from the cache of the inference above.
    $CLICKHOUSE_LOCAL -q "SELECT mdn, toTypeName(mdn) FROM file('$FILE', $FMT) ORDER BY id SETTINGS enable_nullable_tuple_type = 0"
done
