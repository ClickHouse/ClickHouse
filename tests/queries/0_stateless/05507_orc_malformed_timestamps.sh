#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# The nanoseconds of a timestamp with the 7 removed trailing zeros overflow `Int64`.
orc_check nanoseconds "$DATA/malformed_timestamp_nanoseconds.orc" 't Nullable(DateTime64(9))' "SELECT count(), max(t) FROM SOURCE" \
    'Invalid timestamp in ORC column 1: seconds=[0-9]+, encoded nanoseconds=-?[0-9]+'

# The minimum nanoseconds in the statistics is 0x80000000: such statistics must not be used by the filter pushdown.
orc_check statistics "$DATA/malformed_timestamp_statistics_nanoseconds.orc" 't DateTime64(9)' \
    "SELECT count() FROM SOURCE WHERE t > toDateTime64('2000-01-01 00:00:00', 9, 'UTC')"
