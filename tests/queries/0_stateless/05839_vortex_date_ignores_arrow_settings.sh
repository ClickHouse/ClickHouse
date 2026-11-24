#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `output_format_arrow_*` settings belong to the `Arrow` format: a `Date` written to `Vortex` stays a date even
# with `output_format_arrow_date_as_uint16`, which older compatibility values enable.

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.vortex"

for settings in "output_format_arrow_date_as_uint16 = 1" "compatibility = '26.1'"
do
    echo "$settings"
    $CLICKHOUSE_LOCAL --query "
        SELECT toDate('2020-01-02') AS d, toDate32('1950-01-02') AS d32
        INTO OUTFILE '$FILE' TRUNCATE FORMAT Vortex SETTINGS $settings"
    $CLICKHOUSE_LOCAL --query "DESCRIBE file('$FILE', Vortex)"
    $CLICKHOUSE_LOCAL --query "SELECT * FROM file('$FILE', Vortex)"
done

rm -f "$FILE"
