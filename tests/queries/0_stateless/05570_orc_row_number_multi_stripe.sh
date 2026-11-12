#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `row_numbers_multi_stripe.orc`: `id Int64` = 0..9999, so `_row_number = id`, in 3 stripes of 1500, 2500 and 6000 rows
# with a row index stride of 100 (written by pyarrow).
FILE="$CUR_DIR/data_orc/row_numbers_multi_stripe.orc"

# `input_format_orc_row_batch_size` = 37: the batches do not align with the stripes and the row groups.
$CLICKHOUSE_LOCAL -q "SELECT count(), countIf(_row_number = id) FROM file('$FILE', ORC) SETTINGS input_format_orc_row_batch_size = 37"
$CLICKHOUSE_LOCAL -q "SELECT count(), countIf(_row_number = id) FROM file('$FILE', ORC)"

# The filter push-down skips stripe 0 by its statistics and all row groups but two in stripes 1 and 2, so only 200 rows
# are read and the row numbers must count the skipped ones.
QUERY="SELECT count(), countIf(_row_number = id), min(id), max(id) FROM file('$FILE', ORC)
    WHERE id BETWEEN 2550 AND 2560 OR id BETWEEN 7000 AND 7010
    SETTINGS input_format_orc_filter_push_down = 1, input_format_orc_row_batch_size = 37"
$CLICKHOUSE_LOCAL -q "$QUERY"
$CLICKHOUSE_LOCAL --print-profile-events --profile-events-delay-ms=-1 -q "$QUERY FORMAT Null" |& grep -o -m1 'SelectedRows: [0-9]*'
