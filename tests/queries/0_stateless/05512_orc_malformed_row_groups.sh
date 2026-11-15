#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# The stripe has 2^64 - 1 rows: rounding up the number of row groups overflows.
ORC_PUSH_DOWN_VALUES=1 orc_check row_groups "$DATA/malformed_row_group_count_overflow.orc" 'x Int64' "SELECT x FROM SOURCE WHERE x = 1" \
    'The row index of ORC column 1 has 2 entries, less than the [0-9]+ row groups of stripe 0'
ORC_PUSH_DOWN_VALUES=0 orc_check row_groups "$DATA/malformed_row_group_count_overflow.orc" 'x Int64' "SELECT x FROM SOURCE WHERE x = 1" \
    'Unexpected end of ORC stream column 1 kind 1'

# A PATCHED_BASE run whose last patch is a continuation (gap 255, patch 0).
orc_check patched_base "$DATA/malformed_patched_base_terminal_continuation.orc" 'x Int64' "SELECT count(), sum(x) FROM SOURCE" \
    'Corrupt PATCHED_BASE encoded data in ORC stream column 1 kind 1 in stripe 0 \(unterminated gap\)'

# A row index position beyond the end of the stream, and the same file with the valid position.
orc_check row_index_seek "$DATA/malformed_row_index_seek_past_stream.orc" 'x Int64' "SELECT x FROM SOURCE WHERE x = 1" \
    'Seek position 127 is beyond the end of ORC stream column 1 kind 1 in stripe 0 of size 3'
orc_check row_index_control "$DATA/row_index_seek_control.orc" 'x Int64' "SELECT x FROM SOURCE WHERE x = 1"
