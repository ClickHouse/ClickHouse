#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest, the test uses S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./orc_malformed.lib
. "$CUR_DIR"/orc_malformed.lib

DATA="$CUR_DIR/data_orc"

# The sum of the offset and the lengths of the stripe overflows `UInt64`.
orc_check stripe "$DATA/malformed_stripe_offset_overflow.orc" 'a UInt64, b String' "SELECT count(), sum(length(b)) FROM SOURCE" \
    'Malformed stripe information at stripe index 0'

# The length of a stream of a column that is not read is 2^64 - 1, the offsets of the next streams wrap around.
orc_check stream "$DATA/malformed_stream_offset_overflow.orc" 'b String' "SELECT count(), sum(length(b)) FROM SOURCE" \
    'Malformed stream.*11.*18446744073709551615'
