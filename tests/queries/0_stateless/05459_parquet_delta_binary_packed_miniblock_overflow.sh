#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A DELTA_BINARY_PACKED page whose header claims a block of 2^61 values in one miniblock of bit width 64,
# followed by only 20 bytes of deltas. The size of the miniblock in bytes, 2^61 * 64 / 8, overflows,
# and the decoder used to unpack all 199 deltas from the rest of the page and beyond it instead of
# rejecting the page. Now the page is rejected already by the bound on the block size.
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('$CUR_DIR/data_parquet/05459_delta_binary_packed_miniblock_overflow.parquet') FORMAT Null" 2>&1 \
    | grep -o -m1 'INCORRECT_DATA\|ATTEMPT_TO_READ_AFTER_EOF'
