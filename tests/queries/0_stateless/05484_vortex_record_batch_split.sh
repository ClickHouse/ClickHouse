#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Vortex` writer receives a block as an Arrow table that is split into several record batches when
# it exceeds `output_format_arrow_max_record_batch_size_bytes`. Each of them has to be written: the data
# must read back whole, in order.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex
trap 'rm -f "$DATA_FILE"' EXIT

$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, repeat(toString(number % 10), 100) AS s, [toString(number), toString(number + 1)] AS a
    FROM numbers(100000)
    SETTINGS max_block_size = 100000, output_format_arrow_max_record_batch_size_bytes = 65536
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex"

$CLICKHOUSE_LOCAL -q "
    SELECT count(), sum(n), sum(length(s)), countIf(s != repeat(toString(n % 10), 100)), countIf(a != [toString(n), toString(n + 1)])
    FROM file('$DATA_FILE', 'Vortex')"
