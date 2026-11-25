#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Delta Lake kernel (Rust) is not built in the fast test
# no-msan: the Delta Lake kernel (Rust) is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An exception thrown in a callback that the kernel calls must reach the query instead of unwinding through Rust.
DELTA="deltaLakeLocal('$CUR_DIR/data_delta_lake/kernel_golden/basic-with-inserts-updates/delta')"

$CLICKHOUSE_LOCAL -q "
SYSTEM ENABLE FAILPOINT delta_lake_metadata_callback_throw;
SELECT 'metadata', count() FROM $DELTA;
" 2>&1 | grep -o "Forced by the [a-z_]* failpoint" | awk '!seen[$0]++'

$CLICKHOUSE_LOCAL -q "SELECT 'metadata without failpoint', count() FROM $DELTA"
