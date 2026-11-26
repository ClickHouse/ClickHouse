#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# shellcheck source=05839_row_mask_scalar_ready_subquery_union.inc
. "$CUR_DIR"/05839_row_mask_scalar_ready_subquery_union.inc

run_case u64_on UInt64 0 2 3 4 5 1 1 2
run_case i64_off Int64 -9223372036854775808 -3 0 4 7 0 0 2
run_case i64_on Int64 -9223372036854775808 -3 0 4 7 1 1 2
