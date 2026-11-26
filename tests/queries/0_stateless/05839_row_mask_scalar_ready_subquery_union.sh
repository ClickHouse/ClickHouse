#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

# shellcheck source=05839_row_mask_scalar_ready_subquery_union.inc
. "$CUR_DIR"/05839_row_mask_scalar_ready_subquery_union.inc

run_case u8_off UInt8 0 2 3 4 5 0 0 2
run_case u8_on UInt8 0 2 3 4 5 1 1 2
run_case u16_on UInt16 0 2 3 4 5 1 1 2
run_case u32_on UInt32 0 2 3 4 5 1 1 2
