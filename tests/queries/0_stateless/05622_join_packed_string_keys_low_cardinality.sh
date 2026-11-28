#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `parallel_hash` joins on `LowCardinality(String)` and `LowCardinality(Nullable(String))` keys.
TABLES=(lc lcn)
# shellcheck source=./05457_join_packed_string_keys.lib
. "$CUR_DIR"/05457_join_packed_string_keys.lib
