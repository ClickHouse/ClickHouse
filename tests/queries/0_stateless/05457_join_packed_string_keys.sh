#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `parallel_hash` joins on `String` keys, and the joins on keys that are not a single `String`.
TABLES=(s c neg)
# shellcheck source=./05457_join_packed_string_keys.lib
. "$CUR_DIR"/05457_join_packed_string_keys.lib
