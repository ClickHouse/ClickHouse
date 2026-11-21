#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `hash` and `grace_hash` joins on `Nullable(String)` keys.
TABLES=(n)
# shellcheck source=./05460_join_packed_string_keys_hash_grace.lib
. "$CUR_DIR"/05460_join_packed_string_keys_hash_grace.lib
