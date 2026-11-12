#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `hash` and `grace_hash` joins on `String` keys, the joins on keys that are not a single `String`, and the tables
# with the `Join` engine.
TABLES=(s c neg j)
# shellcheck source=./05460_join_packed_string_keys_hash_grace.lib
. "$CUR_DIR"/05460_join_packed_string_keys_hash_grace.lib
