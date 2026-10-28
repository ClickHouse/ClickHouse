#!/usr/bin/env bash

# The scalar subquery of a materialized view is evaluated for every inserted block on a pipeline thread
# and polls the interactive cancel callback of `clickhouse-local`, which must not call the client there.
# `--processed-rows` checks that the progress those threads leave for the client thread still reaches it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --processed-rows -m -q "
SET interactive_delay = 10000;
CREATE TABLE src (x UInt64) ENGINE = Null;
CREATE TABLE dst (c UInt64) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT (SELECT count() FROM src) AS c FROM src;
INSERT INTO src SELECT number FROM numbers(10000)
    SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_threads = 4, max_insert_threads = 4;
SELECT count(), min(c) > 0 FROM dst;
"
