#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Before each INSERT with inline data and `async_insert`, the client checks whether stdin has data too.
# stdin is open for writing only, so the first check cannot read it, and the second one must not read it again.
$CLICKHOUSE_LOCAL --async_insert 1 -q "CREATE TABLE t (x Int) ENGINE = Memory; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); SELECT count() FROM t" 0>/dev/null
