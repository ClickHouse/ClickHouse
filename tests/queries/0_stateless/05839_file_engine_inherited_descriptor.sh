#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A numeric descriptor of the File engine is the one the shell gives to the process (a kernel
# descriptor), even though the descriptors inside the process are randomized.
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (x UInt64) ENGINE = File(TSV, 3); SELECT sum(x), count() FROM t" 3< <(seq 1 100)
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (x UInt64) ENGINE = File(TSV, 4); INSERT INTO t SELECT number FROM numbers(3)" 4>&1
