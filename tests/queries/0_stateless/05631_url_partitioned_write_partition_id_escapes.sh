#!/usr/bin/env bash

# The partition IDs `a b` and `a%20b` are sent to different targets; the target records the ID it received.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05631 (target String, k String, x UInt32) ENGINE = Memory"

# `{` and `}` are escaped, or they would be a glob.
target_url="${CLICKHOUSE_URL}&query=INSERT INTO t_05631 SELECT %7Bp:String%7D, k, x FROM input(\\'k String, x UInt32\\') FORMAT CSV&param_p={_partition_id}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION url('$target_url', 'CSV', 'k String, x UInt32') PARTITION BY k
    SELECT * FROM values('k String, x UInt32', ('a b', 1), ('a%20b', 2))"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_05631 ORDER BY x"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05631"
