#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query parameter of type `UUID2` (also nested) keeps the identifier it was given.

u="61f0c404-5cb3-11e7-907b-a6006ad3dba0"

$CLICKHOUSE_CLIENT --param_p="$u" -q "SELECT toTypeName({p:UUID2}), toString({p:UUID2}), {p:UUID2} = toUUID2('$u'), {p:UUID2} = toUUID('$u')"
$CLICKHOUSE_CLIENT --param_p="['$u']" -q "SELECT toString({p:Array(UUID2)}[1])"
$CLICKHOUSE_CLIENT --param_p="('$u', 1)" -q "SELECT toString(tupleElement({p:Tuple(UUID2, UInt8)}, 1))"
$CLICKHOUSE_CLIENT --param_p="$u" -q "SELECT toString({p:Nullable(UUID2)}), toString({p:LowCardinality(UUID2)})"
$CLICKHOUSE_CLIENT --param_p="$u" -q "SELECT toString({p:UUID})"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&param_p=$u" -d "SELECT toString({p:UUID2})"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05398 (u UUID2) ENGINE = MergeTree ORDER BY u"
$CLICKHOUSE_CLIENT --param_p="$u" -q "INSERT INTO t_05398 SELECT {p:UUID2}"
$CLICKHOUSE_CLIENT --param_p="$u" -q "SELECT toString(u) FROM t_05398 WHERE u = {p:UUID2}"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_05398"
