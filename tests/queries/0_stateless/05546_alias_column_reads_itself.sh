#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE_1}"

# The creation of an Ordinary database emits a warning.
$CLICKHOUSE_CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB} ENGINE = Ordinary"

# Emulates a stored table definition that CREATE would reject: a table is created, its stored
# definition is replaced with `$2`, and the table is attached back with the short ATTACH.
attach_stored()
{
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.$1 (x UInt8) ENGINE = Memory"
    local metadata_file
    metadata_file=$($CLICKHOUSE_CLIENT -q "SELECT getServerSetting('path') || metadata_path FROM system.tables WHERE database = '${DB}' AND name = '$1'")
    $CLICKHOUSE_CLIENT -q "DETACH TABLE ${DB}.$1"
    echo "ATTACH TABLE $1 ($2) ENGINE = Memory" > "$metadata_file"
    $CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.$1"
}

# A full-definition ATTACH is validated like CREATE.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.t_direct (x UInt8, c0 UInt8 ALIAS c0) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }"

# A stored definition is not validated again, so the cycle is found when the column is read.
attach_stored t_direct 'x UInt8, c0 UInt8 ALIAS c0'
$CLICKHOUSE_CLIENT -m <<EOF
SELECT c0 FROM ${DB}.t_direct; -- { serverError CYCLIC_ALIASES }
EOF

attach_stored t_self 'x UInt8, c0 Nullable(IPv6) ALIAS c0.null'
$CLICKHOUSE_CLIENT -m <<EOF
SELECT c0 FROM ${DB}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT c0 FROM remote('127.0.0.1', ${DB}, t_self); -- { serverError CYCLIC_ALIASES }
SELECT if(0, c0, NULL), c0 FROM ${DB}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT multiIf(0, c0, NULL), c0 FROM ${DB}.t_self; -- { serverError CYCLIC_ALIASES }
SELECT c5 FROM format(CSV, 'c0 Int32, c5 String ALIAS c5.size', '1'); -- { serverError CYCLIC_ALIASES }
CREATE TABLE ${DB}.r (x UInt8, k Nullable(IPv6)) ENGINE = Memory;
SELECT x, c0 AS k FROM ${DB}.t_self JOIN ${DB}.r USING (x, k) SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError CYCLIC_ALIASES }
CREATE TABLE ${DB}.m (x UInt8, c0 Nullable(IPv6)) ENGINE = Merge(${DB}, '^t_self$');
SELECT c0 FROM ${DB}.m; -- { serverError CYCLIC_ALIASES }
EOF

attach_stored t_mutual 'x UInt8, c0 Nullable(UInt8) ALIAS c1.null, c1 Nullable(UInt8) ALIAS c0'
$CLICKHOUSE_CLIENT -m <<EOF
SELECT c0 FROM ${DB}.t_mutual; -- { serverError CYCLIC_ALIASES }
EOF

# Not cycles.
attach_stored t_ok 'x UInt8, arr Array(UInt8), a Tuple(n Nullable(UInt8)) ALIAS tuple(x), b Nullable(UInt8) ALIAS a.n, c0 Array(UInt8) ALIAS arrayMap(c0 -> c0 * 2, arr)'
$CLICKHOUSE_CLIENT -m <<EOF
INSERT INTO ${DB}.t_ok (x, arr) VALUES (3, [1, 2]);
SELECT a, b, c0 FROM ${DB}.t_ok;
EOF

# `t_probe.x` and `n` first probe the columns `t_probe` and `n.a`, whose types do not match.
attach_stored t_probe 'x UInt8, a UInt8 ALIAS t_probe.x, t_probe UInt8 ALIAS a, `n.a` UInt64 ALIAS length(n), `n.b` Array(UInt8)'
$CLICKHOUSE_CLIENT -m <<EOF
INSERT INTO ${DB}.t_probe (x, \`n.b\`) VALUES (7, [1, 2]);
SELECT t_probe FROM ${DB}.t_probe;
SELECT a FROM ${DB}.t_probe;
SELECT \`n.a\` FROM ${DB}.t_probe;
EOF

# The cycles go through a branch of `if` that is not taken: `b` and `d` are resolved again after the error in that branch.
attach_stored t_dead 'x Int32, a Int32 ALIAS if(0, b, 1), b Int32 ALIAS arraySum(arrayMap(z -> z + a, [2])), c Int32 ALIAS if(0, d, 1), d Int32 ALIAS q + ((x + c) AS q)'
$CLICKHOUSE_CLIENT -m <<EOF
INSERT INTO ${DB}.t_dead (x) VALUES (7);
SELECT a, b FROM ${DB}.t_dead;
SELECT c, d FROM ${DB}.t_dead;
EOF

$CLICKHOUSE_CLIENT -m <<EOF
DROP DATABASE ${DB};
EOF
