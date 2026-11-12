#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE_1}"

# The creation of an Ordinary database emits a warning.
$CLICKHOUSE_CLIENT --send_logs_level=fatal --allow_deprecated_database_ordinary=1 -q "DROP DATABASE IF EXISTS ${DB}; CREATE DATABASE ${DB} ENGINE = Ordinary"

# A full-definition ATTACH is validated like CREATE.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${DB}.t_direct (x UInt8, c0 UInt8 ALIAS c0) ENGINE = Memory; -- { serverError CYCLIC_ALIASES }"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${DB}"

# Emulates a stored table definition that CREATE would reject in a clickhouse-local data directory: a
# table is created, the columns of its stored definition are replaced with `$2`, and the next start loads it.
data_dir="${CLICKHOUSE_TMP}/data"
stored()
{
    $CLICKHOUSE_LOCAL --path="$data_dir" -q "CREATE TABLE $1 (x UInt8) ENGINE = Memory"
    $CLICKHOUSE_LOCAL --param_metadata="$data_dir/metadata/default/$1.sql" --param_copy="$data_dir.sql" --param_columns="$2" -q "
        SET engine_file_truncate_on_insert = 1;
        INSERT INTO FUNCTION file({copy:String}, 'RawBLOB')
            SELECT replaceOne(raw_blob, '\`x\` UInt8', {columns:String}) FROM file({metadata:String}, 'RawBLOB');
        INSERT INTO FUNCTION file({metadata:String}, 'RawBLOB') SELECT * FROM file({copy:String}, 'RawBLOB')"
}

stored t_direct 'x UInt8, c0 UInt8 ALIAS c0'
stored t_self 'x UInt8, c0 Nullable(IPv6) ALIAS c0.null'
stored t_mutual 'x UInt8, c0 Nullable(UInt8) ALIAS c1.null, c1 Nullable(UInt8) ALIAS c0'
stored t_ok 'x UInt8, arr Array(UInt8), a Tuple(n Nullable(UInt8)) ALIAS tuple(x), b Nullable(UInt8) ALIAS a.n, c0 Array(UInt8) ALIAS arrayMap(c0 -> c0 * 2, arr)'
stored t_probe 'x UInt8, a UInt8 ALIAS t_probe.x, t_probe UInt8 ALIAS a, `n.a` UInt64 ALIAS length(n), `n.b` Array(UInt8)'
stored t_dead 'x Int32, a Int32 ALIAS if(0, b, 1), b Int32 ALIAS arraySum(arrayMap(z -> z + a, [2])), c Int32 ALIAS if(0, d, 1), d Int32 ALIAS q + ((x + c) AS q)'

# A stored definition is not validated again, so the cycle is found when the column is read.
$CLICKHOUSE_LOCAL --path="$data_dir" -m -q "
SELECT c0 FROM t_direct; -- { serverError CYCLIC_ALIASES }

SELECT c0 FROM t_self; -- { serverError CYCLIC_ALIASES }
SELECT if(0, c0, NULL), c0 FROM t_self; -- { serverError CYCLIC_ALIASES }
SELECT multiIf(0, c0, NULL), c0 FROM t_self; -- { serverError CYCLIC_ALIASES }
SELECT c5 FROM format(CSV, 'c0 Int32, c5 String ALIAS c5.size', '1'); -- { serverError CYCLIC_ALIASES }
CREATE TABLE r (x UInt8, k Nullable(IPv6)) ENGINE = Memory;
SELECT x, c0 AS k FROM t_self JOIN r USING (x, k) SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError CYCLIC_ALIASES }
CREATE TABLE m (x UInt8, c0 Nullable(IPv6)) ENGINE = Merge(currentDatabase(), '^t_self$');
SELECT c0 FROM m; -- { serverError CYCLIC_ALIASES }

SELECT c0 FROM t_mutual; -- { serverError CYCLIC_ALIASES }

-- Not cycles.
INSERT INTO t_ok (x, arr) VALUES (3, [1, 2]);
SELECT a, b, c0 FROM t_ok;

-- \`t_probe.x\` and \`n\` first probe the columns \`t_probe\` and \`n.a\`, whose types do not match.
INSERT INTO t_probe (x, \`n.b\`) VALUES (7, [1, 2]);
SELECT t_probe FROM t_probe;
SELECT a FROM t_probe;
SELECT \`n.a\` FROM t_probe;

-- The cycles go through a branch of \`if\` that is not taken: \`b\` and \`d\` are resolved again after the error in that branch.
INSERT INTO t_dead (x) VALUES (7);
SELECT a, b FROM t_dead;
SELECT c, d FROM t_dead;
"
rm -rf "$data_dir" "$data_dir.sql"
