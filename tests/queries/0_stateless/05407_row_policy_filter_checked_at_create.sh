#!/usr/bin/env bash
# CREATE and ALTER ROW POLICY analyze the filter against every existing table the policy is for, in the
# creator's session, and fail if it cannot be applied there. Such a policy used to be stored and then made
# every read of the table fail. Users, functions and the second database are global, so they are named
# after the test database; row policies are dropped explicitly because they outlive the test database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_LAZY="${CLICKHOUSE_DATABASE}_lazy"
U="u_${CLICKHOUSE_DATABASE}"
FIN="fin_${CLICKHOUSE_DATABASE}"
FDICT="fdict_${CLICKHOUSE_DATABASE}"
FPART="fpart_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT <<EOF
CREATE TABLE t_src (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_src SELECT number, toString(number) FROM numbers(10);
CREATE TABLE t_x (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_x SELECT number, number FROM numbers(10);
CREATE TABLE t_ids (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ids SELECT number FROM numbers(10);
CREATE TABLE t_arr (id UInt64, allowed Array(UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_arr VALUES (1, [1, 2]), (2, [3]);
CREATE TABLE a_src ENGINE = Alias('t_src');

-- An unknown column is rejected, and nothing is stored.
CREATE ROW POLICY p ON t_src USING nosuchcol = 1 TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
SELECT 'stored', count() FROM system.row_policies WHERE database = currentDatabase();

-- A policy created before its table is not checked.
CREATE ROW POLICY p_later ON t_later USING anything = 1 TO ALL;
DROP ROW POLICY p_later ON t_later;

-- A database-level policy is checked against every table of the database.
CREATE ROW POLICY p_db ON * USING x = 1 TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
CREATE ROW POLICY p_db ON * USING id >= 0 TO ALL;
DROP ROW POLICY p_db ON *;

-- Every table is checked, not only the first one: only z_last lacks id.
CREATE TABLE z_last (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE ROW POLICY p_db ON * USING id >= 0 TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE z_last;

-- An Alias whose table is gone has no columns, so it is not checked.
CREATE TABLE t_gone (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE a_gone ENGINE = Alias('t_gone');
DROP TABLE t_gone;
CREATE ROW POLICY p_db ON * USING id >= 0 TO ALL;
DROP ROW POLICY p_db ON *;
CREATE ROW POLICY p_gone ON a_gone USING nosuch = 1 TO ALL;
DROP ROW POLICY p_gone ON a_gone;
DROP TABLE a_gone;

-- ALTER is checked too, and a rejected ALTER keeps the policy.
CREATE ROW POLICY p_ok ON t_src USING id < 5 TO ALL;
ALTER ROW POLICY p_ok ON t_src USING nosuchcol = 1; -- { serverError UNKNOWN_IDENTIFIER }
SELECT 'kept', count() FROM t_src;

-- CREATE IF NOT EXISTS of an existing policy installs nothing, so there is nothing to check.
CREATE ROW POLICY IF NOT EXISTS p_ok ON t_src USING nosuchcol = 1 TO ALL;
SELECT 'still kept', count() FROM t_src;
DROP ROW POLICY p_ok ON t_src;

-- A filter that reads another table is resolved against the reader's current database, so it is not checked.
USE default;
CREATE ROW POLICY p_sub ON ${CLICKHOUSE_DATABASE}.t_src USING id IN (SELECT id FROM t_x) TO ALL;
CREATE ROW POLICY p_in ON ${CLICKHOUSE_DATABASE}.t_src USING id IN t_ids TO ALL;
USE ${CLICKHOUSE_DATABASE};
SELECT 'lookup policies', count() FROM t_src;
DROP ROW POLICY p_sub ON t_src;
DROP ROW POLICY p_in ON t_src;

-- So is one that reads a dictionary through an ALIAS column: qualified ALIAS chains, one through a subcolumn.
CREATE DICTIONARY d_val (id UInt64, val String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't_src' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE t_alias (id UInt64, val String ALIAS dictGet(concat('d_val', ''), 'val', id), val2 String ALIAS val, tup Tuple(val String) ALIAS tuple(val))
    ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias SELECT number FROM numbers(10);
CREATE ROW POLICY p_alias ON t_alias USING wrong.val2 = '3' TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
USE default;
CREATE ROW POLICY p_alias ON ${CLICKHOUSE_DATABASE}.t_alias USING t_alias.val2 = '3' TO ALL;
CREATE ROW POLICY p_alias_db ON ${CLICKHOUSE_DATABASE}.t_alias USING ${CLICKHOUSE_DATABASE}.t_alias.tup.val = '3' TO ALL;
USE ${CLICKHOUSE_DATABASE};
SELECT 'alias policy', count() FROM t_alias;
DROP ROW POLICY p_alias, p_alias_db ON t_alias;
EOF

# Nothing is checked against a table whose columns the creator cannot see, so nothing about them is disclosed.
$CLICKHOUSE_CLIENT <<EOF
DROP USER IF EXISTS ${U};
CREATE USER ${U};
GRANT CREATE ROW POLICY ON ${CLICKHOUSE_DATABASE}.t_src TO ${U};
GRANT CREATE ROW POLICY, SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.a_src TO ${U};
EXECUTE AS ${U} CREATE ROW POLICY p_u ON ${CLICKHOUSE_DATABASE}.t_src USING nosuchcol = 1;
DROP ROW POLICY p_u ON t_src;
EXECUTE AS ${U} CREATE ROW POLICY p_ua ON ${CLICKHOUSE_DATABASE}.a_src USING nosuchcol = 1;
DROP ROW POLICY p_ua ON a_src;

GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_src TO ${U};
EXECUTE AS ${U} CREATE ROW POLICY p_u ON ${CLICKHOUSE_DATABASE}.t_src USING nosuchcol = 1; -- { serverError UNKNOWN_IDENTIFIER }
EXECUTE AS ${U} CREATE ROW POLICY p_ua ON ${CLICKHOUSE_DATABASE}.a_src USING nosuchcol = 1; -- { serverError UNKNOWN_IDENTIFIER }

REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_src FROM ${U};
REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.a_src FROM ${U};
GRANT CREATE ROW POLICY ON ${CLICKHOUSE_DATABASE}.* TO ${U};
GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.t_x TO ${U};
EXECUTE AS ${U} CREATE ROW POLICY p_ud ON ${CLICKHOUSE_DATABASE}.* USING x = 1;
DROP ROW POLICY p_ud ON *;
EOF
$CLICKHOUSE_CLIENT -q "EXECUTE AS ${U} CREATE ROW POLICY p_ud ON ${CLICKHOUSE_DATABASE}.* USING val = ''" 2>&1 \
    | grep -m1 -o "against table ${CLICKHOUSE_DATABASE}.t_x. (UNKNOWN_IDENTIFIER)"
$CLICKHOUSE_CLIENT -q "DROP USER ${U}"

# Tables of a lazy_load_tables database that nothing has read yet. A filter over stored columns is checked
# against the declared columns without loading the table; any other filter is checked against the loaded table.
$CLICKHOUSE_CLIENT <<EOF
DROP DATABASE IF EXISTS ${DB_LAZY} SYNC;
CREATE DATABASE ${DB_LAZY} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB_LAZY}.l1 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB_LAZY}.l2 (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ${DB_LAZY}.l3 (id UInt64) ENGINE = MergeTree ORDER BY id;
DETACH DATABASE ${DB_LAZY} SYNC;
ATTACH DATABASE ${DB_LAZY};
CREATE FUNCTION ${FPART} AS () -> _part != '';

CREATE ROW POLICY p_lazy ON ${DB_LAZY}.* USING id >= 0 TO ALL;
DROP ROW POLICY p_lazy ON ${DB_LAZY}.*;
CREATE ROW POLICY p_lazy ON ${DB_LAZY}.* USING tuple(id) TO ALL; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT name, engine FROM system.tables WHERE database = '${DB_LAZY}' ORDER BY name;

CREATE ROW POLICY p_lazy ON ${DB_LAZY}.* USING x = 1 TO ALL; -- { serverError UNKNOWN_IDENTIFIER }

CREATE ROW POLICY p_lazy ON ${DB_LAZY}.l2 USING _part != '' TO ALL;
DROP ROW POLICY p_lazy ON ${DB_LAZY}.l2;
CREATE ROW POLICY p_lazy ON ${DB_LAZY}.l3 USING ${FPART}() TO ALL;
DROP ROW POLICY p_lazy ON ${DB_LAZY}.l3;
EOF

$CLICKHOUSE_CLIENT <<EOF
-- The filter must be usable as a filter.
CREATE ROW POLICY p_t ON t_src USING tuple(id) TO ALL; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }

-- SQL UDF calls are expanded before deciding whether the filter reads another table.
CREATE FUNCTION ${FIN} AS (a, s) -> a IN s;
CREATE ROW POLICY p_udf ON t_src USING ${FIN}(nosuch, (1, 2)) TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
CREATE ROW POLICY p_udf ON t_src USING ${FIN}(id, (1, 2)) TO ALL;
SELECT 'udf', count() FROM t_src;
DROP ROW POLICY p_udf ON t_src;
DROP FUNCTION ${FIN};
CREATE FUNCTION ${FDICT} AS (d, i) -> dictGet(d, 'val', i);
USE default;
CREATE ROW POLICY p_udf ON ${CLICKHOUSE_DATABASE}.t_src USING ${FDICT}('d_val', id) = '3' TO ALL;
USE ${CLICKHOUSE_DATABASE};
SELECT 'udf dictionary', count() FROM t_src;
DROP ROW POLICY p_udf ON t_src;
DROP FUNCTION ${FDICT};

-- A column on the right of IN belongs to the table.
CREATE ROW POLICY p_arr ON t_arr USING nosuch IN allowed TO ALL; -- { serverError UNKNOWN_IDENTIFIER }
CREATE ROW POLICY p_arr ON t_arr USING id IN allowed TO ALL;
SELECT 'arr', count() FROM t_arr;
DROP ROW POLICY p_arr ON t_arr;

-- A row filter cannot aggregate or use window functions.
CREATE ROW POLICY p_agg ON t_src USING sum(id) > 0 TO ALL; -- { serverError ILLEGAL_AGGREGATION }
CREATE ROW POLICY p_agg ON t_src USING row_number() OVER () > 0 TO ALL; -- { serverError ILLEGAL_AGGREGATION }
CREATE ROW POLICY p_agg ON t_src USING grouping(id) = 0 TO ALL; -- { serverError ILLEGAL_AGGREGATION }

SELECT 'left', count() FROM system.row_policies WHERE database IN (currentDatabase(), '${DB_LAZY}');
DROP DATABASE ${DB_LAZY} SYNC;
DROP FUNCTION ${FPART};
EOF
