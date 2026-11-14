#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With parallel replicas a part may be read by a replica, which runs the query as the user of the
# cluster (the test cluster has no secret), so the set would not be built on behalf of the definers.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_parallel_replicas=0"

# The children of a `Merge` read share the sets of the defaults `p IN table` of the columns missing in their
# parts, but the children may read on behalf of different users: here, views with different `SQL SECURITY DEFINER`.
# A set built for one definer must not be reused for another one, whose privileges and row policies differ.

definer_all="definer_all_${CLICKHOUSE_DATABASE}"
definer_two="definer_two_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${definer_all}, ${definer_two};
    DROP TABLE IF EXISTS keys_definers;
    DROP TABLE IF EXISTS t_definers_1;
    DROP TABLE IF EXISTS t_definers_2;

    CREATE TABLE keys_definers (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO keys_definers VALUES (1), (2);

    CREATE TABLE t_definers_1 (p UInt8) ENGINE = MergeTree ORDER BY p;
    CREATE TABLE t_definers_2 (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_definers_1 VALUES (1), (2);
    INSERT INTO t_definers_2 VALUES (1), (2);
    ALTER TABLE t_definers_1 ADD COLUMN f UInt8 DEFAULT p IN keys_definers;
    ALTER TABLE t_definers_2 ADD COLUMN f UInt8 DEFAULT p IN keys_definers;

    CREATE USER ${definer_all} IDENTIFIED WITH no_password;
    CREATE USER ${definer_two} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_definers_1 TO ${definer_all};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.keys_definers TO ${definer_all};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_definers_2 TO ${definer_two};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.keys_definers TO ${definer_two};
    CREATE ROW POLICY ${policy} ON keys_definers USING p = 2 TO ${definer_two};
    -- Other users keep seeing every row even if the server is configured with
    -- \`users_without_row_policies_can_read_rows = false\`.
    CREATE ROW POLICY ${policy}_others ON keys_definers USING 1 TO ALL EXCEPT ${definer_two};

    CREATE VIEW v_definers_1 DEFINER = ${definer_all} SQL SECURITY DEFINER AS SELECT p, f FROM t_definers_1;
    CREATE VIEW v_definers_2 DEFINER = ${definer_two} SQL SECURITY DEFINER AS SELECT p, f FROM t_definers_2;
"

echo 'direct'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM v_definers_1 ORDER BY p"
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM v_definers_2 ORDER BY p"

echo 'merge'
$CLICKHOUSE_CLIENT -q "SELECT _table, p, f FROM merge(currentDatabase(), '^v_definers_') ORDER BY _table, p"

echo 'merge after revoking the right-hand table from one definer'
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.keys_definers FROM ${definer_two}"
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM v_definers_2 ORDER BY p" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "SELECT _table, p, f FROM merge(currentDatabase(), '^v_definers_') ORDER BY _table, p" 2>&1 | grep -o -m1 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT -m -q "
    DROP VIEW v_definers_1 SYNC;
    DROP VIEW v_definers_2 SYNC;
    DROP ROW POLICY ${policy} ON keys_definers;
    DROP ROW POLICY ${policy}_others ON keys_definers;
    DROP USER ${definer_all}, ${definer_two};
    DROP TABLE t_definers_1;
    DROP TABLE t_definers_2;
    DROP TABLE keys_definers;
"
