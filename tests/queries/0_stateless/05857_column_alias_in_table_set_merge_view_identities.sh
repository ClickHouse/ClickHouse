#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With parallel replicas a part may be read by a replica, which runs the query as the user of the
# cluster (the test cluster has no secret), so the set would not be built on behalf of the definers.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_parallel_replicas=0"

# A `Merge` read shares the sets of the `ALIAS` columns `p IN table` that it computes for its children. It computes
# them only for the children that are tables, with its own context, so these sets have the same access and settings.
# A child that is a view computes the `ALIAS` columns in its own plan, with its own sets: with the settings of the
# view and on behalf of its `SQL SECURITY DEFINER` user, and a nested `Merge` read under such a view does not share
# the sets of the outer one. So the children with other settings or access never use a set built for another child.

definer_all="definer_all_${CLICKHOUSE_DATABASE}"
definer_two="definer_two_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${definer_all}, ${definer_two};

    CREATE TABLE keys_alias_identities (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO keys_alias_identities VALUES (NULL), (1);
    INSERT INTO keys_alias_identities VALUES (2);

    CREATE TABLE t_alias_identities_1 (p Nullable(UInt64), f Nullable(UInt8) ALIAS p IN keys_alias_identities) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE t_alias_identities_2 (p Nullable(UInt64), f Nullable(UInt8) ALIAS p IN keys_alias_identities) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_alias_identities_1 VALUES (NULL), (1), (2), (5);
    INSERT INTO t_alias_identities_2 VALUES (NULL), (1), (2), (5);

    CREATE TABLE m_alias_identities_1 ENGINE = Merge(currentDatabase(), '^t_alias_identities_1\$');
    CREATE TABLE m_alias_identities_2 ENGINE = Merge(currentDatabase(), '^t_alias_identities_2\$');

    CREATE VIEW v_alias_null_0 AS SELECT p, f FROM t_alias_identities_1 SETTINGS transform_null_in = 0;
    CREATE VIEW v_alias_null_1 AS SELECT p, f FROM t_alias_identities_2 SETTINGS transform_null_in = 1;
    CREATE VIEW v_alias_nested_null_0 AS SELECT p, f FROM m_alias_identities_1 SETTINGS transform_null_in = 0;
    CREATE VIEW v_alias_nested_null_1 AS SELECT p, f FROM m_alias_identities_2 SETTINGS transform_null_in = 1;

    CREATE VIEW v_alias_limit_0 AS SELECT p, f FROM t_alias_identities_1;
    CREATE VIEW v_alias_limit_1 AS SELECT p, f FROM t_alias_identities_2
        SETTINGS max_rows_in_set = 1, set_overflow_mode = 'break', max_block_size = 1, max_threads = 1;

    CREATE USER ${definer_all} IDENTIFIED WITH no_password;
    CREATE USER ${definer_two} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_alias_identities_1 TO ${definer_all};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_alias_identities_1 TO ${definer_all};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.keys_alias_identities TO ${definer_all};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_alias_identities_2 TO ${definer_two};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m_alias_identities_2 TO ${definer_two};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.keys_alias_identities TO ${definer_two};
    CREATE ROW POLICY ${policy} ON keys_alias_identities USING k = 2 TO ${definer_two};
    -- Other users keep seeing every row even if the server is configured with
    -- \`users_without_row_policies_can_read_rows = false\`.
    CREATE ROW POLICY ${policy}_others ON keys_alias_identities USING 1 TO ALL EXCEPT ${definer_two};

    CREATE VIEW v_alias_definers_1 DEFINER = ${definer_all} SQL SECURITY DEFINER AS SELECT p, f FROM t_alias_identities_1;
    CREATE VIEW v_alias_definers_2 DEFINER = ${definer_two} SQL SECURITY DEFINER AS SELECT p, f FROM t_alias_identities_2;
    CREATE VIEW v_alias_nested_definers_1 DEFINER = ${definer_all} SQL SECURITY DEFINER AS SELECT p, f FROM m_alias_identities_1;
    CREATE VIEW v_alias_nested_definers_2 DEFINER = ${definer_two} SQL SECURITY DEFINER AS SELECT p, f FROM m_alias_identities_2;

    SELECT 'tables';
    SELECT _table, p, f FROM merge(currentDatabase(), '^t_alias_identities_[12]\$') ORDER BY _table, p;

    SELECT 'views with transform_null_in';
    SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_null_[01]\$') ORDER BY _table, p;
    SELECT _table, p FROM merge(currentDatabase(), '^v_alias_null_[01]\$') WHERE f ORDER BY _table, p;

    SELECT 'views over Merge with transform_null_in';
    SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_nested_null_[01]\$') ORDER BY _table, p;

    SELECT 'views with max_rows_in_set';
    SELECT _table, sum(f) FROM merge(currentDatabase(), '^v_alias_limit_[01]\$') GROUP BY _table ORDER BY _table SETTINGS max_threads = 1;

    SELECT 'views with definers';
    SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_definers_[12]\$') ORDER BY _table, p;
    SELECT _table, p FROM merge(currentDatabase(), '^v_alias_definers_[12]\$') WHERE f ORDER BY _table, p;

    SELECT 'views over Merge with definers';
    SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_nested_definers_[12]\$') ORDER BY _table, p;
"

echo 'after revoking the right-hand table from one definer'
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.keys_alias_identities FROM ${definer_two}"
$CLICKHOUSE_CLIENT -q "SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_definers_[12]\$') ORDER BY _table, p" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "SELECT _table, p, f FROM merge(currentDatabase(), '^v_alias_nested_definers_[12]\$') ORDER BY _table, p" 2>&1 | grep -o -m1 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT -m -q "
    DROP VIEW v_alias_definers_1 SYNC;
    DROP VIEW v_alias_definers_2 SYNC;
    DROP VIEW v_alias_nested_definers_1 SYNC;
    DROP VIEW v_alias_nested_definers_2 SYNC;
    DROP ROW POLICY ${policy} ON keys_alias_identities;
    DROP ROW POLICY ${policy}_others ON keys_alias_identities;
    DROP USER ${definer_all}, ${definer_two};
"
