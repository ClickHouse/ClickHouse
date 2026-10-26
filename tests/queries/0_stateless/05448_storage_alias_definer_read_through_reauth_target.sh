#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: a DDL on a replicated database runs with no user, so the creator-privilege
#                         check at alias creation is skipped and the deny path silently allows.

# An `Alias` runs with definer rights, so a target that performs its own authorization while reading -
# a `SQL SECURITY INVOKER` view building its inner query, a `Merge` enumerating its children - must run
# with the definer's identity, not the invoker's. A reader granted only SELECT on the alias must be
# able to read through it even though it cannot read the target's underlying tables directly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

reader="reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${reader};
    DROP TABLE IF EXISTS base;
    DROP TABLE IF EXISTS inv_view;
    DROP TABLE IF EXISTS alias_over_view;
    DROP TABLE IF EXISTS merge_part_1;
    DROP TABLE IF EXISTS merge_part_2;
    DROP TABLE IF EXISTS merge_all;
    DROP TABLE IF EXISTS alias_over_merge;

    CREATE TABLE base (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO base VALUES (1), (2), (3);
    CREATE VIEW inv_view DEFINER = default SQL SECURITY INVOKER AS SELECT x FROM base;
    CREATE TABLE alias_over_view ENGINE = Alias('inv_view');

    CREATE TABLE merge_part_1 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE merge_part_2 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO merge_part_1 VALUES (10);
    INSERT INTO merge_part_2 VALUES (20);
    CREATE TABLE merge_all (x UInt64) ENGINE = Merge(currentDatabase(), '^merge_part_[12]\$');
    CREATE TABLE alias_over_merge ENGINE = Alias('merge_all');

    CREATE USER ${reader} NOT IDENTIFIED;
    GRANT SELECT ON alias_over_view TO ${reader};
    GRANT SELECT ON alias_over_merge TO ${reader};
"

# The reader has SELECT on the aliases only, nothing on the views/Merge or their underlying tables.
echo "Read through the alias over a SQL SECURITY INVOKER view"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT sum(x) FROM alias_over_view;"

echo "Read through the alias over a Merge table"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT sum(x) FROM alias_over_merge;"

# The target's underlying tables stay inaccessible directly: the alias delegates, it does not grant.
echo "Reading the underlying base table directly is still denied"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT sum(x) FROM base;" 2>&1 | grep -o "ACCESS_DENIED" | uniq
echo "Reading the INVOKER view directly is still denied (view runs as the reader)"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT sum(x) FROM inv_view;" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE alias_over_view;
    DROP TABLE inv_view;
    DROP TABLE base;
    DROP TABLE alias_over_merge;
    DROP TABLE merge_all;
    DROP TABLE merge_part_1;
    DROP TABLE merge_part_2;
    DROP USER ${reader};
"
