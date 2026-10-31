#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: a DDL on a replicated database runs with no user, so the creator-privilege
#                         check asserted here is skipped and the deny path silently allows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

definer="definer_${CLICKHOUSE_TEST_UNIQUE_NAME}"
reader="reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${definer};
    DROP USER IF EXISTS ${reader};
    DROP TABLE IF EXISTS def_target;
    DROP TABLE IF EXISTS def_alias;

    CREATE TABLE def_target (id UInt32, value String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO def_target VALUES (1, 'one'), (2, 'two');

    CREATE USER ${definer} NOT IDENTIFIED;
    GRANT CREATE TABLE ON def_alias TO ${definer};
    GRANT TABLE ENGINE ON Alias TO ${definer};
"

# Creation gate: the creator (the definer) must hold the whole forwardable privilege set on the
# target, so a partial grant is not enough.
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON def_target TO ${definer};"
echo "Create with only SELECT on target"
${CLICKHOUSE_CLIENT} --user="${definer}" --query "CREATE TABLE def_alias ENGINE = Alias('def_target');" 2>&1 | grep -o "ACCESS_DENIED" | uniq

${CLICKHOUSE_CLIENT} --query "GRANT INSERT, ALTER, TRUNCATE, OPTIMIZE, CHECK ON def_target TO ${definer};"
echo "Create with the full forwardable privilege set on target"
${CLICKHOUSE_CLIENT} --user="${definer}" --query "CREATE TABLE def_alias ENGINE = Alias('def_target');" 2>&1 | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'def_alias';"

echo "SHOW CREATE carries the SQL SECURITY DEFINER clause"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE def_alias" | grep -o "SQL SECURITY DEFINER"

# A reader granted only on the alias (nothing on the target) operates with the definer's rights.
${CLICKHOUSE_CLIENT} -m --query "
    CREATE USER ${reader} NOT IDENTIFIED;
    GRANT SELECT, SHOW COLUMNS ON def_alias TO ${reader};
"
echo "Reader DESCRIBEs the alias with no target grant"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "DESCRIBE TABLE def_alias" | cut -f1 | tr '\n' ' '
echo ""
echo "Reader sees the alias columns in system.columns with no target grant"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'def_alias';"
echo "Reader SELECTs through the alias with no target grant"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM def_alias;"

# The trivial-count shortcut (SELECT count()) returns a row count without going through the read path,
# so it must authorize the definer exactly as a read does - whether or not the optimization is enabled.
echo "Trivial count while the definer is authorized (optimization on, then off)"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM def_alias SETTINGS optimize_trivial_count_query = 1;"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM def_alias SETTINGS optimize_trivial_count_query = 0;"

${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON def_target FROM ${definer};"
echo "After REVOKE SELECT on the target from the definer, count is denied (optimization on)"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM def_alias SETTINGS optimize_trivial_count_query = 1;" 2>&1 | grep -o "ACCESS_DENIED" | uniq
echo "...and with the optimization off"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT count() FROM def_alias SETTINGS optimize_trivial_count_query = 0;" 2>&1 | grep -o "ACCESS_DENIED" | uniq
echo "...and a column read is denied the same way"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT id FROM def_alias;" 2>&1 | grep -o "ACCESS_DENIED" | uniq
echo "...and total_rows is hidden in system.tables"
${CLICKHOUSE_CLIENT} --user="${reader}" --query "SELECT total_rows IS NULL FROM system.tables WHERE database = currentDatabase() AND name = 'def_alias';"

${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE def_alias;
    DROP TABLE def_target;
    DROP USER ${definer};
    DROP USER ${reader};
"
