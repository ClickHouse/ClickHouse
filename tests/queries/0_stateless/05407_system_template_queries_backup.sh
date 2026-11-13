#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the catalog of template queries is global.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_DATABASE}/template_queries')"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_kept'"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_lost'"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.template_queries (name, category, description, query) VALUES ('05407_backup_kept', 'test', 'old', 'SELECT 1'), ('05407_backup_lost', 'test', 'Tab\there', 'SELECT 2\nFROM system.one')"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.template_queries TO ${backup_name} FORMAT Null"

# One row is lost and the other is replaced: the restore brings back the lost one and keeps the replaced one.
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_lost'"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_kept'"
${CLICKHOUSE_CLIENT} --query "INSERT INTO system.template_queries (name, category, description, query) VALUES ('05407_backup_kept', 'test', 'new', 'SELECT 3')"

# A restore adds rows, so it needs INSERT on the table.
user="u05407_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} --query "GRANT ALL ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON system.template_queries FROM ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "RESTORE TABLE system.template_queries FROM ${backup_name} FORMAT Null" 2>&1 | grep -o -m1 "INSERT ON system.template_queries"
${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.template_queries WHERE name = '05407_backup_lost'"

${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.template_queries FROM ${backup_name} FORMAT Null"

${CLICKHOUSE_CLIENT} --query "SELECT name, category, description, query, is_builtin FROM system.template_queries WHERE name LIKE '05407\\_backup\\_%' ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_kept'"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '05407_backup_lost'"
