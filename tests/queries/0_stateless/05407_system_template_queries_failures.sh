#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: the catalog of template queries is global, and so is the failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

prefix="05407_failures_${CLICKHOUSE_DATABASE}"
backup_dir="${CLICKHOUSE_DATABASE}/template_queries_failures"
backup_name="Disk('backups', '${backup_dir}')"

${CLICKHOUSE_CLIENT} --query "INSERT INTO system.template_queries (name, category, description, query) VALUES ('${prefix}_seed', 'test', '', 'SELECT 1')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.template_queries TO ${backup_name} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '${prefix}_seed'"

disk_path=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name = 'backups'")
backup_file=$(find "${disk_path}${backup_dir}" -name template_queries.tsv)

# A restore stops at the row limit instead of decoding the rest of the file.
awk -v p="${prefix}" 'BEGIN { for (i = 0; i <= 10000; ++i) printf "%s_%d\ttest\t\tSELECT 1\t0\n", p, i; print "malformed" }' > "${backup_file}"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.template_queries FROM ${backup_name} FORMAT Null" 2>&1 | grep -o -m1 "Too many user template queries"

# An oversized field is refused while it is read.
awk -v p="${prefix}" 'BEGIN { q = "x"; for (i = 0; i < 23; ++i) q = q q; printf "%s_big\ttest\t\t%s\t0\n", p, q }' > "${backup_file}"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.template_queries FROM ${backup_name} FORMAT Null" 2>&1 | grep -o -m1 "A template query field is longer than the maximum of 1048576 bytes"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.template_queries WHERE startsWith(name, '${prefix}')"

# A deletion whose directory sync fails after the file is replaced is still logged.
${CLICKHOUSE_CLIENT} --query "INSERT INTO system.template_queries (name, category, description, query) VALUES ('${prefix}_sync', 'test', '', 'SELECT 1')"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT local_directory_sync_fail"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '${prefix}_sync'" 2>&1 | grep -o -m1 "CANNOT_FSYNC"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT local_directory_sync_fail"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.template_queries WHERE name = '${prefix}_sync'"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND message LIKE 'Deleted template query ''${prefix}_sync''%'"
