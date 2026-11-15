#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: the catalog of template queries is global, and so is the failpoint.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

prefix="05407_sync_${CLICKHOUSE_DATABASE}"

# A deletion whose directory sync fails after the file is replaced is still logged.
${CLICKHOUSE_CLIENT} --query "INSERT INTO system.template_queries (name, category, description, query) VALUES ('${prefix}', 'test', '', 'SELECT 1')"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT local_directory_sync_fail"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.template_queries WHERE name = '${prefix}'" 2>&1 | grep -o -m1 "CANNOT_FSYNC"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT local_directory_sync_fail"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.template_queries WHERE name = '${prefix}'"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND message LIKE 'Deleted template query ''${prefix}''%'"
