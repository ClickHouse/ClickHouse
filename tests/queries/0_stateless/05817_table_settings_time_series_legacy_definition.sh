#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: `ATTACH TABLE` with an explicit UUID is not allowed there.
#
# A `TimeSeries` table whose stored definition predates the `version` setting is converted when it is attached: it gets
# `version = 0`, and `recent_samples_ttl_seconds = 0` without a recent samples target. `system.table_settings` shows the
# settings of the converted definition, which the table uses, not the defaults for a new table. A full `ATTACH TABLE`
# stores the definition as it is given, as an older server stored it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

uuid=$(${CLICKHOUSE_CLIENT} --query "SELECT generateUUIDv4()")

# A full `ATTACH TABLE` logs a warning, which would pollute stderr.
${CLICKHOUSE_CLIENT} --query "
    SET send_logs_level = 'fatal';
    ATTACH TABLE ts_legacy UUID '${uuid}' (metric_name String) ENGINE = TimeSeries"

echo "--- the stored definition states neither setting"
${CLICKHOUSE_CLIENT} --query "
    SELECT position(create_table_query, 'version') = 0, position(create_table_query, 'recent_samples_ttl_seconds') = 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_legacy'"

echo "--- the settings the table uses"
${CLICKHOUSE_CLIENT} --query "
    SELECT name, value, changed, inherited FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'ts_legacy' AND name IN ('version', 'recent_samples_ttl_seconds')
    ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ts_legacy"
