#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An explicitly typed default using `dictGet` is not analyzed on RESTORE, so the dictionary need not exist.
# The same holds for a column with a column TTL, which resets it to its default in merges.

db=${CLICKHOUSE_DATABASE}
backup="Disk('backups', '05837_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} <<EOL
CREATE TABLE $db.src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.src VALUES (1, 10), (2, 20);
CREATE DICTIONARY $db.d (k UInt64, v UInt64) PRIMARY KEY k SOURCE(CLICKHOUSE(DB '$db' TABLE 'src')) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE $db.t (k UInt64, v UInt64 DEFAULT dictGet('$db.d', 'v', k)) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.t (k) VALUES (1), (2);
CREATE TABLE $db.t_ttl (k UInt64, ts DateTime, v UInt64 DEFAULT dictGet('$db.d', 'v', k) TTL ts + INTERVAL 1 DAY) ENGINE = MergeTree ORDER BY k;
INSERT INTO $db.t_ttl (k, ts) VALUES (1, now()), (2, now());
EOL

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE $db.t, TABLE $db.t_ttl TO $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_ttl SYNC"
${CLICKHOUSE_CLIENT} --query "DROP DICTIONARY $db.d SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.src SYNC"

${CLICKHOUSE_CLIENT} --send_logs_level=error --query "RESTORE TABLE $db.t, TABLE $db.t_ttl FROM $backup FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM $db.t ORDER BY k"
${CLICKHOUSE_CLIENT} --query "SELECT k FROM $db.t_ttl ORDER BY k"

${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $db.t_ttl SYNC"
