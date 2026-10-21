#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag no-replicated-database: in a `Replicated` database the `ALTER` is executed by the DDL worker, which does not run as the initiating user, so no author is recorded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mixed `MODIFY COMMENT` + `UPDATE` on a `ReplicatedMergeTree` table from a user whose name is too
# long to be recorded as the mutation author must be rejected before the local comment change is
# committed, so that neither the comment nor the mutation is applied.
# User names are global, so they are suffixed with the test database to let the test run in parallel
# with itself.
LONG_USER="$(printf 'u%.0s' {1..250})_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_replicated_mutation_author_long_user_name;
DROP USER IF EXISTS \`$LONG_USER\`;

CREATE TABLE t_replicated_mutation_author_long_user_name (id UInt64, value String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated_mutation_author_long_user_name', 'r1') ORDER BY id
COMMENT 'old'
SETTINGS persist_mutation_author = 1;
INSERT INTO t_replicated_mutation_author_long_user_name VALUES (1, 'a');

CREATE USER \`$LONG_USER\` IDENTIFIED WITH no_password;
GRANT ALTER ON *.* TO \`$LONG_USER\`;
"

$CLICKHOUSE_CLIENT --query "
EXECUTE AS \`$LONG_USER\` ALTER TABLE t_replicated_mutation_author_long_user_name MODIFY COMMENT 'new', UPDATE value = 'b' WHERE id = 1 SETTINGS mutations_sync = 1;
" 2>&1 | grep -oF 'TOO_LARGE_STRING_SIZE' | head -n 1

$CLICKHOUSE_CLIENT --multiquery "
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 't_replicated_mutation_author_long_user_name';
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_replicated_mutation_author_long_user_name';
SELECT value FROM t_replicated_mutation_author_long_user_name;

DROP TABLE t_replicated_mutation_author_long_user_name;
DROP USER \`$LONG_USER\`;
"
