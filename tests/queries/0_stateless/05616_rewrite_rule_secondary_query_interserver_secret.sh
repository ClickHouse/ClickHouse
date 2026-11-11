#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: rewrite rules are global server state
# no-fasttest: interserver mode requires SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rewrite rules are applied once, on the initiator. The initiator sends an explicitly empty
# `query_rules` with a secondary query, but on the shard settings constraints are enforced on
# secondary queries too, so a `query_rules` value fixed by a `CONST` constraint survives the
# override. On a connection authenticated with the cluster secret the shard can trust the query
# kind, and it must clear `query_rules` for the secondary query regardless of the constraint.
# With the secret, the shard runs the fragment as the initial user, so the constrained profile of
# that user applies on the shard as well.

RULE="rule_interserver_secret_${CLICKHOUSE_DATABASE}"
USER="user_interserver_secret_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>/dev/null
$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"

# A rule that exists but matches neither the user's query nor the shard fragment.
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT 'never matches this query') REJECT WITH 'rejected'"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS query_rules = '${RULE}' CONST"
$CLICKHOUSE_CLIENT --query "GRANT REMOTE ON *.* TO ${USER}"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.one TO ${USER}"

LOG_COMMENT="05616_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT --user "${USER}" --log_comment "${LOG_COMMENT}" --prefer_localhost_replica 0 \
    --query "SELECT count() FROM cluster('test_cluster_interserver_secret', system.one)"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

echo "initial query carries query_rules:"
$CLICKHOUSE_CLIENT --query "
    SELECT Settings['query_rules'] = '${RULE}'
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND log_comment = '${LOG_COMMENT}'
        AND type = 'QueryFinish'
        AND is_initial_query = 1
        AND event_date >= yesterday()"

echo "secondary fragments / fragments carrying query_rules:"
$CLICKHOUSE_CLIENT --query "
    WITH initial_query AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '${LOG_COMMENT}'
            AND type = 'QueryFinish'
            AND is_initial_query = 1
            AND event_date >= yesterday()
    )
    SELECT
        count() AS secondary_fragments,
        countIf(Settings['query_rules'] != '') AS fragments_with_rules
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial_query)
        AND is_initial_query = 0
        AND type = 'QueryFinish'
        AND event_date >= yesterday()"

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
