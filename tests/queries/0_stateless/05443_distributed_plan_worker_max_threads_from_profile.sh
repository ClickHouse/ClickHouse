#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the stateless worker endpoint of the test configuration.

# `max_threads` from the user's settings profile describes the initiator, so worker tasks run with their own;
# a `max_threads` the user sets over the profile caps the worker tasks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_${CLICKHOUSE_DATABASE}"
profile="p_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS ${user};
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t SELECT number FROM numbers(1000000);
    CREATE SETTINGS PROFILE ${profile} SETTINGS max_threads = 3;
    CREATE USER ${user} SETTINGS PROFILE ${profile};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user};
"

settings="make_distributed_plan = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    enable_cascades_optimizer = 0, distributed_plan_fallback_to_local_execution = 0, distributed_plan_default_reader_bucket_count = 2,
    max_rows_to_group_by = 0"

# The test client passes random settings, `max_threads` among them; this client must not, or it would override the profile.
client="$CLICKHOUSE_CLIENT_BINARY --host ${CLICKHOUSE_HOST} --port ${CLICKHOUSE_PORT_TCP} --database ${CLICKHOUSE_DATABASE} --user ${user}"

$client --query "SELECT sum(k) FROM t SETTINGS ${settings}, log_comment = '05443_profile'"
$client --query "SELECT sum(k) FROM t SETTINGS ${settings}, max_threads = 2, log_comment = '05443_user_cap'"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT q.log_comment, count() > 0, groupUniqArray(f.Settings['max_threads'])
    FROM
    (
        SELECT log_comment, argMax(query_id, event_time_microseconds) AS initiator_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND query_id = initial_query_id AND log_comment IN ('05443_profile', '05443_user_cap')
        GROUP BY log_comment
    ) AS q
    INNER JOIN system.query_log AS f ON f.initial_query_id = q.initiator_id
    WHERE f.event_date >= yesterday() AND f.type = 'QueryFinish' AND f.query_id != f.initial_query_id
    GROUP BY q.log_comment
    ORDER BY q.log_comment;
    DROP USER ${user};
    DROP SETTINGS PROFILE ${profile};
"
