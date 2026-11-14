#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: authentication methods with GRANTS are not supported there
# Tag no-replicated-database: users, roles and row policies are not a part of the replicated database

# A `SQL SECURITY DEFINER` view that can hide rows is sealed, except for a query that runs with the identity
# the view's own query runs with: the definer, with its default roles and no limit from the authentication method.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Disable force_primary_key_reverse_order: The test checks that the view is read in order
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --force_primary_key_reverse_order=0"

db=${CLICKHOUSE_DATABASE}
reader="reader_${db}_$RANDOM"
definer="definer_${db}_$RANDOM"
role="role_${db}_$RANDOM"
full_password="full_$RANDOM"
token_password="token_$RANDOM"
run=$RANDOM

pin="--enable_parallel_replicas 0 --use_query_condition_cache 0 --optimize_read_in_order 1 --max_threads 1 --max_block_size 128"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP ROW POLICY IF EXISTS policy_${db} ON $db.v_policy; DROP VIEW IF EXISTS $db.vd;
        DROP USER IF EXISTS $reader, $definer; DROP ROLE IF EXISTS $role"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} <<EOF
CREATE USER $reader;
CREATE USER $definer IDENTIFIED WITH plaintext_password BY '$full_password',
    plaintext_password BY '$token_password' VALID UNTIL '2077-01-01' GRANTS (SELECT ON $db.vd, SELECT ON $db.dist_vd);
CREATE ROLE $role;
GRANT $role TO $definer;

CREATE TABLE $db.t (k String, ts DateTime, v String) ENGINE = MergeTree ORDER BY (k, ts)
    SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO $db.t SELECT if(number < 20000, 'a', 'b'), toDateTime(1000000 + number % 20000), if(number < 20000, 'x', if(number % 2 = 0, 'z', 'y'))
    FROM numbers(21000);

-- The second branch filters by a column that is not in the key, so it hides rows.
CREATE VIEW $db.v DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT k, ts, v FROM $db.t WHERE k = 'a' UNION ALL SELECT k, ts, v FROM $db.t WHERE k = 'b' AND v = 'z';
CREATE VIEW $db.v_policy DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, ts, v FROM $db.t WHERE k = 'a';
CREATE ROW POLICY policy_${db} ON $db.v_policy USING k = 'a' TO CURRENT_USER;
CREATE TABLE $db.dist_t AS $db.t ENGINE = Distributed(test_shard_localhost, $db, t);
CREATE VIEW $db.v_dist DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, ts, v FROM $db.dist_t WHERE k = 'a';

CREATE TABLE $db.secrets (owner String, secret String) ENGINE = MergeTree ORDER BY secret SETTINGS index_granularity = 1;
INSERT INTO $db.secrets VALUES ('alice', 'visible'), ('bob', 'HIDDEN');
CREATE VIEW $db.vs DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets WHERE owner = 'alice';
-- The shard of test_shard_localhost runs the query as the default user, the definer of the view.
CREATE TABLE $db.dist_vs AS $db.secrets ENGINE = Distributed(test_shard_localhost, $db, vs);
CREATE VIEW $db.vd DEFINER = $definer SQL SECURITY DEFINER AS SELECT owner, secret FROM $db.secrets WHERE owner = 'alice';
-- With the interserver secret, the shards run the query as the initial user, without the limits of its credential.
CREATE TABLE $db.dist_vd AS $db.secrets ENGINE = Distributed(test_cluster_interserver_secret, $db, vd);

GRANT SELECT ON $db.v TO $reader;
GRANT SELECT ON $db.dist_vs TO $reader;
-- With serialize_query_plan = 1 the initiator builds the plan of the shard, which reads the view.
GRANT SELECT ON $db.vs TO $reader;
GRANT SELECT ON $db.secrets TO $definer;
GRANT SELECT ON $db.vd TO $definer;
GRANT SELECT ON $db.dist_vd TO $definer;
EOF

query="WITH w AS (SELECT * FROM $db.v) SELECT k, toUInt32(ts), v FROM w ORDER BY ts DESC LIMIT 5"

echo "--- the definer and another user get the same rows"
${CLICKHOUSE_CLIENT} $pin --query "$query"
${CLICKHOUSE_CLIENT} $pin --user "$reader" --query "$query"

echo "--- the definer reads the view in order, another user reads it sealed"
${CLICKHOUSE_CLIENT} $pin --query "
    SELECT countIf(explain ILIKE '%ReadFromSealedView%'), countIf(explain ILIKE '%InReverseOrder%') > 0 FROM (EXPLAIN actions = 1 $query)"
${CLICKHOUSE_CLIENT} $pin --user "$reader" --query "EXPLAIN $query" | grep -c ReadFromSealedView

echo "--- so only the definer reads just the end of the table"
${CLICKHOUSE_CLIENT} $pin --log_comment "05616_definer_${db}_$run" --query "$query" > /dev/null
${CLICKHOUSE_CLIENT} $pin --user "$reader" --log_comment "05616_reader_${db}_$run" --query "$query" > /dev/null
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT if(log_comment LIKE '05616_definer_%', 'definer', 'reader'), read_rows < 5000 FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday()
      AND log_comment IN ('05616_definer_${db}_$run', '05616_reader_${db}_$run')
    ORDER BY log_comment"

echo "--- a secondary query of the definer reads the view sealed"
# Read transparently, the view would skip data by the outer predicate on the key, so the amount read would depend on the hidden row.
for serialize in 0 1; do
    for secret in HIDDEN zzz; do
        ${CLICKHOUSE_CLIENT} $pin --user "$reader" --serialize_query_plan "$serialize" --prefer_localhost_replica 0 \
            --log_comment "05616_dist_${db}_${run}_$serialize" --query "SELECT count() FROM $db.dist_vs WHERE secret = '$secret'"
    done
done
${CLICKHOUSE_CLIENT} --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT min(read_rows) = max(read_rows) FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday() AND is_initial_query
      AND log_comment LIKE '05616_dist_${db}_${run}_%'
    GROUP BY log_comment ORDER BY log_comment"

echo "--- a row policy on the view keeps it sealed for the definer"
${CLICKHOUSE_CLIENT} $pin --query "
    SELECT countIf(explain ILIKE '%ReadFromSealedView%') FROM (EXPLAIN WITH w AS (SELECT * FROM $db.v_policy) SELECT * FROM w ORDER BY ts DESC LIMIT 5)"

echo "--- the outer ORDER BY ... LIMIT is pushed into a view over a Distributed table read by its definer"
${CLICKHOUSE_CLIENT} $pin --prefer_localhost_replica 1 --exact_rows_before_limit 0 --extremes 0 --prefer_column_name_to_alias 0 --query "
    SELECT countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%') FROM (EXPLAIN SELECT * FROM $db.v_dist ORDER BY ts DESC LIMIT 5)"

echo "--- a session of the definer limited by the GRANTS of its credential reads the view sealed"
function login()
{
    local password=$1
    shift
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${definer}&password=${password}" -d "$@"
}
explain_full=$(login "$full_password" "EXPLAIN SELECT * FROM $db.vd")
explain_token=$(login "$token_password" "EXPLAIN SELECT * FROM $db.vd")
echo "full password: sealed $(grep -c ReadFromSealedView <<< "$explain_full")"
echo "token: sealed $(grep -c ReadFromSealedView <<< "$explain_token"), plan hidden $(grep -c 'plan hidden' <<< "$explain_token")"
login "$token_password" "SELECT count() FROM $db.secrets" 2>&1 | grep -o -m1 ACCESS_DENIED
for table in vd dist_vd; do
    for secret in HIDDEN zzz; do
        login "$token_password" "SELECT count() FROM $db.$table WHERE secret = '$secret' SETTINGS use_query_condition_cache = 0,
            enable_parallel_replicas = 0, prefer_localhost_replica = 0, log_comment = '05616_token_${db}_${run}_$table'"
    done
done
# The HTTP interface logs a query after it sends the response.
for _ in $(seq 1 60); do
    logged=$(${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT count() FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday() AND is_initial_query
          AND log_comment LIKE '05616_token_${db}_${run}_%'")
    [ "$logged" -ge 4 ] && break
    sleep 0.5
done
${CLICKHOUSE_CLIENT} --query "
    SELECT if(log_comment LIKE '%_dist_vd', 'through a Distributed table', 'directly'), min(read_rows) = max(read_rows) FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND event_date >= yesterday() AND is_initial_query
      AND log_comment LIKE '05616_token_${db}_${run}_%'
    GROUP BY log_comment ORDER BY log_comment DESC"

echo "--- a session of the definer with other roles reads the view sealed"
${CLICKHOUSE_CLIENT} --user "$definer" --password "$full_password" --query "SET ROLE NONE; EXPLAIN SELECT * FROM $db.vd" | grep -c ReadFromSealedView

echo "--- analyzer_inline_views does not inline the view, which is still read in order"
${CLICKHOUSE_CLIENT} $pin --analyzer_inline_views 1 --query "
    SELECT countIf(explain ILIKE '%ReadFromSealedView%'), countIf(explain ILIKE '%InReverseOrder%') > 0 FROM (EXPLAIN actions = 1 $query)"
