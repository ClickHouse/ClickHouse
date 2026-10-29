#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `fileCluster` and WASM UDFs are not in the fast test build.
# no-msan: WASM UDFs are not supported under MSan.
# A deterministic user-defined function exists only on the server where it was created
# (`EXECUTABLE` UDFs are configured per server, WASM UDFs are created per server), so a
# conjunct that calls it must stay on the initiator instead of being copied into the
# `WHERE` of the `IStorageCluster` JOIN wrap that is sent to the cluster workers.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE="identity_${CLICKHOUSE_DATABASE}"
FUNCTION="wasm_wrap_udf_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} << SQL
CREATE OR REPLACE FUNCTION ${FUNCTION}
    LANGUAGE WASM FROM '${MODULE}' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

INSERT INTO FUNCTION file(currentDatabase() || '_05449_wrap_left.tsv', 'TSV', 'n Int32')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05449_right;
CREATE TABLE t_05449_right
(
    n Int32
)
ENGINE = Memory;
INSERT INTO t_05449_right VALUES (0), (1);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05449_wrap_left.tsv',
    'TSV',
    'n Int32') AS l
INNER JOIN t_05449_right AS r ON l.n = r.n
WHERE ${FUNCTION}(l.n) < 2 AND l.n >= 0
SETTINGS enable_analyzer = 1,
    query_plan_filter_push_down = 1,
    query_plan_join_swap_table = 0,
    enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0,
    log_comment = '05449_${CLICKHOUSE_DATABASE}';

SYSTEM FLUSH LOGS query_log;

-- The workers received queries, and none of them refers to the UDF.
SELECT count() > 0, countIf(position(query, '${FUNCTION}') > 0)
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment = '05449_${CLICKHOUSE_DATABASE}';

DROP TABLE t_05449_right;
DROP FUNCTION ${FUNCTION};
SQL

${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE}'"
