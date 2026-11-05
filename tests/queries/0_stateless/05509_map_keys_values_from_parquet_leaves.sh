#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-parallel-replicas: parallel replicas read `file()` and Iceberg through -Cluster storages, which keep the whole-map read.
# Map functions that need only the keys or only the values of a map (`has`, `mapKeys`, `mapContainsValue`, ...) read only
# that side of the map from Parquet on `file()` and data lake tables. The results must not change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TYPES="${CLICKHOUSE_DATABASE}_05509_types.parquet"
BYTES="${CLICKHOUSE_DATABASE}_05509_bytes.parquet"
WITH_M="${CLICKHOUSE_DATABASE}_05509_d_with_m.parquet"
WITHOUT_M="${CLICKHOUSE_DATABASE}_05509_d_without_m.parquet"
COLLISION="${CLICKHOUSE_DATABASE}_05509_collision.parquet"
MISSING="${CLICKHOUSE_DATABASE}_05509_missing.parquet"
ICEBERG_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05509_evolved"
ICEBERG_MIXED_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05509_mixed"
ICEBERG_TYPES_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05509_types"
ICEBERG_BYTES_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05509_bytes"
rm -rf "${ICEBERG_PATH}" "${ICEBERG_MIXED_PATH}" "${ICEBERG_TYPES_PATH}" "${ICEBERG_BYTES_PATH}"

${CLICKHOUSE_CLIENT} -m --query "
INSERT INTO FUNCTION file('${TYPES}')
SELECT number AS id,
    if(number % 7 = 0, map(), map('k' || toString(number % 3), toString(number), 'c', 'v')) :: Map(String, String) AS ms,
    map('k' || toString(number % 3), 'v' || toString(number % 2)) :: Map(LowCardinality(String), LowCardinality(String)) AS mlc,
    map('k', if(number % 4 = 0, NULL, toInt32(number))) :: Map(String, Nullable(Int32)) AS mn,
    map('k', range(number % 3)) :: Map(String, Array(UInt8)) AS ma,
    map('k', map('i', toUInt8(number % 2))) :: Map(String, Map(String, UInt8)) AS mm,
    map('k', tuple(toInt8(number % 2))) :: Map(String, Tuple(a Int8)) AS mt,
    tuple(map('x' || toString(number % 2), toUInt8(number)), toInt8(number % 3)) :: Tuple(x Map(String, UInt8), y Int8) AS t
FROM numbers(30)
SETTINGS output_format_parquet_row_group_size = 7, engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('${BYTES}')
SELECT number AS id,
    map('k' || toString(number % 5), repeat(toString(number % 10), 200)) AS m,
    map(repeat(toString(number % 10), 200), toUInt8(number % 5)) AS mv
FROM numbers(1000)
SETTINGS output_format_parquet_row_group_size = 100, output_format_parquet_compression_method = 'zstd', engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('${WITH_M}') SELECT number AS id, map('a', toUInt8(number), 'b', 7) AS m FROM numbers(5)
SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('${WITHOUT_M}') SELECT number + 100 AS id FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('${COLLISION}') SELECT map('key', toUInt64(1)) AS m, ['other'] AS \`M.keys\`, [toUInt64(4)] AS \`M.values\`
SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file('${MISSING}') SELECT map('k', tuple(toUInt8(1))) :: Map(String, Tuple(b UInt8)) AS mv,
    map(tuple(toUInt8(1)), 'v') :: Map(Tuple(b UInt8), String) AS mk, toUInt64(7) AS id
SETTINGS engine_file_truncate_on_insert = 1;
"

# Every query below, with the rewrite off and on.
QUERIES="
SELECT 'mapKeys, mapValues';
SELECT id, mapKeys(ms), mapValues(mlc), mapKeys(mn), mapValues(mn), mapValues(ma), mapValues(mm), mapValues(mt), mapKeys(t.x) FROM file('${TYPES}') ORDER BY id;
SELECT 'one side, the map alone';
SELECT count() FROM file('${TYPES}') WHERE has(ms, 'k1');
SELECT count() FROM file('${TYPES}') WHERE notHas(ms, 'k1');
SELECT count() FROM file('${TYPES}') WHERE mapContainsKey(mlc, 'k2');
SELECT count() FROM file('${TYPES}') WHERE mapContainsKeyLike(ms, 'k%');
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(ms, '10');
SELECT count() FROM file('${TYPES}') WHERE mapContainsValueLike(ms, '1%');
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(mlc, 'v1');
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(mn, NULL);
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(ma, [0, 1]);
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(mm, map('i', 1));
SELECT count() FROM file('${TYPES}') WHERE mapContainsValue(mt, tuple(1));
SELECT count() FROM file('${TYPES}') WHERE has(t.x, 'x1');
SELECT 'both sides';
SELECT countIf(has(ms, 'k0')), countIf(mapContainsValue(ms, 'v')), sum(length(mapKeys(mn))), sum(length(mapValues(mn))) FROM file('${TYPES}');
SELECT 'with other columns';
SELECT id, mapValues(ms) FROM file('${TYPES}') WHERE has(ms, 'k2') ORDER BY id;
SELECT id FROM file('${TYPES}') WHERE mapContainsValue(ma, [0]) ORDER BY id;
SELECT id, ms FROM file('${TYPES}') WHERE has(ms, 'k0') ORDER BY id;
SELECT 'PREWHERE';
SELECT id, mapKeys(ms) FROM file('${TYPES}') PREWHERE has(ms, 'k2') ORDER BY id;
SELECT id FROM file('${TYPES}') PREWHERE has(ms, 'k2') WHERE mapContainsValue(ms, '11') ORDER BY id;
SELECT mapKeys(ms) FROM file('${TYPES}') PREWHERE id > 25 ORDER BY id;
SELECT 'lazy materialization';
SELECT id, mapKeys(ms), mapValues(mn) FROM file('${TYPES}') ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT id, ms.keys, mn.values FROM file('${TYPES}') ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT id, ms.keys, mn.values FROM t_05509_types ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT 'filter on another column';
SELECT id, mapKeys(ms) FROM file('${TYPES}') WHERE id > 5 ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT id, mapValues(mn) FROM file('${TYPES}') WHERE id > 5 ORDER BY id LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT id, mapKeys(ms) FROM file('${TYPES}') PREWHERE id > 5 ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT mapKeys(ms) FROM file('${TYPES}') WHERE id > 25 ORDER BY id SETTINGS optimize_move_to_prewhere = 1;
SELECT countIf(has(ms, 'k1')), count() FROM file('${TYPES}') WHERE id > 5 SETTINGS optimize_move_to_prewhere = 1;
SELECT id, mapKeys(ms) FROM t_05509_types WHERE id > 5 ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT id, mapValues(mn) FROM t_05509_types WHERE id > 5 ORDER BY id LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT 'defaults';
SELECT id, mapKeys(m), n FROM file('${CLICKHOUSE_DATABASE}_05509_d_*.parquet', Parquet,
    'id UInt64, m Map(String, UInt8) DEFAULT map(\'d\', 1), n String DEFAULT toString(m[\'a\'])') ORDER BY id;
SELECT id, mapValues(m) FROM file('${CLICKHOUSE_DATABASE}_05509_d_*.parquet', Parquet,
    'id UInt64, m Map(String, UInt8) DEFAULT map(\'d\', 1)') ORDER BY id;
SELECT id, m.keys, n FROM file('${CLICKHOUSE_DATABASE}_05509_d_*.parquet', Parquet,
    'id UInt64, m Map(String, UInt8) DEFAULT map(\'d\', 1), n String DEFAULT toString(m[\'a\'])') ORDER BY id;
SELECT 'row policy, additional_table_filters';
SELECT id, mapKeys(m) FROM t_05509_policy ORDER BY id;
SELECT count() FROM t_05509_policy WHERE has(m, 'a');
SELECT id, mapKeys(m) FROM t_05509_filter ORDER BY id SETTINGS additional_table_filters = {'t_05509_filter': 'm[\'a\'] = 2'};
SELECT count() FROM t_05509_filter WHERE has(m, 'b') SETTINGS additional_table_filters = {'t_05509_filter': 'm[\'a\'] = 2'};
SELECT 'Iceberg, schema evolution';
SELECT id, mapKeys(a), mapKeys(m), a, s2 FROM t_05509_evolved ORDER BY id;
SELECT id, m.keys, a FROM t_05509_evolved ORDER BY id;
SELECT id, mapValues(a) FROM t_05509_evolved ORDER BY id;
SELECT count() FROM t_05509_evolved WHERE has(a, 'k1');
SELECT id, mapKeys(a) FROM t_05509_evolved ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT 'Iceberg, Parquet and ORC files';
SELECT id, mapKeys(m) FROM t_05509_mixed ORDER BY id;
SELECT count() FROM t_05509_mixed WHERE has(m, 'k1');
SELECT count() FROM t_05509_mixed WHERE mapContainsValueLike(m, 'o%');
SELECT 'filters from the plan';
SELECT id, k FROM (SELECT id, mapKeys(ms) AS k FROM file('${TYPES}')) WHERE id > 5 ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT id, v FROM v_05509 WHERE id > 5 ORDER BY id LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT id, mapKeys(m) FROM t_05509_filter ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1, additional_table_filters = {'t_05509_filter': 'id > 1'};
SELECT a.id, mapKeys(a.ms) FROM file('${TYPES}') AS a INNER JOIN (SELECT number AS n FROM numbers(30) WHERE number % 2 = 0) AS b ON a.id = b.n ORDER BY a.id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1, enable_join_runtime_filters = 1, join_algorithm = 'hash';
SELECT a.id, mapKeys(a.ms) FROM file('${TYPES}') AS a INNER JOIN numbers(30) AS b ON a.id = b.number WHERE b.number > 5 ORDER BY a.id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1, join_algorithm = 'hash';
SELECT id, k FROM (SELECT id, mapKeys(ms) AS k FROM t_05509_types) WHERE id > 5 ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1;
SELECT length(mapKeys(m)) FROM t_05509_filter ORDER BY rand() LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10, optimize_move_to_prewhere = 1, additional_table_filters = {'t_05509_filter': 'id > 1'};
SELECT 'case-insensitive name collision';
SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE has(m, 'other') SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE has(m, 'key') SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE mapContainsValue(m, toUInt64(4)) SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE mapContainsValue(m, toUInt64(1)) SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
SELECT 'the side that is read has no leaf in the file';
SELECT mapValues(mv) FROM file('${MISSING}', Parquet, 'mv Map(String, Tuple(a String))') SETTINGS input_format_parquet_allow_missing_columns = 1;
SELECT count() FROM file('${MISSING}', Parquet, 'mv Map(String, Tuple(a String))') WHERE mapContainsValue(mv, tuple('')) SETTINGS input_format_parquet_allow_missing_columns = 1;
SELECT mapKeys(mk) FROM file('${MISSING}', Parquet, 'mk Map(Tuple(a String), String)') SETTINGS input_format_parquet_allow_missing_columns = 1;
SELECT mv.values, mk.keys, id FROM file('${MISSING}', Parquet, 'mv Map(String, Tuple(a String)), mk Map(Tuple(a String), String), id UInt64') SETTINGS input_format_parquet_allow_missing_columns = 1;
"

${CLICKHOUSE_CLIENT} -m --query "
SET allow_experimental_insert_into_iceberg = 1, allow_insert_into_iceberg = 1;

CREATE TABLE t_05509_policy (id UInt64, m Map(String, UInt8)) ENGINE = File(Parquet, '${WITH_M}');
CREATE ROW POLICY ${CLICKHOUSE_DATABASE}_05509 ON t_05509_policy USING mapContainsValue(m, 3) TO ALL;
CREATE TABLE t_05509_filter (id UInt64, m Map(String, UInt8)) ENGINE = File(Parquet, '${WITH_M}');

-- Files written before the renames are read with the names of their own schema: there \`m\` is the current \`a\`.
CREATE TABLE t_05509_evolved (id Int64, m Map(String, String), m2 Map(String, String), s String)
    ENGINE = IcebergLocal('${ICEBERG_PATH}', 'Parquet');
INSERT INTO t_05509_evolved SELECT number, map('k' || toString(number % 3), 'v' || toString(number)), map('p' || toString(number), 'q'), 's' || toString(number) FROM numbers(4);
ALTER TABLE t_05509_evolved ADD COLUMN n Nullable(Int64);
INSERT INTO t_05509_evolved SELECT number + 10, map('k' || toString(number % 3), 'w' || toString(number)), map('p', 'r'), 's', number FROM numbers(2);
ALTER TABLE t_05509_evolved RENAME COLUMN s TO s2;
ALTER TABLE t_05509_evolved RENAME COLUMN m TO a;
ALTER TABLE t_05509_evolved RENAME COLUMN m2 TO m;
INSERT INTO t_05509_evolved SELECT number + 20, map('a' || toString(number), 'x'), map('z', 'y' || toString(number)), 's', number FROM numbers(2);

CREATE TABLE t_05509_types (id Int64, ms Map(String, String), mn Map(String, Nullable(Int32)))
    ENGINE = IcebergLocal('${ICEBERG_TYPES_PATH}', 'Parquet');
INSERT INTO t_05509_types SELECT number,
    if(number % 7 = 0, map(), map('k' || toString(number % 3), toString(number), 'c', 'v')),
    map('k', if(number % 4 = 0, NULL, toInt32(number)))
FROM numbers(30);
CREATE VIEW v_05509 AS SELECT id, mapValues(mn) AS v FROM t_05509_types;

CREATE TABLE t_05509_bytes (id Int64, m Map(String, String), mv Map(String, UInt8))
    ENGINE = IcebergLocal('${ICEBERG_BYTES_PATH}', 'Parquet');
INSERT INTO t_05509_bytes SELECT toInt64(number),
    map('k' || toString(number % 5), repeat(toString(number % 10), 200)),
    map(repeat(toString(number % 10), 200), toUInt8(number % 5))
FROM numbers(1000);

CREATE TABLE t_05509_mixed (id Int64, m Map(String, String)) ENGINE = IcebergLocal('${ICEBERG_MIXED_PATH}', 'Parquet');
INSERT INTO t_05509_mixed SELECT number, map('k' || toString(number % 3), 'v' || toString(number)) FROM numbers(4);
INSERT INTO TABLE FUNCTION icebergLocal('${ICEBERG_MIXED_PATH}', 'ORC', 'id Int64, m Map(String, String)')
    SELECT number + 100, map('k' || toString(number % 3), 'o' || toString(number)) FROM numbers(3);
"

off=$(${CLICKHOUSE_CLIENT} -m --query "SET optimize_functions_to_subcolumns = 0; ${QUERIES}" 2>&1)
on=$(${CLICKHOUSE_CLIENT} -m --query "SET optimize_functions_to_subcolumns = 1; ${QUERIES}" 2>&1)
echo "${on}"
if [ "${off}" != "${on}" ]; then
    echo "The rewrite changed the results:"
    diff <(echo "${off}") <(echo "${on}")
fi

echo "rewritten"
${CLICKHOUSE_CLIENT} -m --query "
SET optimize_functions_to_subcolumns = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE has(m, 'k3')) WHERE explain ILIKE '%m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE mapContainsValue(mv, 3)) WHERE explain ILIKE '%mv.values%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT mapKeys(m) FROM file('${BYTES}')) WHERE explain ILIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT mapValues(mv) FROM file('${BYTES}')) WHERE explain ILIKE '%mv.values%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE mapContainsKey(m, 'k3')) WHERE explain ILIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE notHas(m, 'k3')) WHERE explain ILIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE mapContainsKeyLike(m, 'k%')) WHERE explain ILIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${BYTES}') WHERE mapContainsValueLike(m, '1%')) WHERE explain ILIKE '%m.values%';
-- A physical \`M.keys\` or \`M.values\` does not stop the rewrite. LIKE, not ILIKE: the structure argument names them.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE has(m, 'other')) WHERE explain LIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM file('${COLLISION}', Parquet, 'm Map(String, UInt64), \`M.keys\` Array(String), \`M.values\` Array(UInt64)') WHERE mapContainsValue(m, toUInt64(4))) WHERE explain LIKE '%m.values%';
-- Not rewritten anywhere: PREWHERE reads the whole map.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT mapKeys(m) FROM file('${BYTES}') PREWHERE has(m, 'k3')) WHERE explain ILIKE '%m.keys%';
-- Not rewritten: the query reads another column of the table.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id FROM file('${BYTES}') WHERE has(m, 'k3')) WHERE explain ILIKE '%m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id, mapKeys(m) FROM file('${BYTES}') WHERE id > 5 ORDER BY id LIMIT 3) WHERE explain ILIKE '%m.keys%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT id, mapKeys(m) FROM file('${BYTES}')) WHERE explain ILIKE '%m.keys%';
SELECT count() FROM (EXPLAIN SELECT id, mapKeys(m) FROM file('${BYTES}') ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10) WHERE explain ILIKE '%LazilyReadFromFile%';
SELECT count() FROM (EXPLAIN SELECT id, mapKeys(m) FROM t_05509_bytes ORDER BY id DESC LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10) WHERE explain ILIKE '%LazilyReadFromObjectStorage%';
"

echo "decompressed bytes"
URL="http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+%2A+FROM+file(%27${BYTES}%27)+FORMAT+Parquet"
for setting in 0 1; do
    ${CLICKHOUSE_CLIENT} -m --query "
    SET optimize_functions_to_subcolumns = ${setting}, use_columns_cache = 0, use_query_condition_cache = 0;
    SELECT count() FROM file('${BYTES}') WHERE has(m, 'k3') SETTINGS log_comment = 'bytes_keys_${setting}' FORMAT Null;
    SELECT count() FROM file('${BYTES}') WHERE mapContainsValue(mv, 3) SETTINGS log_comment = 'bytes_values_${setting}' FORMAT Null;
    SELECT count() FROM t_05509_bytes WHERE has(m, 'k3') SETTINGS log_comment = 'bytesice_keys_${setting}' FORMAT Null;
    SELECT count() FROM t_05509_bytes WHERE mapContainsValue(mv, 3) SETTINGS log_comment = 'bytesice_values_${setting}' FORMAT Null;
    SELECT count() FROM url('${URL}', Parquet, 'id UInt64, m Map(String, String), mv Map(String, UInt8)') WHERE has(m, 'k3')
        SETTINGS parallel_replicas_for_cluster_engines = 0, log_comment = 'bytesurl_keys_${setting}' FORMAT Null;
    SELECT count() FROM url('${URL}', Parquet, 'id UInt64, m Map(String, String), mv Map(String, UInt8)') WHERE mapContainsValue(mv, 3)
        SETTINGS parallel_replicas_for_cluster_engines = 0, log_comment = 'bytesurl_values_${setting}' FORMAT Null;
    "
done
${CLICKHOUSE_CLIENT} -m --query "
-- TopN filtering would skip a timing-dependent number of row groups of \`id\` in the main read.
SET use_columns_cache = 0, use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT id, mapKeys(m) FROM file('${BYTES}') ORDER BY id DESC LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 0, log_comment = 'lazy_keys_0' FORMAT Null;
SELECT id, m.keys FROM file('${BYTES}') ORDER BY id DESC LIMIT 3 SETTINGS log_comment = 'lazy_keys_1' FORMAT Null;
SELECT id, mapValues(mv) FROM t_05509_bytes ORDER BY id DESC LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 0, log_comment = 'lazyice_values_0' FORMAT Null;
SELECT id, mv.values FROM t_05509_bytes ORDER BY id DESC LIMIT 3 SETTINGS log_comment = 'lazyice_values_1' FORMAT Null;
"
${CLICKHOUSE_CLIENT} -m --query "
SYSTEM FLUSH LOGS query_log;
-- The Iceberg and URL files have one row group, so one dictionary per leaf and a smaller ratio.
-- The lazy pairs share the main read of \`id\`, so only \`on < off\`.
SELECT side, multiIf(startsWith(side, 'bytes_'), on * 4 < off, startsWith(side, 'lazy'), on < off, on * 2 < off)
FROM
(
    SELECT
        splitByChar('_', log_comment)[1] || '_' || splitByChar('_', log_comment)[2] AS side,
        sumIf(ProfileEvents['ParquetDecompressedBytes'], endsWith(log_comment, '_0')) AS off,
        sumIf(ProfileEvents['ParquetDecompressedBytes'], endsWith(log_comment, '_1')) AS on
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (startsWith(log_comment, 'bytes') OR startsWith(log_comment, 'lazy'))
    GROUP BY side
)
ORDER BY side;
"

${CLICKHOUSE_CLIENT} -m --query "
DROP ROW POLICY ${CLICKHOUSE_DATABASE}_05509 ON t_05509_policy;
DROP TABLE t_05509_policy;
DROP TABLE t_05509_filter;
DROP TABLE t_05509_evolved;
DROP TABLE t_05509_mixed;
DROP VIEW v_05509;
DROP TABLE t_05509_types;
DROP TABLE t_05509_bytes;
"
rm -rf "${ICEBERG_PATH}" "${ICEBERG_MIXED_PATH}" "${ICEBERG_TYPES_PATH}" "${ICEBERG_BYTES_PATH}"
rm -f "${CLICKHOUSE_USER_FILES}/${TYPES}" "${CLICKHOUSE_USER_FILES}/${BYTES}" "${CLICKHOUSE_USER_FILES}/${WITH_M}" "${CLICKHOUSE_USER_FILES}/${WITHOUT_M}" "${CLICKHOUSE_USER_FILES}/${COLLISION}" "${CLICKHOUSE_USER_FILES}/${MISSING}"
