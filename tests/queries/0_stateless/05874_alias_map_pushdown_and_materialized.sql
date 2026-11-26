-- Test constant-key ALIAS Map pushdown + automatic MATERIALIZED column substitution

SET optimize_map_alias_pushdown = 1;
SET optimize_map_materialized_columns = 1;

-- 1. Use Case 1a: Migrating / merging maps A, B -> C with prefix slicing
DROP TABLE IF EXISTS t_merged_map;
CREATE TABLE t_merged_map
(
    id UInt64,
    C Map(String, String),
    A Map(String, String) ALIAS mapApply((k, v) -> (substring(k, 3), v), mapFilter((k, v) -> startsWith(k, 'a.'), C)),
    B Map(String, String) ALIAS mapApply((k, v) -> (substring(k, 3), v), mapFilter((k, v) -> startsWith(k, 'b.'), C))
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_merged_map VALUES (1, {'a.foo': '10', 'a.bar': '20', 'b.foo': '30'}), (2, {'b.baz': '40'});

SELECT '=== 1a. Merged map queries ===';
SELECT id, A['foo'], A['bar'], B['foo'], B['baz'], A['missing'] FROM t_merged_map ORDER BY id;
SELECT id, has(A, 'foo'), mapContainsKey(A, 'baz'), has(B, 'foo') FROM t_merged_map ORDER BY id;

SELECT '=== 1a. Dynamic key column falls back cleanly to standard ALIAS ===';
SELECT id, k, A[k], has(A, k) FROM t_merged_map ARRAY JOIN ['foo', 'bar', 'baz'] AS k ORDER BY id, k;

SELECT '=== 1a. Whole-map access ===';
SELECT id, mapSort(A), mapSort(B) FROM t_merged_map ORDER BY id;

SELECT '=== 1a. Verify mapApply/mapFilter pushed down ===';
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT A['foo'] FROM t_merged_map SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%C%' AND explain LIKE '%a.foo%');
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT A['foo'] FROM t_merged_map SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%mapApply%');

-- 2. Use Case 1b: Splitting hot_attrs and cold_attrs with unused-column pruning
DROP TABLE IF EXISTS t_split_map;
CREATE TABLE t_split_map
(
    id UInt64,
    hot_attrs Map(String, String),
    cold_attrs Map(String, String),
    attrs Map(String, String) ALIAS mapUpdate(
        mapFilter((k, v) -> k NOT IN ('service', 'status'), cold_attrs),
        mapFilter((k, v) -> k IN ('service', 'status'), hot_attrs)
    )
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_split_map VALUES
    (1, {'service': 'auth', 'status': '200', 'stale': 'ignore'}, {'trace_id': 't1', 'service': 'old_auth'}),
    (2, {'service': 'pay'}, {'trace_id': 't2', 'user_agent': 'curl'});

SELECT '=== 1b. Split map queries & unused map pruning ===';
SELECT id, attrs['service'], attrs['status'], attrs['trace_id'], has(attrs, 'stale'), has(attrs, 'service'), has(attrs, 'trace_id')
FROM t_split_map ORDER BY id;
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT attrs['service'] FROM t_split_map SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%cold_attrs%');
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT attrs['trace_id'] FROM t_split_map SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%hot_attrs%');

-- 3. Use Case 2: Dynamic MATERIALIZED columns without editing ALIAS, PK pruning, & subcolumn synergy
DROP TABLE IF EXISTS t_logs;
CREATE TABLE t_logs
(
    ts UInt32,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service'],
    attrs Map(String, String) ALIAS mapFilter((k, v) -> k != 'ssn', raw_attrs),
    INDEX idx_mat_svc __mat_service TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (__mat_service, ts)
SETTINGS index_granularity = 2;

INSERT INTO t_logs VALUES
    (1, {'service': 'checkout', 'http.url': '/pay', 'ssn': '111-11', 'status': '200'}),
    (2, {'service': 'auth', 'http.url': '/login', 'ssn': '222-22', 'status': '200'}),
    (3, {'service': 'search', 'http.url': '/find', 'ssn': '333-33', 'status': '404'}),
    (4, {'service': 'checkout', 'http.url': '/confirm', 'ssn': '444-44', 'status': '500'});

SELECT '=== 2. Materialized + masked query & PK condition check ===';
SELECT ts, attrs['service'], attrs['http.url'], attrs['ssn'], has(attrs, 'ssn') FROM t_logs WHERE attrs['service'] = 'checkout' ORDER BY ts;
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT ts FROM t_logs WHERE attrs['service'] = 'checkout' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%raw_attrs%');
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT ts FROM t_logs WHERE attrs['service'] = 'checkout' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_service%');
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN indexes = 1 SELECT ts FROM t_logs WHERE attrs['service'] = 'checkout' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Condition: (__mat_service in%');
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT count() FROM t_logs WHERE has(attrs, 'service') SETTINGS optimize_functions_to_subcolumns = 1, enable_parallel_replicas = 0) WHERE explain LIKE '%raw_attrs.keys%');

SELECT '=== 2b. Add & drop __mat_http_url WITHOUT changing attrs ALIAS ===';
ALTER TABLE t_logs ADD COLUMN __mat_http_url String MATERIALIZED raw_attrs['http.url'] SETTINGS mutations_sync = 1;
ALTER TABLE t_logs MATERIALIZE COLUMN __mat_http_url SETTINGS mutations_sync = 1;
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT attrs['http.url'] FROM t_logs SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_http_url%');
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT attrs['http.url'] FROM t_logs SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%raw_attrs%');
SELECT ts, attrs['http.url'] FROM t_logs ORDER BY ts;

ALTER TABLE t_logs DROP COLUMN __mat_http_url SETTINGS mutations_sync = 1;
SELECT ts, attrs['http.url'] FROM t_logs ORDER BY ts;

SELECT '=== 2c. Type mismatch guard prevents silent breakage ===';
ALTER TABLE t_logs ADD COLUMN __mat_status_u16 UInt16 MATERIALIZED toUInt16OrZero(raw_attrs['status']) SETTINGS mutations_sync = 1;
ALTER TABLE t_logs ADD COLUMN __mat_status_direct UInt16 MATERIALIZED raw_attrs['status'] SETTINGS mutations_sync = 1;
SELECT ts, startsWith(attrs['status'], '2') FROM t_logs ORDER BY ts;

SELECT '=== 3. Non-substitution guards: Memory engine, SummingMergeTree, ReplacingMergeTree FINAL, column TTL ===';
DROP TABLE IF EXISTS t_mem_mat;
CREATE TABLE t_mem_mat (id UInt64, raw_attrs Map(String, String)) ENGINE = Memory;
INSERT INTO t_mem_mat VALUES (1, {'service': 'legacy_mem'});
ALTER TABLE t_mem_mat ADD COLUMN __mat_service String MATERIALIZED raw_attrs['service'];
SELECT id, raw_attrs['service'], __mat_service FROM t_mem_mat ORDER BY id;
DROP TABLE t_mem_mat;

DROP TABLE IF EXISTS t_sum_mat;
CREATE TABLE t_sum_mat
(
    id UInt64,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service']
)
ENGINE = SummingMergeTree
ORDER BY id;
INSERT INTO t_sum_mat VALUES (1, {'service': 'live_sum'});
SELECT id, raw_attrs['service'] FROM t_sum_mat ORDER BY id;
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT raw_attrs['service'] FROM t_sum_mat SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_service%');
DROP TABLE t_sum_mat;

DROP TABLE IF EXISTS t_rep_final;
CREATE TABLE t_rep_final
(
    id UInt64,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service']
)
ENGINE = ReplacingMergeTree
ORDER BY id;
INSERT INTO t_rep_final VALUES (1, {'service': 'live_rep'});
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT raw_attrs['service'] FROM t_rep_final FINAL SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_service%');
DROP TABLE t_rep_final;

DROP TABLE IF EXISTS t_ttl_mat;
CREATE TABLE t_ttl_mat
(
    dt Date,
    raw_attrs Map(String, String),
    __mat_service String MATERIALIZED raw_attrs['service'] TTL dt + INTERVAL 1 DAY
)
ENGINE = MergeTree
ORDER BY dt;
INSERT INTO t_ttl_mat VALUES (today(), {'service': 'live_ttl'});
SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT raw_attrs['service'] FROM t_ttl_mat SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_service%');
DROP TABLE t_ttl_mat;

SELECT '=== 4. SQL shapes: GROUP BY, ROLLUP, JOIN, PREWHERE, CTE, VIEW, EXPLAIN SYNTAX, self-join ===';
SELECT attrs['service'] AS svc, count() AS cnt FROM t_logs GROUP BY attrs['service'] ORDER BY svc;
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN actions = 1 SELECT attrs['service'], count() FROM t_logs GROUP BY attrs['service'] SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%__mat_service%');
SELECT attrs['service'], count() FROM t_logs GROUP BY attrs['service'] WITH ROLLUP ORDER BY 1 NULLS FIRST, 2 SETTINGS group_by_use_nulls = 1;
SELECT attrs['service'], count() FROM t_logs GROUP BY attrs ORDER BY 1, 2;
SELECT raw_attrs['service'], count() FROM t_logs GROUP BY raw_attrs ORDER BY 1, 2;

SELECT count() = 0 FROM (SELECT explain FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT attrs['service'] AS svc FROM t_logs SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%raw_attrs%' OR explain LIKE '%__mat_service%');
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT attrs['service'] AS svc FROM t_logs SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%AS svc%');
SELECT t1.attrs['service'], t2.attrs['service'] FROM t_logs AS t1 INNER JOIN t_logs AS t2 ON t1.ts = t2.ts WHERE t1.ts <= 2 ORDER BY t1.ts;

DROP TABLE IF EXISTS t_logs_right;
CREATE TABLE t_logs_right
(
    ts UInt32,
    raw_attrs Map(String, String),
    attrs Map(String, String) ALIAS mapFilter((k, v) -> k != 'secret_r', raw_attrs)
)
ENGINE = MergeTree
ORDER BY ts;
INSERT INTO t_logs_right VALUES (1, {'service': 'right_svc', 'secret_r': 'hidden'});
SELECT t_logs.ts, r.attrs['service'], r.attrs['secret_r'] FROM t_logs LEFT JOIN t_logs_right AS r ON t_logs.ts = r.ts ORDER BY t_logs.ts SETTINGS join_use_nulls = 1;
DROP TABLE t_logs_right;

SELECT attrs['service'], attrs['http.url'], attrs['ssn'] FROM t_logs PREWHERE attrs['service'] = 'checkout' ORDER BY ts;
SELECT ts, m['service'] FROM t_logs ARRAY JOIN [map('service', 'shadow_val')] AS m ORDER BY ts;
SELECT ts, arrayMap(attrs -> attrs['service'], [map('service', 'lambda_val')]) FROM t_logs WHERE ts = 1;
WITH cte AS (SELECT ts, attrs FROM t_logs WHERE ts <= 2) SELECT ts, attrs['service'], attrs['ssn'] FROM cte ORDER BY ts;
DROP VIEW IF EXISTS v_logs;
CREATE VIEW v_logs AS SELECT ts, attrs FROM t_logs;
SELECT ts, attrs['service'], attrs['ssn'] FROM v_logs ORDER BY ts;
DROP VIEW v_logs;

DROP TABLE IF EXISTS t_logs_nested_pw;
CREATE TABLE t_logs_nested_pw
(
    ts UInt32,
    raw_attrs Map(String, String),
    a1 Map(String, String) ALIAS mapFilter((k, v) -> k != 'ssn', raw_attrs),
    a2 Map(String, String) ALIAS mapFilter((k, v) -> k != 'secret', a1)
)
ENGINE = MergeTree
ORDER BY ts;
INSERT INTO t_logs_nested_pw VALUES (1, {'service': 'ok', 'ssn': '1', 'secret': '2'}), (2, {'service': 'other'});
SELECT ts, a2['service'], a2['ssn'], a2['secret'] FROM t_logs_nested_pw PREWHERE a2['service'] = 'ok' AND a1['service'] = 'ok' ORDER BY ts;
DROP TABLE t_logs_nested_pw;

SELECT '=== 5. Map(Int64, String) key normalization & PK candidate preference ===';
DROP TABLE IF EXISTS t_int_map_pk;
CREATE TABLE t_int_map_pk
(
    id UInt64,
    raw_attrs Map(Int64, String),
    __mat_non_pk String MATERIALIZED raw_attrs[42],
    __mat_pk String MATERIALIZED raw_attrs[42]
)
ENGINE = MergeTree
ORDER BY (__mat_pk, id);
INSERT INTO t_int_map_pk VALUES (1, {42: 'found_42'}), (2, {7: 'other'});
SELECT id, raw_attrs[toInt64(42)], raw_attrs[toUInt8(42)] FROM t_int_map_pk WHERE raw_attrs[toUInt8(42)] = 'found_42' ORDER BY id;
SELECT count() >= 1 FROM (SELECT explain FROM (EXPLAIN indexes = 1 SELECT id FROM t_int_map_pk WHERE raw_attrs[42] = 'found_42' SETTINGS enable_parallel_replicas = 0) WHERE explain LIKE '%Condition: (__mat_pk in%');
DROP TABLE t_int_map_pk;

SELECT '=== 6. Algebraic soundness regressions ===';
DROP TABLE IF EXISTS t_edge_alg;
CREATE TABLE t_edge_alg
(
    id UInt64,
    raw_attrs Map(String, String),
    raw_dates Map(String, String),
    raw_nums Map(String, String),
    raw_int32 Map(String, Int32),
    m_nullable_val Map(String, Nullable(Int32)) ALIAS raw_int32,
    m_date_cast Map(String, Date) ALIAS CAST(raw_dates, 'Map(String, Date)'),
    m_empty Map(Nothing, Nothing) ALIAS map(),
    m_cast_empty Map(String, String) ALIAS CAST(map(), 'Map(String, String)'),
    m_dead Map(String, String) ALIAS mapApply((k, v) -> (k, if(0, toString(intDiv(1, toInt64OrZero(v))), v)), raw_attrs),
    m_null_pred Map(String, String) ALIAS mapFilter((k, v) -> toNullable(k != 'ssn'), raw_attrs),
    m_lc_null_pred Map(String, String) ALIAS mapFilter((k, v) -> toLowCardinality(if(k = 'ssn', CAST(NULL, 'Nullable(UInt8)'), 1)), raw_attrs),
    m_and_u16 Map(String, String) ALIAS mapFilter((k, v) -> and(1, toUInt16(256)), raw_attrs),
    m_and_u8 Map(String, String) ALIAS mapFilter((k, v) -> and(1, toUInt8(5)), raw_attrs),
    m_fs_in Map(FixedString(4), String) ALIAS mapFilter((k, v) -> k IN (toFixedString('k1', 4), toFixedString('k2', 4)), CAST(map('k1', 'v1', 'k3', 'v3'), 'Map(FixedString(4), String)')),
    m_date Map(Date, String) ALIAS mapFilter((k, v) -> k IN (toDateTime(1)), CAST(map(toDate(1), 'x'), 'Map(Date, String)')),
    m_u8 Map(UInt8, String) ALIAS mapFilter((k, v) -> k != 1, CAST(map(44, 'hit'), 'Map(UInt8, String)')),
    m_sc_div Map(String, Int64) ALIAS mapApply((k, v) -> (k, intDiv(100, toInt64(v))), raw_nums),
    m_null_in Map(String, String) ALIAS mapFilter((k, v) -> k IN ('service', NULL), raw_attrs)
)
ENGINE = MergeTree
ORDER BY id;
INSERT INTO t_edge_alg VALUES (1, {'service': '10', 'ssn': '123', 'not_num': 'abc'}, {'d1': '2026-01-01'}, {'x': '10'}, {'a': 7});
SELECT m_nullable_val['a'], m_nullable_val['missing'], m_date_cast['d1'], m_date_cast['missing'] FROM t_edge_alg;
SELECT m_cast_empty['k'], has(m_cast_empty, 'k'), has(m_empty, 'k') FROM t_edge_alg;
SELECT m_dead['not_num'], m_null_pred['service'], m_null_pred['ssn'], has(m_null_pred, 'ssn') FROM t_edge_alg;
SELECT m_lc_null_pred['service'], m_lc_null_pred['ssn'], has(m_lc_null_pred, 'ssn') FROM t_edge_alg;
SELECT m_null_pred[CAST(NULL, 'Nullable(String)')], m_null_pred[CAST(NULL, 'LowCardinality(Nullable(String))')], m_null_pred[toNullable('service')] FROM t_edge_alg;
SELECT m_and_u16['service'], has(m_and_u16, 'service'), m_and_u8['service'], has(m_and_u8, 'service') FROM t_edge_alg;
SELECT m_fs_in[toFixedString('k1', 4)], m_fs_in[toFixedString('k3', 4)] FROM t_edge_alg;
SELECT m_date[toDate(1)], has(m_u8, toUInt16(300)), has(m_u8, toUInt8(44)) FROM t_edge_alg;
SELECT m_sc_div['missing'] FROM t_edge_alg SETTINGS short_circuit_function_evaluation = 'disable';
SELECT m_null_in['service'], has(m_null_in, 'not_num') FROM t_edge_alg SETTINGS transform_null_in = 1;
SELECT ts, attrs['service'] FROM t_logs WHERE ts = 1 SETTINGS optimize_map_alias_pushdown = 0, optimize_map_materialized_columns = 0;

DROP TABLE t_edge_alg;
DROP TABLE t_logs;
DROP TABLE t_split_map;
DROP TABLE t_merged_map;
