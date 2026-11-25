-- A `bloom_filter` index on a column literally named like an expression is not used for that expression.

DROP TABLE IF EXISTS t_bf_array;
DROP TABLE IF EXISTS t_bf_lower;
DROP TABLE IF EXISTS t_bf_fixed;
DROP TABLE IF EXISTS t_bf_lc;
DROP TABLE IF EXISTS t_bf_real;
DROP TABLE IF EXISTS t_bf_order;
DROP TABLE IF EXISTS t_bf_json;
DROP TABLE IF EXISTS t_bf_json_shadow;

CREATE TABLE t_bf_fixed
(
    m Map(String, String),
    `mapKeys(m)` FixedString(16),
    INDEX idx `mapKeys(m)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_fixed VALUES (map('a', 'x'), 'deadbeefdeadbeef');
INSERT INTO t_bf_fixed VALUES (map('k', 'v'), 'deadbeefdeadbeef');

SELECT count() FROM t_bf_fixed WHERE m['k'] = 'v';
SELECT count() FROM t_bf_fixed WHERE m['k'] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bf_fixed WHERE mapContainsKey(m, 'k');
SELECT count() FROM t_bf_fixed WHERE has(m, 'k');

CREATE TABLE t_bf_lc
(
    m Map(String, String),
    `mapKeys(m)` LowCardinality(String),
    `mapValues(m)` LowCardinality(String),
    INDEX idx_k `mapKeys(m)` TYPE bloom_filter,
    INDEX idx_v `mapValues(m)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_lc VALUES (map('k', 'v'), 'x', 'y');

SELECT count() FROM t_bf_lc WHERE m['k'] = 'v';
SELECT count() FROM t_bf_lc WHERE m['k'] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_bf_lc WHERE mapContainsKey(m, 'k');
SELECT count() FROM t_bf_lc WHERE mapContainsValue(m, 'v');
SELECT count() FROM t_bf_lc WHERE has(m, 'k');

-- A real `mapKeys(m)` index is still used.
CREATE TABLE t_bf_real
(
    m Map(String, String),
    INDEX idx mapKeys(m) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_real VALUES (map('k', 'v'));

SELECT count() FROM t_bf_real WHERE m['k'] = 'v' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bf_real WHERE mapContainsKey(m, 'k') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_bf_real WHERE has(mapKeys(m), 'k') SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;

CREATE TABLE t_bf_array
(
    m Map(String, String),
    `mapKeys(m)` Array(String),
    `mapValues(m)` Array(String),
    INDEX idx_k `mapKeys(m)` TYPE bloom_filter,
    INDEX idx_v `mapValues(m)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_array VALUES (map('k', 'v'), ['x'], ['y']);

SET optimize_functions_to_subcolumns = 0;

SELECT count() FROM t_bf_array WHERE has(mapKeys(m), 'k');
SELECT count() FROM t_bf_array WHERE mapKeys(m) IN (['k']);
SELECT count() FROM t_bf_array WHERE arrayJoin(mapKeys(m)) = 'k';
SELECT count() FROM t_bf_array WHERE arrayJoin(mapKeys(m)) IN ('k');
SELECT count() FROM t_bf_array WHERE has(mapValues(m), 'v');
SELECT count() FROM t_bf_array WHERE mapValues(m) IN (['v']);

-- Still used for the column itself.
SELECT count() FROM t_bf_array WHERE has(`mapKeys(m)`, 'x') SETTINGS force_data_skipping_indices = 'idx_k';
SELECT count() FROM t_bf_array WHERE `mapKeys(m)` IN (['x']) SETTINGS force_data_skipping_indices = 'idx_k';
SELECT count() FROM t_bf_array WHERE has(`mapValues(m)`, 'y') SETTINGS force_data_skipping_indices = 'idx_v';

-- Not specific to `Map`.
CREATE TABLE t_bf_lower
(
    s String,
    `lower(s)` String,
    INDEX idx `lower(s)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_lower VALUES ('ABC', 'x');

SELECT count() FROM t_bf_lower WHERE lower(s) = 'abc';
SELECT count() FROM t_bf_lower WHERE lower(s) IN ('abc');
SELECT count() FROM t_bf_lower WHERE `lower(s)` = 'x' SETTINGS force_data_skipping_indices = 'idx';

-- Same columns and names, different argument order.
CREATE TABLE t_bf_order
(
    `'k'` String,
    arr Array(String),
    INDEX idx_map mapKeys(map(`'k'`, 'k')) TYPE bloom_filter,
    INDEX idx_concat concat(`'k'`, 'k') TYPE bloom_filter,
    INDEX idx_lambda arrayMap(x -> concat(x, `'k'`, 'k'), arr) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_order VALUES ('x', ['a']);

SELECT count() FROM t_bf_order WHERE map('k', `'k'`)['k'] = 'x';
SELECT count() FROM t_bf_order WHERE mapContainsKey(map('k', `'k'`), 'k');
SELECT count() FROM t_bf_order WHERE has(mapKeys(map('k', `'k'`)), 'k');
SELECT count() FROM t_bf_order WHERE mapKeys(map('k', `'k'`)) IN (['k']);
SELECT count() FROM t_bf_order WHERE arrayJoin(mapKeys(map('k', `'k'`))) = 'k';
SELECT count() FROM t_bf_order WHERE concat('k', `'k'`) = 'kx';
SELECT count() FROM t_bf_order WHERE concat('k', `'k'`) IN ('kx');
SELECT count() FROM t_bf_order WHERE has(arrayMap(x -> concat(x, 'k', `'k'`), arr), 'akx');

SELECT count() FROM t_bf_order WHERE has(mapKeys(map(`'k'`, 'k')), 'x') SETTINGS force_data_skipping_indices = 'idx_map';
SELECT count() FROM t_bf_order WHERE concat(`'k'`, 'k') = 'xk' SETTINGS force_data_skipping_indices = 'idx_concat';
SELECT count() FROM t_bf_order WHERE has(arrayMap(x -> concat(x, `'k'`, 'k'), arr), 'axk') SETTINGS force_data_skipping_indices = 'idx_lambda';

-- The same for `JSON` subcolumns.
CREATE TABLE t_bf_json
(
    j JSON,
    `JSONAllPaths(j)` Array(String),
    INDEX idx `JSONAllPaths(j)` TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_json VALUES ('{"a":1}', ['x']);

SELECT count() FROM t_bf_json WHERE j.a = 1;
SELECT count() FROM t_bf_json WHERE j.a::Int64 = 1;
SELECT count() FROM t_bf_json WHERE isNotNull(j.a);
SELECT count() FROM t_bf_json WHERE has(`JSONAllPaths(j)`, 'x') SETTINGS force_data_skipping_indices = 'idx';

-- A column named like a `JSON` subcolumn is not that subcolumn.
CREATE TABLE t_bf_json_shadow
(
    j JSON,
    `j.a` Nullable(String),
    `j.t` Tuple(c String),
    `j.n` JSON,
    INDEX idx JSONAllPaths(j) TYPE bloom_filter,
    INDEX idx_n JSONAllPaths(`j.n`) TYPE bloom_filter
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bf_json_shadow VALUES ('{"b":1,"n":{"c":1}}', 'v', tuple('v'), '{"d":1}');

SELECT count() FROM t_bf_json_shadow WHERE `j.a` = 'v';
SELECT count() FROM t_bf_json_shadow WHERE `j.a` IN ('v');
SELECT count() FROM t_bf_json_shadow WHERE isNotNull(`j.a`) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_bf_json_shadow WHERE j.t.c = 'v';
SELECT count() FROM t_bf_json_shadow WHERE j.n.c = 1;
SELECT count() FROM t_bf_json_shadow WHERE j.b = 1 SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_bf_fixed;
DROP TABLE t_bf_lc;
DROP TABLE t_bf_real;
DROP TABLE t_bf_array;
DROP TABLE t_bf_lower;
DROP TABLE t_bf_order;
DROP TABLE t_bf_json;
DROP TABLE t_bf_json_shadow;
