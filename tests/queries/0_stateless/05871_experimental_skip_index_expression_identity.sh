#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The experimental `cuckoo_filter` and `binary_fuse_filter` skip indexes must match a predicate to an
# index column by the expression that computes it, not by its name alone, like `bloom_filter` does
# (see `05620_bloom_filter_index_map_keys_column_name`). Every query below returns 1.

function run_test()
{
    local index_type=$1
    local setting=$2

    $CLICKHOUSE_CLIENT --"$setting"=1 --multiquery <<SQL
-- A column literally named like an expression is not that expression.
DROP TABLE IF EXISTS t_expr;
CREATE TABLE t_expr
(
    s String,
    \`lower(s)\` String,
    INDEX idx \`lower(s)\` TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_expr VALUES ('ABC', 'x');

SELECT count() FROM t_expr WHERE lower(s) = 'abc';
SELECT count() FROM t_expr WHERE lower(s) IN ('abc');
SELECT count() FROM t_expr WHERE \`lower(s)\` = 'x' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_expr WHERE \`lower(s)\` IN ('x') SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_expr;

-- The same for columns named like `mapKeys` / `mapValues` of a map.
DROP TABLE IF EXISTS t_map_names;
CREATE TABLE t_map_names
(
    m Map(String, String),
    \`mapKeys(m)\` Array(String),
    \`mapValues(m)\` Array(String),
    INDEX idx_k \`mapKeys(m)\` TYPE $index_type GRANULARITY 1,
    INDEX idx_v \`mapValues(m)\` TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map_names VALUES (map('k', 'v'), ['x'], ['y']);

SELECT count() FROM t_map_names WHERE m['k'] = 'v';
SELECT count() FROM t_map_names WHERE m['k'] = 'v' SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_names WHERE m['k'] IN ('v');
SELECT count() FROM t_map_names WHERE mapContainsKey(m, 'k');
SELECT count() FROM t_map_names WHERE mapContainsValue(m, 'v');
SELECT count() FROM t_map_names WHERE has(m, 'k');
SELECT count() FROM t_map_names WHERE has(mapKeys(m), 'k') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_names WHERE has(mapValues(m), 'v') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_names WHERE has(\`mapKeys(m)\`, 'x') SETTINGS force_data_skipping_indices = 'idx_k';
SELECT count() FROM t_map_names WHERE has(\`mapValues(m)\`, 'y') SETTINGS force_data_skipping_indices = 'idx_v';
DROP TABLE t_map_names;

-- A real `mapKeys(m)` index is still used.
DROP TABLE IF EXISTS t_map_real;
CREATE TABLE t_map_real
(
    m Map(String, String),
    INDEX idx mapKeys(m) TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map_real VALUES (map('k', 'v'));

SELECT count() FROM t_map_real WHERE m['k'] = 'v' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM t_map_real WHERE mapContainsKey(m, 'k') SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_map_real;

-- Same columns and names, different argument order.
DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order
(
    \`'k'\` String,
    INDEX idx_concat concat(\`'k'\`, 'k') TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_order VALUES ('x');

SELECT count() FROM t_order WHERE concat('k', \`'k'\`) = 'kx';
SELECT count() FROM t_order WHERE concat('k', \`'k'\`) IN ('kx');
SELECT count() FROM t_order WHERE concat(\`'k'\`, 'k') = 'xk' SETTINGS force_data_skipping_indices = 'idx_concat';
DROP TABLE t_order;

-- A column named like `JSONAllPaths` of a `JSON` column.
DROP TABLE IF EXISTS t_json_names;
CREATE TABLE t_json_names
(
    j JSON,
    \`JSONAllPaths(j)\` Array(String),
    INDEX idx \`JSONAllPaths(j)\` TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_names VALUES ('{"a":1}', ['x']);

SELECT count() FROM t_json_names WHERE j.a = 1;
SELECT count() FROM t_json_names WHERE j.a::Int64 = 1;
SELECT count() FROM t_json_names WHERE j.a::Int64 IN (1);
SELECT count() FROM t_json_names WHERE isNotNull(j.a);
SELECT count() FROM t_json_names WHERE has(\`JSONAllPaths(j)\`, 'x') SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_json_names;

-- A column, or a subcolumn of another column, named like a `JSON` subcolumn is not that subcolumn.
DROP TABLE IF EXISTS t_json_shadow;
CREATE TABLE t_json_shadow
(
    j JSON,
    \`j.a\` Nullable(String),
    \`j.t\` Tuple(c String),
    INDEX idx JSONAllPaths(j) TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_shadow VALUES ('{"b":1}', 'v', tuple('v'));

SELECT count() FROM t_json_shadow WHERE \`j.a\` = 'v';
SELECT count() FROM t_json_shadow WHERE \`j.a\` IN ('v');
SELECT count() FROM t_json_shadow WHERE isNotNull(\`j.a\`) SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_json_shadow WHERE j.t.c = 'v';
SELECT count() FROM t_json_shadow WHERE j.b = 1 SETTINGS force_data_skipping_indices = 'idx';
DROP TABLE t_json_shadow;

-- A missing map key gives the default of the lookup type, here \`UInt64\` 0, which the \`String\` set
-- matches as '0'. The keys index must not be used to reject the granule.
DROP TABLE IF EXISTS t_map_default;
CREATE TABLE t_map_default
(
    m Map(String, UInt64),
    INDEX idx mapKeys(m) TYPE $index_type GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map_default VALUES (map('present', 1));

SELECT count() FROM t_map_default WHERE m['missing'] IN (SELECT '0') SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_map_default WHERE m['missing'] IN (SELECT '0') SETTINGS optimize_functions_to_subcolumns = 0, use_skip_indexes = 0;
SELECT count() FROM t_map_default WHERE m['missing'] IN (SELECT '0') SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_map_default WHERE m['present'] IN (SELECT '1') SETTINGS optimize_functions_to_subcolumns = 0, force_data_skipping_indices = 'idx';
DROP TABLE t_map_default;
SQL
}

run_test cuckoo_filter allow_experimental_cuckoo_filter_index
run_test binary_fuse_filter allow_experimental_binary_fuse_filter_index
