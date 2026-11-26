-- A column named like a subexpression of a skip index expression is not that subexpression, so a filter on it
-- must not be bound to the index column: `upper(`lower(x)`)` looks like `upper(lower(x))` by name only.

SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_set_nested_name;
CREATE TABLE t_set_nested_name (k UInt64, x String, `lower(x)` String, INDEX s upper(lower(x)) TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_set_nested_name VALUES (1, 'B', 'q'), (2, 'A', 'r');

SELECT 'set';
SELECT x FROM t_set_nested_name WHERE upper(`lower(x)`) = 'R' ORDER BY x;
SELECT x FROM t_set_nested_name WHERE upper(`lower(x)`) = 'R' ORDER BY x SETTINGS use_skip_indexes = 0;
SELECT x FROM t_set_nested_name WHERE upper(`lower(x)`) IN ('R', 'Z') ORDER BY x;
SELECT x FROM t_set_nested_name WHERE NOT (upper(`lower(x)`) != 'R') ORDER BY x;
SELECT x FROM t_set_nested_name WHERE upper(lower(x)) = 'A' ORDER BY x;
DROP TABLE t_set_nested_name;

DROP TABLE IF EXISTS t_tokenbf_nested_name;
CREATE TABLE t_tokenbf_nested_name (k UInt64, x String, `lower(x)` String, INDEX s upper(lower(x)) TYPE tokenbf_v1(512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_tokenbf_nested_name VALUES (1, 'B', 'q'), (2, 'A', 'r');

SELECT 'tokenbf_v1';
SELECT x FROM t_tokenbf_nested_name WHERE upper(`lower(x)`) = 'R' ORDER BY x;
SELECT x FROM t_tokenbf_nested_name WHERE hasToken(upper(`lower(x)`), 'R') ORDER BY x;
SELECT x FROM t_tokenbf_nested_name WHERE upper(lower(x)) = 'A' ORDER BY x;
DROP TABLE t_tokenbf_nested_name;

DROP TABLE IF EXISTS t_ngrambf_nested_name;
CREATE TABLE t_ngrambf_nested_name (k UInt64, x String, `lower(x)` String, INDEX s upper(lower(x)) TYPE ngrambf_v1(1, 512, 2, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_ngrambf_nested_name VALUES (1, 'B', 'q'), (2, 'A', 'r');

SELECT 'ngrambf_v1';
SELECT x FROM t_ngrambf_nested_name WHERE upper(`lower(x)`) = 'R' ORDER BY x;
SELECT x FROM t_ngrambf_nested_name WHERE upper(`lower(x)`) LIKE '%R%' ORDER BY x;
SELECT x FROM t_ngrambf_nested_name WHERE upper(lower(x)) = 'A' ORDER BY x;
DROP TABLE t_ngrambf_nested_name;

DROP TABLE IF EXISTS t_text_nested_name;
CREATE TABLE t_text_nested_name (k UInt64, x String, `lower(x)` String, INDEX s upper(lower(x)) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_text_nested_name VALUES (1, 'B', 'q'), (2, 'A', 'r');

SELECT 'text';
SELECT x FROM t_text_nested_name WHERE hasToken(upper(`lower(x)`), 'R') ORDER BY x;
SELECT x FROM t_text_nested_name WHERE hasToken(upper(`lower(x)`), 'R') ORDER BY x SETTINGS query_plan_direct_read_from_text_index = 1;
SELECT x FROM t_text_nested_name WHERE hasToken(upper(`lower(x)`), 'R') ORDER BY x SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT x FROM t_text_nested_name WHERE hasAnyTokens(upper(`lower(x)`), ['R']) ORDER BY x;
SELECT x FROM t_text_nested_name WHERE hasToken(upper(lower(x)), 'A') ORDER BY x;
DROP TABLE t_text_nested_name;
