-- `unnest(...)` as a table source in FROM and JOIN, as in PostgreSQL: https://github.com/ClickHouse/ClickHouse/issues/129645

DROP TABLE IF EXISTS t_unnest;
DROP TABLE IF EXISTS t_unnest_binder;
DROP TABLE IF EXISTS t_unnest_element;
DROP TABLE IF EXISTS t_unnest_tuple;
DROP VIEW IF EXISTS v_unnest;
DROP VIEW IF EXISTS mv_unnest;
DROP VIEW IF EXISTS v_unnest_ord;
DROP VIEW IF EXISTS v_unnest_names;
DROP VIEW IF EXISTS v_unnest_names_star;
DROP VIEW IF EXISTS v_unnest_names_dot;
DROP VIEW IF EXISTS v_unnest_names_dot_star;
DROP VIEW IF EXISTS rmv_unnest;
DROP TABLE IF EXISTS src_unnest;
DROP TABLE IF EXISTS dst_unnest;
DROP TABLE IF EXISTS ctas_unnest;
DROP TABLE IF EXISTS rmv_dst_unnest;

CREATE TABLE t_unnest (id UInt32, arr Array(UInt32), tags Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unnest VALUES (1, [10, 20], ['a', 'b']), (2, [], ['c']), (3, [30], []);

SELECT '-- issue examples';
SELECT x FROM unnest([1, 2, 3]) AS x ORDER BY ALL;
SELECT t.id, x FROM values('id Int32', 1, 2) t CROSS JOIN unnest([1, 2]) AS x ORDER BY ALL;
SELECT t.id, x FROM values('id Int32', 1, 2) t LEFT JOIN unnest([1, 2]) AS x ON x > 1 ORDER BY ALL;
SELECT x, n FROM unnest(['a', 'b']) WITH ORDINALITY AS u(x, n) ORDER BY ALL;

SELECT '-- standalone';
SELECT * FROM unnest([1, 2]) ORDER BY ALL;
SELECT * FROM UNNEST([1, 2]) ORDER BY ALL;
DESCRIBE (SELECT * FROM unnest([1, 2]));
DESCRIBE (SELECT * FROM unnest([1, 2]) AS x);
SELECT unnest.unnest FROM unnest([1, 2]) ORDER BY ALL;
SELECT u.x FROM unnest([1, 2]) AS u(x) ORDER BY ALL;
SELECT u.* FROM unnest([1, 2]) AS u(x) ORDER BY ALL;
SELECT x.x FROM unnest([1, 2]) AS x ORDER BY ALL;
SELECT count() FROM unnest(CAST([], 'Array(UInt8)'));
SELECT * FROM unnest([[1, 2], [3]]) ORDER BY ALL;
SELECT a, b FROM unnest([1, 2], ['a', 'b']) AS u(a, b) ORDER BY ALL;
DESCRIBE (SELECT * FROM unnest([1, 2], ['a', 'b']));
DESCRIBE (SELECT * FROM unnest(['a', 'b']) WITH ORDINALITY);
DESCRIBE (SELECT * FROM unnest(['a', 'b']) WITH ORDINALITY AS u(x));
DESCRIBE (SELECT * FROM unnest(['a', 'b']) WITH ORDINALITY AS t);
SELECT * FROM unnest([1, 2], ['a']) ORDER BY ALL SETTINGS enable_unaligned_array_join = 1;
SELECT * FROM unnest([1, 2], ['a']) WITH ORDINALITY ORDER BY ALL SETTINGS enable_unaligned_array_join = 1;
SELECT a.x, b.y FROM unnest([1, 2, 3]) AS a(x) JOIN unnest([2, 3, 4]) AS b(y) ON a.x = b.y ORDER BY ALL;

SELECT '-- errors';
SELECT * FROM unnest(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM unnest(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM unnest([1]) AS u(a, b); -- { serverError BAD_ARGUMENTS }
SELECT * FROM unnest([1, 2], [3]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT * FROM numbers(2) WITH ORDINALITY; -- { serverError NOT_IMPLEMENTED }
SELECT * FROM unnest([1]) FINAL; -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM unnest([1], [2]) AS u(x, x); -- { serverError BAD_ARGUMENTS }
SELECT * FROM unnest([1]) WITH ORDINALITY AS ordinality; -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_unnest AS t, unnest(t.arr, t.arr) AS u(x, x); -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_unnest AS u, unnest(u.arr) AS u; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM unnest([1]) AS u, unnest([2]) AS u; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM t_unnest AS t, unnest(t.arr), unnest(t.tags); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM t_unnest AS t, unnest(t.arr) AS u(x) WHERE 10 IN u; -- { serverError UNKNOWN_IDENTIFIER }
SELECT * FROM t_unnest AS t, unnest(t.arr) AS u(x) JOIN u ON 1; -- { serverError UNKNOWN_TABLE }
SELECT * FROM t_unnest AS t, unnest(*); -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_unnest AS t CROSS JOIN unnest(COLUMNS('arr')) AS u(x); -- { serverError BAD_ARGUMENTS }
SELECT * FROM unnest(*); -- { serverError BAD_ARGUMENTS }
SELECT * FROM t_unnest AS t, unnest(t.arr, SETTINGS max_threads = 1); -- { serverError BAD_ARGUMENTS }
SELECT one.x FROM system.one, unnest([1]) AS one(x); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT * FROM t_unnest, unnest(t_unnest.arr) AS t_unnest; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON length(tuple(*)) = 4; -- { serverError NOT_IMPLEMENTED }
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON tuple(u.*) = tuple(20); -- { serverError NOT_IMPLEMENTED }
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS u(x) ON u.COLUMNS('x') > 15; -- { serverError NOT_IMPLEMENTED }
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON arrayExists(e -> e = COLUMNS('^x$'), [20]); -- { serverError NOT_IMPLEMENTED }
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON EXISTS (SELECT 1 WHERE u.x > 10); -- { serverError UNKNOWN_IDENTIFIER }
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON EXISTS (SELECT 1 WHERE x > 10); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
WITH tuple(*) AS cols SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON cols.1 = 1; -- { serverError NOT_IMPLEMENTED }
WITH tuple(u.*) AS cols SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON cols.1 = 20; -- { serverError NOT_IMPLEMENTED }
SELECT id, x FROM t_unnest AS t, unnest(arrayMap(e -> tuple(*).1, t.arr)) AS u(x); -- { serverError NOT_IMPLEMENTED }
WITH u.x AS y SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON y > 10; -- { serverError UNKNOWN_IDENTIFIER }
WITH id AS k SELECT t.id, id FROM t_unnest AS t JOIN unnest(t.arr) AS u(id) ON k > 1; -- { serverError NOT_IMPLEMENTED }
SELECT t.id, id, id + 0 AS k FROM t_unnest AS t JOIN unnest(t.arr) AS u(id) ON k > 1; -- { serverError NOT_IMPLEMENTED }
WITH id AS k SELECT t.id, x, id FROM t_unnest AS t JOIN unnest(t.arr) WITH ORDINALITY AS u(x, id) ON k = 1; -- { serverError NOT_IMPLEMENTED }
WITH id AS a, a + 0 AS k SELECT t.id, id FROM t_unnest AS t JOIN unnest(t.arr) AS u(id) ON k > 1; -- { serverError NOT_IMPLEMENTED }

SELECT '-- lateral';
SELECT id, x FROM t_unnest AS t, unnest(t.arr) AS x ORDER BY ALL;
SELECT id, x FROM t_unnest AS t CROSS JOIN unnest(t.arr) AS x ORDER BY ALL;
SELECT id, x FROM t_unnest AS t, LATERAL unnest(t.arr) AS x ORDER BY ALL;
SELECT id, x FROM t_unnest AS t CROSS JOIN LATERAL unnest(t.arr) AS x ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS x ON true ORDER BY ALL;
SELECT id, x FROM t_unnest AS t INNER JOIN unnest(t.arr) AS x ON 1 ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN LATERAL unnest(t.arr) AS x ON TRUE ORDER BY ALL;
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS x ON true ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x FROM t_unnest AS t LEFT JOIN LATERAL unnest(t.arr) AS x ON true ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS x ON true ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT id, x FROM t_unnest AS t LEFT JOIN LATERAL unnest(t.arr) AS x ON true ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT id, x, toTypeName(x) FROM t_unnest AS t LEFT JOIN unnest(if(t.id = 1, [[1, 2]], [])) AS x ON true ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT id, u.x FROM t_unnest AS t, unnest(t.arr) AS u(x) WHERE u.x > 10 ORDER BY ALL;
SELECT u.x % 20 AS k, count() FROM t_unnest AS t, unnest(t.arr) AS u(x) GROUP BY u.x % 20 ORDER BY ALL;
SELECT id, u.x FROM t_unnest AS t, unnest(t.arr) AS u(x) ORDER BY u.x DESC;
SELECT * FROM t_unnest AS t, unnest(t.tags) AS u(tag) ORDER BY ALL;
DESCRIBE (SELECT * FROM t_unnest AS t, unnest(t.tags) AS u(tag));
DESCRIBE (SELECT * FROM t_unnest AS t, unnest(t.arr) AS u(id));
SELECT t.id, u.id, id FROM t_unnest AS t, unnest(t.arr) AS u(id) ORDER BY ALL;
SELECT u.* FROM t_unnest AS t, unnest(t.tags) AS u(tag) ORDER BY ALL;
SELECT t.* FROM t_unnest AS t, unnest(t.tags) AS u(tag) ORDER BY ALL;
SELECT COLUMNS('ta') FROM t_unnest AS t, unnest(t.tags) AS u(tag) ORDER BY ALL;
SELECT id, x, tag FROM t_unnest AS t, unnest(t.arr, t.tags) AS u(x, tag) ORDER BY ALL; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT id, x, tag FROM t_unnest AS t, unnest(t.arr, t.tags) AS u(x, tag) ORDER BY ALL SETTINGS enable_unaligned_array_join = 1;
SELECT id, x, n FROM t_unnest AS t, unnest(t.arr) WITH ORDINALITY AS u(x, n) ORDER BY ALL;
SELECT id, x, tag, n FROM t_unnest AS t, unnest(t.arr, t.tags) WITH ORDINALITY AS u(x, tag, n) ORDER BY ALL SETTINGS enable_unaligned_array_join = 1;
SELECT id, u.x.1, u.x.b FROM t_unnest AS t, unnest(arrayMap(e -> CAST((e, toString(e)), 'Tuple(a UInt32, b String)'), t.arr)) AS u(x) ORDER BY ALL;

SELECT '-- on';
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS x ON x > 15 ORDER BY ALL;
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS x ON x > 15 ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS x ON t.id = 1 ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x, n FROM t_unnest AS t LEFT JOIN unnest(t.arr) WITH ORDINALITY AS u(x, n) ON u.x > 15 ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x FROM t_unnest AS t LEFT JOIN unnest(t.arr) AS x ON false ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS x ON false ORDER BY ALL;
SELECT id, tag FROM t_unnest AS t JOIN unnest(t.tags) AS tag ON tag IS NOT NULL ORDER BY ALL;
SELECT id, u.tag FROM t_unnest AS t JOIN unnest(t.tags) AS u(tag) ON u.tag = t.tags[1] ORDER BY ALL;
SELECT id, tag FROM t_unnest AS t JOIN unnest(t.tags) AS tag ON tag IN (SELECT 'a') ORDER BY ALL;
SELECT id, x, tag FROM t_unnest AS t JOIN unnest(t.arr, t.tags) AS u(x, tag) ON u.x > 0 OR u.tag != '' ORDER BY ALL SETTINGS enable_unaligned_array_join = 1;
-- A lambda in `ON` cannot capture the unnest column: `u.x` stays the element, `x` is the lambda parameter.
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON arrayExists(x -> u.x = x, [10]) ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON arrayExists(u -> u = x, [20]) ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON arrayExists(u -> u.x = x, [CAST(tuple(20), 'Tuple(x UInt32)')]) ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON tuple(t.*).1 = 1 ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON x IN (SELECT * FROM values('v UInt32', 20, 30)) ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON COLUMNS(x) > 15 ORDER BY ALL;
SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON arrayExists(u -> tuple(u.*).1 = x, [CAST(tuple(20), 'Tuple(x UInt32)')]) ORDER BY ALL;
WITH 15 AS lim SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON x > lim ORDER BY ALL;
SELECT id, a, tag FROM t_unnest AS t ARRAY JOIN arrayConcat(COLUMNS('^arr$')) AS a CROSS JOIN unnest(t.tags) AS tag ORDER BY ALL;
WITH 1 AS lim SELECT t.id, id FROM t_unnest AS t JOIN unnest(t.arr) AS u(id) ON id > lim ORDER BY ALL;
WITH x AS k SELECT id, x FROM t_unnest AS t JOIN unnest(t.arr) AS u(x) ON k > 15 ORDER BY ALL;

CREATE TABLE t_unnest_binder (id UInt32, arr Array(UInt32), __unnest_column_1 UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unnest_binder VALUES (1, [10, 20], 0), (2, [30], 1), (3, [40], 0);
SELECT id, x FROM t_unnest_binder AS t JOIN unnest(t.arr) AS u(x) ON __unnest_column_1 = 0 AND u.x > 15 ORDER BY ALL;

CREATE TABLE t_unnest_element (id UInt32, __unnest_element Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unnest_element VALUES (1, [10, 20]), (2, []);
SELECT id, (SELECT groupArray((x, n)) FROM unnest(__unnest_element) WITH ORDINALITY AS u(x, n)) AS g FROM t_unnest_element ORDER BY id;

SELECT '-- precedence of a Tuple column over the unnest alias, as for a table alias';
CREATE TABLE t_unnest_tuple (id UInt32, u Tuple(x UInt8), arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_unnest_tuple VALUES (1, tuple(7), [1, 2]);
SELECT u.x FROM t_unnest_tuple AS t, unnest(t.arr) AS u(x) ORDER BY ALL SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT u.x FROM t_unnest_tuple AS t, unnest(t.arr) AS u(x) ORDER BY ALL SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT u.* FROM t_unnest_tuple AS t, unnest(t.arr) AS u(x) ORDER BY ALL SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT u.* FROM t_unnest_tuple AS t, unnest(t.arr) AS u(x) ORDER BY ALL SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT * FROM t_unnest_tuple AS t, unnest(t.arr) AS u(x) ORDER BY ALL FORMAT TSVWithNames;

SELECT '-- unnest columns with the same name as other columns';
SELECT * FROM (SELECT * FROM numbers(1) AS t, unnest([1, 2]) AS u(x), unnest([3, 4]) AS v(x)) ORDER BY ALL;
SELECT * FROM (SELECT * FROM values('x UInt8', 1), unnest([2]) AS u(x)) FORMAT TSVWithNames;
SELECT 5 AS x, u.* FROM numbers(1) AS t, unnest([1, 2]) AS u(x) ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM (SELECT * FROM numbers(1) AS t, unnest([1, 2]) AS u(`a.b`), unnest([3, 4]) AS v(`a.b`)) ORDER BY ALL;
SELECT 5 AS `a.b`, u.* FROM numbers(1) AS t, unnest([1, 2]) AS u(`a.b`) ORDER BY ALL FORMAT TSVWithNames;

SELECT '-- the unnest columns on the hidden side of a SEMI JOIN';
SELECT u.x FROM t_unnest AS t, unnest(t.arr) AS u(x) RIGHT SEMI JOIN values('id UInt32', 1, 3) AS s ON s.id = t.id SETTINGS semi_join_include_columns_from_both_sides = 0; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT u.* FROM t_unnest AS t, unnest(t.arr) AS u(x) RIGHT SEMI JOIN values('id UInt32', 1, 3) AS s ON s.id = t.id SETTINGS semi_join_include_columns_from_both_sides = 0; -- { serverError SEMI_ANTI_JOIN_COLUMN_ACCESS_DENIED }
SELECT s.id FROM t_unnest AS t, unnest(t.arr) AS u(x) RIGHT SEMI JOIN values('id UInt32', 1, 3) AS s ON s.id = t.id ORDER BY ALL SETTINGS semi_join_include_columns_from_both_sides = 0;

SELECT '-- chains';
SELECT t.id, x FROM t_unnest AS t JOIN t_unnest AS s ON s.id = t.id, unnest(s.arr) AS x ORDER BY ALL;
SELECT t.id, u.tag, m.v FROM t_unnest AS t, unnest(t.tags) AS u(tag) JOIN (SELECT 'a' AS k, 'A' AS v) AS m ON m.k = u.tag ORDER BY ALL;
SELECT t.id, tag, m.v FROM t_unnest AS t, unnest(t.tags) AS u(tag) JOIN (SELECT 'a' AS tag, 'A' AS v) AS m USING (tag) ORDER BY ALL;
SELECT u.* APPLY toTypeName, toTypeName(u.x) FROM numbers(1) AS t, unnest([1::UInt8]) AS u(x) JOIN (SELECT 1::UInt64 AS x) AS r USING (x);
SELECT u.* APPLY toTypeName, toTypeName(u.x), toTypeName(u.n) FROM numbers(1) AS t, unnest([1::UInt8]) WITH ORDINALITY AS u(x, n) JOIN (SELECT 1::UInt64 AS x) AS r USING (x);
SELECT id, a, b FROM t_unnest AS t, unnest(t.arr) AS a, unnest(t.tags) AS b ORDER BY ALL;
SELECT x FROM unnest([[1, 2], [3]]) AS u(arr), unnest(u.arr) AS x ORDER BY ALL;
SELECT j.x FROM (t_unnest AS t, unnest(t.arr) AS x) AS j ORDER BY ALL;
SELECT * FROM (t_unnest AS t, unnest(t.arr) AS x) AS j ORDER BY ALL;

SELECT '-- other join kinds';
SELECT t.id, x FROM t_unnest AS t RIGHT JOIN unnest([1, 3, 5]) AS x ON t.id = x ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id FROM t_unnest AS t JOIN unnest([1, 3]) AS u(id) USING (id) ORDER BY ALL;
SELECT t.id, x FROM t_unnest AS t RIGHT JOIN unnest(t.arr) AS x ON t.id = x; -- { serverError NOT_IMPLEMENTED }

SELECT '-- correlated';
SELECT id FROM t_unnest AS t WHERE EXISTS (SELECT 1 FROM unnest(t.tags) AS tag WHERE tag = 'b') ORDER BY id;
SELECT id, (SELECT groupArray(x) FROM unnest(t.arr) AS x) AS g FROM t_unnest AS t ORDER BY id;
SELECT arrayMap((x -> x + 1) | (u -> (SELECT u.x FROM numbers(1) AS t, unnest([1]) AS u(x))), [1]);

SELECT '-- distributed';
SELECT id, x FROM remote('127.0.0.{1,2}', currentDatabase(), t_unnest) AS t, unnest(t.arr) AS u(x) WHERE u.x > 10 ORDER BY ALL;
SELECT id, x FROM remote('127.0.0.{1,2}', currentDatabase(), t_unnest) AS t LEFT JOIN unnest(t.arr) AS u(x) ON u.x > 15 ORDER BY ALL SETTINGS join_use_nulls = 0;
SELECT id FROM remote('127.0.0.{1,2}', currentDatabase(), t_unnest) AS t WHERE id IN (SELECT x FROM unnest([1, 3]) AS x) ORDER BY ALL;

SELECT '-- views';
CREATE VIEW v_unnest AS SELECT id, u.x FROM t_unnest AS t, unnest(t.arr) AS u(x);
SELECT * FROM v_unnest ORDER BY ALL;
CREATE TABLE src_unnest (id UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_unnest (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY (id, x);
CREATE MATERIALIZED VIEW mv_unnest TO dst_unnest AS SELECT id, x FROM src_unnest AS s, unnest(s.arr) AS x;
INSERT INTO src_unnest VALUES (1, [5, 6]), (2, []);
SELECT * FROM dst_unnest ORDER BY ALL;
SELECT position(create_table_query, 'unnest(t.arr) AS u(x)') > 0, position(create_table_query, 'HIDDEN')
FROM system.tables WHERE database = currentDatabase() AND name = 'v_unnest';
CREATE VIEW v_unnest_ord AS SELECT x, n FROM UNNEST(['a', 'b']) WITH ORDINALITY AS u(x, n);
SELECT * FROM v_unnest_ord ORDER BY n;
SELECT position(create_table_query, 'UNNEST(') > 0 AND position(create_table_query, 'WITH ORDINALITY AS u(x, n)') > 0,
    position(create_table_query, 'HIDDEN')
FROM system.tables WHERE database = currentDatabase() AND name = 'v_unnest_ord';
CREATE VIEW v_unnest_names AS SELECT u.*, v.* FROM numbers(1) AS t, unnest([1, 2]) AS u(x), unnest([3, 4]) AS v(x);
SELECT * FROM v_unnest_names ORDER BY ALL FORMAT TSVWithNames;
CREATE VIEW v_unnest_names_star AS SELECT * FROM numbers(1) AS t, unnest([1, 2]) AS u(x), unnest([3, 4]) AS v(x);
SELECT * FROM v_unnest_names_star ORDER BY ALL FORMAT TSVWithNames;
CREATE VIEW v_unnest_names_dot AS SELECT u.*, v.* FROM numbers(1) AS t, unnest([1, 2]) AS u(`a.b`), unnest([3, 4]) AS v(`a.b`);
SELECT * FROM v_unnest_names_dot ORDER BY ALL FORMAT TSVWithNames;
CREATE VIEW v_unnest_names_dot_star AS SELECT * FROM numbers(1) AS t, unnest(['p']) WITH ORDINALITY AS u(x, `a.b`), unnest([3, 4]) AS v(`a.b`);
SELECT * FROM v_unnest_names_dot_star ORDER BY ALL FORMAT TSVWithNames;
CREATE TABLE ctas_unnest ENGINE = Memory AS SELECT x FROM unnest([1, 2]) AS x;
SELECT * FROM ctas_unnest ORDER BY x;
ALTER TABLE mv_unnest MODIFY QUERY SELECT id, x FROM src_unnest AS s, unnest(s.arr) AS x WHERE x > 6;
INSERT INTO src_unnest VALUES (3, [6, 7]);
SELECT * FROM dst_unnest ORDER BY ALL;
CREATE TABLE rmv_dst_unnest (x UInt8) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW rmv_unnest REFRESH EVERY 1 YEAR APPEND TO rmv_dst_unnest EMPTY
    AS SELECT x FROM unnest([1, 2]) AS x;
SYSTEM REFRESH VIEW rmv_unnest;
SYSTEM WAIT VIEW rmv_unnest;
SELECT * FROM rmv_dst_unnest ORDER BY x;
SELECT 'refreshed';
SELECT x FROM unnest([7]) AS x FORMAT Null SETTINGS log_comment = '05566_unnest_masking';
SYSTEM FLUSH LOGS query_log;
SELECT position(query, 'unnest([7])') > 0, position(query, 'HIDDEN') FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND log_comment = '05566_unnest_masking'
    AND type = 'QueryFinish' AND is_initial_query;

SELECT '-- syntax';
SELECT formatQuerySingleLine('SELECT x, n FROM unnest([1]) WITH ORDINALITY AS u(x, n)');
SELECT formatQuerySingleLine('SELECT x FROM unnest([1]) WITH ORDINALITY x');
SELECT formatQuerySingleLine('SELECT x FROM t, LATERAL unnest(t.arr) AS x');
SELECT formatQuerySingleLine('SELECT x FROM t LEFT JOIN LATERAL unnest(t.arr) AS x ON true');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT x, n FROM unnest([1]) WITH ORDINALITY AS u(x, n)'));
SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT 1 FROM t'), '"type":"TableExpression"', '"type":"TableExpression","with_ordinality":true')); -- { serverError BAD_ARGUMENTS }
SELECT unnest([1, 2]) AS y ORDER BY y;

DROP VIEW rmv_unnest;
DROP VIEW v_unnest_names_dot_star;
DROP VIEW v_unnest_names_dot;
DROP VIEW v_unnest_names_star;
DROP VIEW v_unnest_names;
DROP VIEW v_unnest_ord;
DROP VIEW mv_unnest;
DROP VIEW v_unnest;
DROP TABLE rmv_dst_unnest;
DROP TABLE ctas_unnest;
DROP TABLE src_unnest;
DROP TABLE dst_unnest;
DROP TABLE t_unnest_tuple;
DROP TABLE t_unnest_element;
DROP TABLE t_unnest_binder;
DROP TABLE t_unnest;
