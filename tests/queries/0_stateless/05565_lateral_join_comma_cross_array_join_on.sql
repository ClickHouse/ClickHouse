-- `JOIN LATERAL`: the comma and `CROSS JOIN` forms, `arrayJoin` inside the lateral subquery,
-- a non-trivial `ON`, and a left side that is itself a join.

SET enable_lateral_join = 1;

DROP TABLE IF EXISTS outer_t;
DROP TABLE IF EXISTS inner_t;
CREATE TABLE outer_t (id UInt32, owner UInt32, tags String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE inner_t (id UInt32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO outer_t VALUES (1, 10, 'a,b'), (2, 20, ''), (3, 30, 'c'), (4, 10, 'a,b');
INSERT INTO inner_t VALUES (10, 'x'), (10, 'y'), (20, 'z');

SELECT '-- comma';
SELECT t.id, s.c FROM values('id Int32, owner Int32', (1, 10), (2, 20)) t,
    LATERAL (SELECT count() AS c FROM values('id Int32, name String', (10, 'a'), (20, 'b')) u WHERE u.id = t.owner) s
ORDER BY 1;
SELECT o.id, s.name FROM outer_t AS o, LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ORDER BY ALL;

SELECT '-- cross';
SELECT t.id, s.c FROM values('id Int32, owner Int32', (1, 10), (2, 20)) t
    CROSS JOIN LATERAL (SELECT count() AS c FROM values('id Int32, name String', (10, 'a'), (20, 'b')) u WHERE u.id = t.owner) s
ORDER BY 1;
SELECT o.id, s.name FROM outer_t AS o CROSS JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ORDER BY ALL;

SELECT '-- formatting';
SELECT formatQuerySingleLine('SELECT o.id FROM outer_t AS o, LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s');
SELECT formatQuerySingleLine('SELECT o.id FROM outer_t AS o CROSS JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s');
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT o.id FROM outer_t AS o, LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s'));
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT o.id FROM outer_t AS o CROSS JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s'));

SELECT o.id FROM outer_t AS o CROSS JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON true; -- { clientError SYNTAX_ERROR }
SELECT o.id FROM outer_t AS o, LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s SETTINGS enable_lateral_join = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- left side is a join';
SELECT o.id, s.c FROM outer_t AS o JOIN inner_t AS i ON o.owner = i.id
    JOIN LATERAL (SELECT count() AS c FROM inner_t AS j WHERE j.name <= i.name) AS s ON true
ORDER BY ALL;
SELECT o.id, s.c FROM outer_t AS o, inner_t AS i, LATERAL (SELECT count() AS c FROM inner_t AS j WHERE j.name <= i.name) AS s
WHERE o.owner = i.id
ORDER BY ALL;
SELECT o.id, tag, s.c FROM outer_t AS o ARRAY JOIN splitByChar(',', o.tags) AS tag
    JOIN LATERAL (SELECT count() AS c FROM inner_t AS i WHERE i.id = o.owner) AS s ON true
ORDER BY ALL;

SELECT '-- arrayJoin';
SELECT t.id, s.x FROM values('id Int32, owner Int32', (1, 10), (2, 20)) t
    JOIN LATERAL (SELECT arrayJoin([t.id, t.owner]) AS x) s ON TRUE
ORDER BY ALL;
SELECT o.id, s.x FROM outer_t AS o JOIN LATERAL (SELECT arrayJoin([o.id, o.owner]) AS x) AS s ON true ORDER BY ALL;
SELECT o.id, s.tag FROM outer_t AS o
    LEFT JOIN LATERAL (SELECT unnest(arrayFilter(x -> x != '', splitByChar(',', o.tags))) AS tag) AS s ON true
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.tag FROM outer_t AS o
    JOIN LATERAL (SELECT unnest(arrayFilter(x -> x != '', splitByChar(',', o.tags))) AS tag) AS s ON true
ORDER BY ALL;
SELECT o.id, s.tag FROM outer_t AS o, LATERAL (SELECT unnest(splitByChar(',', o.tags)) AS tag) AS s ORDER BY ALL;
SELECT o.id, s.n FROM outer_t AS o JOIN LATERAL (SELECT sum(arrayJoin(range(o.id))) AS n) AS s ON true ORDER BY ALL;
SELECT o.id FROM outer_t AS o JOIN LATERAL (SELECT arrayJoin(range(rand() % 3 + o.id)) AS x) AS s ON true; -- { serverError NOT_IMPLEMENTED }

SELECT '-- non-trivial ON';
SELECT t.id, s.c FROM values('id Int32, owner Int32', (1, 10), (2, 20), (3, 30)) t
    LEFT JOIN LATERAL (SELECT count() AS c FROM values('id Int32, name String', (10, 'a'), (20, 'b')) u WHERE u.id = t.owner) s ON s.c > 0
ORDER BY 1 SETTINGS join_use_nulls = 1;
SELECT t.id, s.c FROM values('id Int32, owner Int32', (1, 10), (2, 20), (3, 30)) t
    LEFT JOIN LATERAL (SELECT count() AS c FROM values('id Int32, name String', (10, 'a'), (20, 'b')) u WHERE u.id = t.owner) s ON s.c > 1
ORDER BY 1 SETTINGS join_use_nulls = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name != 'x'
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name != 'x'
ORDER BY ALL;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON o.id > 1
ORDER BY ALL;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name < if(o.id = 1, 'y', 'z')
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name = 'y' OR o.id = 2
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON false
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON NULL
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON 1 = 0
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON 1
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON 256; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT o.id FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON 'abc'; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON false
ORDER BY ALL;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name IN (SELECT 'y')
ORDER BY ALL SETTINGS join_use_nulls = 1;
SELECT o.id FROM outer_t AS o JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name = 'z' ORDER BY ALL;

SELECT '-- non-trivial ON, other layouts of the decorrelation join';
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name != 'x'
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_default_join_kind = 'left';
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_default_join_kind = 'left';
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name < if(o.id = 1, 'y', 'z')
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_default_join_kind = 'left';
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON false
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_default_join_kind = 'left';

SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name != 'x'
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_use_in_memory_buffer = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_use_in_memory_buffer = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name < if(o.id = 1, 'y', 'z')
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_use_in_memory_buffer = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON false
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_use_in_memory_buffer = 0;

SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON s.name != 'x'
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s
    ON s.name < if(o.id = 1, 'y', 'z')
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_substitute_equivalent_expressions = 0;
SELECT o.id, s.name FROM outer_t AS o LEFT JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id = o.owner) AS s ON false
ORDER BY ALL SETTINGS join_use_nulls = 1, correlated_subqueries_substitute_equivalent_expressions = 0;

SELECT '-- ON over the left side only, the lateral subquery reads the left side';
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id <=> o.owner) AS s ON o.id > 1
ORDER BY ALL;
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id <=> o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS correlated_subqueries_default_join_kind = 'left';
SELECT o.id, s.name FROM outer_t AS o INNER JOIN LATERAL (SELECT name FROM inner_t AS i WHERE i.id <=> o.owner) AS s ON o.id > 1
ORDER BY ALL SETTINGS correlated_subqueries_use_in_memory_buffer = 0;
SELECT o.id, s.n FROM outer_t AS o INNER JOIN LATERAL (SELECT arrayJoin(range(o.id)) AS n) AS s ON o.id > 2 AND o.owner = 10
ORDER BY ALL;
SELECT o.id FROM outer_t AS o INNER JOIN LATERAL (SELECT sum(arrayJoin(range(o.id))) AS n) AS s ON throwIf(true)
SETTINGS correlated_subqueries_default_join_kind = 'left'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE outer_t;
DROP TABLE inner_t;
