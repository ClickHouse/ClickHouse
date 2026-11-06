-- Reading sparse `String` and `Nullable(String)` columns as full ones, and functions over them, give the same
-- result as for columns that are not sparse: values in the first and last rows, a part without values, function
-- results that are empty for some values, and functions whose result for an empty string is not empty.

DROP TABLE IF EXISTS t_sparse_string;
DROP TABLE IF EXISTS t_full_string;

CREATE TABLE t_sparse_string (id UInt64, s String, n Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse';

CREATE TABLE t_full_string (id UInt64, s String, n Nullable(String))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 1;

SYSTEM STOP MERGES t_sparse_string;

INSERT INTO t_sparse_string SELECT
    number,
    if(number % 7 = 0 OR number = 999, repeat(toString(number % 10), number % 40 + 1), ''),
    if(number % 11 = 0 OR number = 999, repeat('n', number % 5), NULL)
FROM numbers(1000);
INSERT INTO t_sparse_string SELECT number, '', NULL FROM numbers(1000, 500);

INSERT INTO t_full_string SELECT
    number,
    if(number % 7 = 0 OR number = 999, repeat(toString(number % 10), number % 40 + 1), ''),
    if(number % 11 = 0 OR number = 999, repeat('n', number % 5), NULL)
FROM numbers(1000);
INSERT INTO t_full_string SELECT number, '', NULL FROM numbers(1000, 500);

SELECT table, column, groupUniqArray(serialization_kind)
FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_sparse_string', 't_full_string') AND column IN ('s', 'n') AND active
GROUP BY table, column
ORDER BY table, column;

SELECT id, s || 'x', substring(s, 20), ifNull(n, 'NULL') FROM t_sparse_string WHERE id IN (0, 1, 7, 55, 998, 999, 1000) ORDER BY id;

SELECT 'read', (SELECT sum(cityHash64(id, s)) FROM t_sparse_string) = (SELECT sum(cityHash64(id, s)) FROM t_full_string);
SELECT 'concat', (SELECT sum(cityHash64(id, s || 'x')) FROM t_sparse_string) = (SELECT sum(cityHash64(id, s || 'x')) FROM t_full_string);
SELECT 'concat3', (SELECT sum(cityHash64(id, concat('<', s, '>'))) FROM t_sparse_string) = (SELECT sum(cityHash64(id, concat('<', s, '>'))) FROM t_full_string);
SELECT 'substring', (SELECT sum(cityHash64(id, substring(s, 20))) FROM t_sparse_string) = (SELECT sum(cityHash64(id, substring(s, 20))) FROM t_full_string);
SELECT 'upper', (SELECT sum(cityHash64(id, upper(s))) FROM t_sparse_string) = (SELECT sum(cityHash64(id, upper(s))) FROM t_full_string);
SELECT 'nullable', (SELECT sum(cityHash64(ifNull(concat(n, '|', toString(id)), 'NULL'))) FROM t_sparse_string) = (SELECT sum(cityHash64(ifNull(concat(n, '|', toString(id)), 'NULL'))) FROM t_full_string);
SELECT 'nullable_ifnull', (SELECT sum(cityHash64(id, ifNull(n, 'NULL'))) FROM t_sparse_string) = (SELECT sum(cityHash64(id, ifNull(n, 'NULL'))) FROM t_full_string);
SELECT 'nullable_assume', (SELECT sum(cityHash64(id, isNull(n), assumeNotNull(n))) FROM t_sparse_string) = (SELECT sum(cityHash64(id, isNull(n), assumeNotNull(n))) FROM t_full_string);
SELECT 'group_by', (SELECT sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_sparse_string GROUP BY s)) = (SELECT sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_full_string GROUP BY s));
SELECT 'join', count() FROM t_sparse_string AS a INNER JOIN t_full_string AS b ON a.id = b.id AND a.s = b.s;

DROP TABLE t_sparse_string;
DROP TABLE t_full_string;
