-- `equals` and `notEquals` on tuples propagate the NULL of a member, while set membership and `has`
-- treat it as a value. The rewrites of comparison chains into `IN` / `NOT IN` and of `arrayExists`
-- into `has` must therefore not apply to tuples with `Nullable` members.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_tuple_nullable;
CREATE TABLE t_tuple_nullable (v Tuple(Nullable(UInt64))) ENGINE = Memory;
INSERT INTO t_tuple_nullable VALUES ((3)), ((NULL)), ((1));

SELECT 'equals chain';
SELECT v, v = tuple(NULL::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64))
FROM t_tuple_nullable ORDER BY v SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT v, v = tuple(NULL::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64))
FROM t_tuple_nullable ORDER BY v SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'equals chain in WHERE';
SELECT count() FROM t_tuple_nullable
WHERE NOT (v = tuple(NULL::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64)))
SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_tuple_nullable
WHERE NOT (v = tuple(NULL::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64)))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'equals chain with non-NULL constants';
SELECT count() FROM t_tuple_nullable
WHERE NOT (v = tuple(5::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64)))
SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT count() FROM t_tuple_nullable
WHERE NOT (v = tuple(5::Nullable(UInt64)) OR v = tuple(1::Nullable(UInt64)) OR v = tuple(2::Nullable(UInt64)))
SETTINGS optimize_min_equality_disjunction_chain_length = 100;

SELECT 'notEquals chain';
SELECT count() FROM t_tuple_nullable
WHERE v != tuple(NULL::Nullable(UInt64)) AND v != tuple(1::Nullable(UInt64)) AND v != tuple(2::Nullable(UInt64))
SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT count() FROM t_tuple_nullable
WHERE v != tuple(NULL::Nullable(UInt64)) AND v != tuple(1::Nullable(UInt64)) AND v != tuple(2::Nullable(UInt64))
SETTINGS optimize_min_inequality_conjunction_chain_length = 100;

DROP TABLE t_tuple_nullable;

SELECT 'arrayExists';
DROP TABLE IF EXISTS t_array_tuple_nullable;
CREATE TABLE t_array_tuple_nullable (a Array(Tuple(Nullable(UInt64)))) ENGINE = Memory;
INSERT INTO t_array_tuple_nullable VALUES ([(NULL)]), ([(1)]);

SELECT a, arrayExists(x -> x = tuple(NULL::Nullable(UInt64)), a) FROM t_array_tuple_nullable ORDER BY a
SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT a, arrayExists(x -> x = tuple(NULL::Nullable(UInt64)), a) FROM t_array_tuple_nullable ORDER BY a
SETTINGS optimize_rewrite_array_exists_to_has = 0;
SELECT count() FROM t_array_tuple_nullable WHERE arrayExists(x -> x = tuple(NULL::Nullable(UInt64)), a)
SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT count() FROM t_array_tuple_nullable WHERE arrayExists(x -> x = tuple(NULL::Nullable(UInt64)), a)
SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE t_array_tuple_nullable;
