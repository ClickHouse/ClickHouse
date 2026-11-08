-- `Bool IN` with a non-constant tuple compares integer elements by value, like `=` and the constant `IN` set.

SELECT groupArray(number) FROM (SELECT number, CAST(number % 2 = 1, 'Bool') AS b FROM numbers(4))
WHERE b IN (toUInt8(number), toUInt8(number + 10));
SELECT groupArray(number) FROM (SELECT number, CAST(number % 2 = 1, 'Bool') AS b FROM numbers(4))
WHERE b NOT IN (toUInt8(number), toUInt8(number + 10));

SELECT
    CAST(true, 'Bool') IN (materialize(toUInt8(2)), toUInt8(3)),
    CAST(true, 'Bool') IN (materialize(toUInt8(1)), toUInt8(3)),
    CAST(false, 'Bool') IN (materialize(toUInt8(0)), toUInt8(3)),
    CAST(true, 'Bool') NOT IN (materialize(toUInt8(2)), toUInt8(3)),
    CAST(true, 'Bool') GLOBAL IN (materialize(toUInt8(2)), toUInt8(3)),
    CAST(true, 'Bool') IN (CAST(materialize(false), 'Bool'), materialize(toUInt8(2))),
    CAST(true, 'Bool') IN (materialize(toNullable(toUInt8(2))), toUInt8(3)),
    CAST(true, 'Nullable(Bool)') IN (materialize(toUInt8(2)), NULL),
    nullIn(CAST(true, 'Bool'), (materialize(toNullable(toUInt8(2))), toUInt8(3)));

SELECT CAST(true, 'Bool') IN (materialize(toNullable(toUInt8(2))), toUInt8(3)) SETTINGS transform_null_in = 1;

SELECT
    toLowCardinality(CAST(true, 'Bool')) IN (materialize(toUInt8(2)), toUInt8(3)),
    CAST(true, 'Bool') IN (materialize(toLowCardinality(toUInt8(2))), toUInt8(3))
SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT
    (CAST(true, 'Nullable(Bool)'), 1) IN ((materialize(toUInt8(2)), 1), (toUInt8(3), 1)),
    [CAST(true, 'Nullable(Bool)')] IN ([materialize(toUInt8(2))], [toUInt8(3)]),
    map('a', CAST(true, 'Nullable(Bool)')) IN (map('a', materialize(toUInt8(2))), map('a', toUInt8(3)));

DROP TABLE IF EXISTS t_bool_in;
CREATE TABLE t_bool_in (b Bool, u UInt8, v UInt8, m UInt8 MATERIALIZED b IN (u, v)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bool_in (b, u, v) VALUES (true, 2, 3), (true, 1, 3), (false, 0, 5), (false, 2, 3);
SELECT b, u, v, m, b IN (u), b IN (u, v) FROM t_bool_in ORDER BY u, v, b;
SELECT count() FROM t_bool_in WHERE b IN (u, v);
DROP TABLE t_bool_in;
