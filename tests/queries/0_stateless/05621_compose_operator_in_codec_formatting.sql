-- The `f | g` operator must keep its operator form where operators are otherwise formatted as function calls,
-- because `__compose(f, g)` parses back as an ordinary call to a function of that name.

SELECT formatQuerySingleLine('CREATE TABLE t (c UInt64 CODEC(arrayMap(toString(_) | toString(_), a), LZ4)) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (c UInt64 STATISTICS(arrayMap(toString(_) | toString(_), a))) ENGINE = Memory');
SELECT formatQuerySingleLine('CREATE TABLE t (c UInt64 CODEC(arrayMap(x -> x + 1 | toString(_) | upper(_), a), LZ4)) ENGINE = Memory');

-- An ordinary call to `__compose` stays a call.
SELECT formatQuerySingleLine('CREATE TABLE t (c UInt64 CODEC(arrayMap(__compose(toString(_), toString(_)), a), LZ4)) ENGINE = Memory');

EXPLAIN SYNTAX SELECT arrayMap(toString(_) | upper(_), [1, 2]);

-- Debug builds check that the formatted query parses back to the same AST.
DROP TABLE IF EXISTS t_compose_codec;
CREATE TABLE t_compose_codec (c UInt64 CODEC(arrayMap(toString(_) | toString(_), a), LZ4)) ENGINE = Memory; -- { serverError UNKNOWN_CODEC }
CREATE TABLE t_compose_codec (c UInt64 STATISTICS(arrayMap(toString(_) | toString(_), a))) ENGINE = Memory; -- { serverError INCORRECT_QUERY }
DROP TABLE IF EXISTS t_compose_codec;
