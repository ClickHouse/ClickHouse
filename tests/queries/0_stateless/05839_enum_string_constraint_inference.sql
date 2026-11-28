-- a comparison of an `Enum` with a string orders by the `Enum` value, a comparison of two strings by name.
-- constraint inference must not chain the two orders or put an `Enum` and a string in one equality class.

DROP TABLE IF EXISTS t_enum_chain;
DROP TABLE IF EXISTS t_enum_hint;
DROP TABLE IF EXISTS t_enum_hint_eq;
DROP TABLE IF EXISTS t_enum_const_eq;
DROP TABLE IF EXISTS t_enum_const_bound;

SET convert_query_to_cnf = 1, optimize_using_constraints = 1, optimize_use_implicit_projections = 0;

CREATE TABLE t_enum_chain (e Enum8('z' = 5, 'a' = 9, 'm' = 1), s String, CONSTRAINT c CHECK e <= s AND s <= 'm') ENGINE = Memory;
CREATE TABLE t_enum_hint (e Enum8('z' = 5, 'a' = 9, 'm' = 1), s String, CONSTRAINT c CHECK e <= s) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 1;
CREATE TABLE t_enum_hint_eq (e Enum8('z' = 5, 'a' = 9, 'm' = 1), s String, CONSTRAINT c CHECK e = s) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 1;
CREATE TABLE t_enum_const_eq (e Enum8('z' = 5, 'a' = 9, 'm' = 1), CONSTRAINT c CHECK e = 'a') ENGINE = Memory;
CREATE TABLE t_enum_const_bound (e Enum8('z' = 5, 'a' = 9, 'm' = 1), CONSTRAINT c CHECK e <= 'a') ENGINE = Memory;

INSERT INTO t_enum_chain VALUES ('z', 'a');
INSERT INTO t_enum_hint VALUES ('z', 'a');
INSERT INTO t_enum_hint_eq VALUES ('a', 'a');
INSERT INTO t_enum_const_eq VALUES ('a');
INSERT INTO t_enum_const_bound VALUES ('a');

-- 5 <= 1 is false, the constraints do not imply it
SELECT 'chain', count() FROM t_enum_chain WHERE e <= 'm';
-- 'a' <= 'm' is true, the hint must not prune by e <= 1
SELECT 'hint', count() FROM t_enum_hint WHERE s <= 'm' SETTINGS optimize_append_index = 1;
SELECT 'hint equality', count() FROM t_enum_hint_eq WHERE s <= 'm' SETTINGS optimize_append_index = 1;
-- 9 < 1 is false, the constant 'a' must not replace e
SELECT 'constant equality', count() FROM t_enum_const_eq WHERE e < 'm';
-- 9 <= 5 is false, the bound 'a' is not below 'z' by value
SELECT 'constant bound', count() FROM t_enum_const_bound WHERE e <= 'z';

DROP TABLE t_enum_chain;
DROP TABLE t_enum_hint;
DROP TABLE t_enum_hint_eq;
DROP TABLE t_enum_const_eq;
DROP TABLE t_enum_const_bound;
