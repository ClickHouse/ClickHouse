-- a query comparison of an `Enum` with a string orders by the `Enum` value, so constraint bounds in another order say nothing about it.
-- a substitution must not turn a comparison in one order into a comparison in the other order.

DROP TABLE IF EXISTS t_enum_string_bound;
DROP TABLE IF EXISTS t_enum_number_bound;
DROP TABLE IF EXISTS t_enum_number_equal;
DROP TABLE IF EXISTS t_enum_hint_to_enum;
DROP TABLE IF EXISTS t_enum_hint_from_enum;

SET convert_query_to_cnf = 1, optimize_use_implicit_projections = 0;

CREATE TABLE t_enum_string_bound (s String, CONSTRAINT c CHECK s >= '9') ENGINE = Memory;
CREATE TABLE t_enum_number_bound (e Enum8('9' = 1, 'a' = 2), CONSTRAINT c CHECK e <= 2) ENGINE = Memory;
CREATE TABLE t_enum_number_equal (e Enum8('9' = 1, 'a' = 2), CONSTRAINT c CHECK e = 2) ENGINE = Memory;
CREATE TABLE t_enum_hint_to_enum (e Enum8('9' = 1, 'a' = 2), n UInt8, CONSTRAINT c CHECK e <= n) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t_enum_hint_from_enum (e Enum8('9' = 1, 'a' = 2), n UInt8, CONSTRAINT c CHECK n >= e) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_enum_string_bound VALUES ('9');
INSERT INTO t_enum_number_bound VALUES ('a');
INSERT INTO t_enum_number_equal VALUES ('a');
INSERT INTO t_enum_hint_to_enum VALUES ('a', 3);
INSERT INTO t_enum_hint_from_enum VALUES ('a', 3);

-- '9' < '2' as `Enum` values is 1 < 2, true
SELECT 'string bound', count() FROM t_enum_string_bound WHERE s < CAST('2', 'Enum8(''9'' = 1, ''2'' = 2)') SETTINGS optimize_using_constraints = 0;
SELECT 'string bound', count() FROM t_enum_string_bound WHERE s < CAST('2', 'Enum8(''9'' = 1, ''2'' = 2)') SETTINGS optimize_using_constraints = 1;
-- 'a' < '9' is 2 < 1, false
SELECT 'number bound', count() FROM t_enum_number_bound WHERE e < '9' SETTINGS optimize_using_constraints = 0;
SELECT 'number bound', count() FROM t_enum_number_bound WHERE e < '9' SETTINGS optimize_using_constraints = 1;
SELECT 'number equality', count() FROM t_enum_number_equal WHERE e < '9' SETTINGS optimize_using_constraints = 0;
SELECT 'number equality', count() FROM t_enum_number_equal WHERE e < '9' SETTINGS optimize_using_constraints = 1;
-- n <= '9' is 3 <= 9, the hint must not prune by e <= '9', which is e <= 1
SELECT 'hint to enum', count() FROM t_enum_hint_to_enum WHERE n <= '9' SETTINGS optimize_using_constraints = 1, optimize_append_index = 0;
SELECT 'hint to enum', count() FROM t_enum_hint_to_enum WHERE n <= '9' SETTINGS optimize_using_constraints = 1, optimize_append_index = 1;
-- e >= '9' is 2 >= 1, the hint must not prune by n >= '9', which is n >= 9
SELECT 'hint from enum', count() FROM t_enum_hint_from_enum WHERE e >= '9' SETTINGS optimize_using_constraints = 1, optimize_append_index = 0;
SELECT 'hint from enum', count() FROM t_enum_hint_from_enum WHERE e >= '9' SETTINGS optimize_using_constraints = 1, optimize_append_index = 1;

DROP TABLE t_enum_string_bound;
DROP TABLE t_enum_number_bound;
DROP TABLE t_enum_number_equal;
DROP TABLE t_enum_hint_to_enum;
DROP TABLE t_enum_hint_from_enum;
