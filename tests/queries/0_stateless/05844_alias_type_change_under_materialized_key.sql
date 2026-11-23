-- Changing the type of an ALIAS column changes its value (the alias casts to its type), so it is rejected when a
-- MATERIALIZED key column is computed from the alias, which cannot be recalculated in an existing part.

DROP TABLE IF EXISTS t_alias_key;
DROP TABLE IF EXISTS t_alias_not_key;

CREATE TABLE t_alias_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED a % 1000) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_alias_key (x) VALUES (5000000001);
ALTER TABLE t_alias_key MODIFY COLUMN a Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT x, a, m, a % 1000 FROM t_alias_key;

-- The alias reached through a subcolumn of another MATERIALIZED column (`m.v` of `m MATERIALIZED tuple(a)`).
DROP TABLE IF EXISTS t_alias_key_through_subcolumn;
CREATE TABLE t_alias_key_through_subcolumn
    (x Int64, a Int64 ALIAS x, m Tuple(v Int64) MATERIALIZED tuple(a), k Int64 MATERIALIZED m.v % 1000)
ENGINE = MergeTree ORDER BY k;
ALTER TABLE t_alias_key_through_subcolumn MODIFY COLUMN a Int32; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- An alias that the key column does not read can still change its type.
CREATE TABLE t_alias_not_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED x % 1000) ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_alias_not_key MODIFY COLUMN a Int32;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_alias_not_key' AND name = 'a';

-- A lambda parameter named like the alias is not a reference to it.
DROP TABLE IF EXISTS t_alias_lambda_parameter;
CREATE TABLE t_alias_lambda_parameter (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED arraySum(arrayMap(a -> a + 1, [1, 2])))
ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_alias_lambda_parameter MODIFY COLUMN a Int32;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_alias_lambda_parameter' AND name = 'a';

DROP TABLE t_alias_key;
DROP TABLE t_alias_key_through_subcolumn;
DROP TABLE t_alias_not_key;
DROP TABLE t_alias_lambda_parameter;
