-- Changing the type of an ALIAS column changes its value (the alias casts to its type), so it is rejected when a
-- MATERIALIZED key column is computed from the alias, which cannot be recalculated in an existing part.

DROP TABLE IF EXISTS t_alias_key;
DROP TABLE IF EXISTS t_alias_not_key;

CREATE TABLE t_alias_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED a % 1000) ENGINE = MergeTree ORDER BY m;
INSERT INTO t_alias_key (x) VALUES (5000000001);
ALTER TABLE t_alias_key MODIFY COLUMN a Int32 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT x, a, m, a % 1000 FROM t_alias_key;

-- An alias that the key column does not read can still change its type.
CREATE TABLE t_alias_not_key (x Int64, a Int64 ALIAS x, m Int64 MATERIALIZED x % 1000) ENGINE = MergeTree ORDER BY m;
ALTER TABLE t_alias_not_key MODIFY COLUMN a Int32;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_alias_not_key' AND name = 'a';

DROP TABLE t_alias_key;
DROP TABLE t_alias_not_key;
