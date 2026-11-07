-- Under `analyzer_alias_hides_table_name` the alias of `t AS c` hides the name `t` from a qualifier when an
-- enclosing query gives `t` another reading. A `Nested` value reconstructed from the flattened columns
-- `t.val` of an enclosing table is such a reading for a matcher, the same as with the outer name renamed.
-- Checking for a competing reading uses the declared types of the enclosing table only, so it must not
-- resolve an unused `ALIAS` column there, and a scalar column `t` does not hide the name of `t.val`.

SET enable_analyzer = 1;
SET analyzer_alias_hides_table_name = 1;
SET flatten_nested = 1;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS v;
DROP TABLE IF EXISTS w;
DROP DICTIONARY IF EXISTS d;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'nested prefix';
CREATE TABLE u (t Nested(val UInt64)) ENGINE = Memory;
INSERT INTO u VALUES ([100]);
SELECT (SELECT t.* FROM t AS c LIMIT 1) FROM u SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT (SELECT t.* FROM t AS c LIMIT 1) FROM u SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT (SELECT x.* FROM t AS c LIMIT 1) FROM (SELECT t AS x FROM u) SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM u SETTINGS analyzer_alias_hides_table_name = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'exact column before nested prefix';
-- A column `t` takes precedence over the `Nested` prefix reconstructed from `t.val`, as in normal resolution.
-- The scalar `w.t` cannot be expanded by a matcher, so it is no competing reading for `t.*`.
CREATE TABLE w (t UInt64, `t.val` Array(UInt64)) ENGINE = Memory;
INSERT INTO w VALUES (1, [100]);
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM w SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM w SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SELECT (SELECT t.* FROM t AS c ORDER BY val LIMIT 1) FROM w SETTINGS analyzer_alias_hides_table_name = 0;
DROP TABLE w;

SELECT 'unused storage alias';
CREATE DICTIONARY d (key UInt64, value UInt64) PRIMARY KEY key SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0);
CREATE TABLE v (key UInt64, t UInt64 ALIAS dictGet('d', 'value', key)) ENGINE = Memory;
INSERT INTO v (key) VALUES (1);
DROP DICTIONARY d SETTINGS check_table_dependencies = 0, check_referential_table_dependencies = 0;
SELECT (SELECT count() FROM t AS c WHERE t.val > 15) FROM v;

DROP TABLE v;
DROP TABLE u;
DROP TABLE t;
