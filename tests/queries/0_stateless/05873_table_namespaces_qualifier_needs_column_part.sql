-- A table name qualifies a column only if a part of the identifier is left for the column.

SET enable_table_namespaces = 1;
SET analyzer_compatibility_prefer_alias_over_subcolumn = 0;
SET single_join_prefer_left_table = 1;

DROP TABLE IF EXISTS `ns.t`;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS `ns2.t`;
DROP TABLE IF EXISTS u2;
DROP TABLE IF EXISTS u3;
CREATE TABLE `ns.t` (ns Tuple(t UInt64)) ENGINE = Memory;
CREATE TABLE u (t UInt64) ENGINE = Memory;
CREATE TABLE `ns2.t` (x UInt64) ENGINE = Memory;
CREATE TABLE u2 (ns2 Tuple(t UInt64)) ENGINE = Memory;
CREATE TABLE u3 (ns2 Tuple(t Tuple(x UInt64))) ENGINE = Memory;
INSERT INTO `ns.t` VALUES (tuple(8));
INSERT INTO u VALUES (5);
INSERT INTO `ns2.t` VALUES (1);
INSERT INTO u2 VALUES (tuple(7));
INSERT INTO u3 VALUES (tuple(tuple(9)));

SELECT '-- 1. a subcolumn named like the table and an alias: both are columns';
SELECT ns.t FROM `ns.t` AS o JOIN u AS ns ON 1 SETTINGS single_join_prefer_left_table = 0; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT ns.t FROM `ns.t` AS o JOIN u AS ns ON 1;

SELECT '-- 2. the same with a comma join';
SELECT ns.t FROM `ns.t` AS o, u AS ns SETTINGS single_join_prefer_left_table = 0; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT ns.t FROM `ns.t` AS o, u AS ns;

SELECT '-- 3. the alias on the left';
SELECT ns.t FROM u AS ns JOIN `ns.t` AS o ON 1 SETTINGS single_join_prefer_left_table = 0; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT ns.t FROM u AS ns JOIN `ns.t` AS o ON 1;

SELECT '-- 4. the alias is preferred with the compatibility setting';
SELECT ns.t FROM `ns.t` AS o JOIN u AS ns ON 1 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT '-- 5. a table named like a subcolumn of the other side does not hide it';
SELECT ns2.t FROM `ns2.t` AS o JOIN u2 AS u ON 1 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT ns2.t FROM `ns2.t` AS o JOIN u2 AS u ON 1;

SELECT '-- 6. a table path followed by a column still qualifies';
SELECT ns2.t.x FROM `ns2.t` AS o JOIN u2 AS u ON 1 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT ns2.t.x FROM `ns2.t` AS o JOIN u2 AS u ON 1;
SELECT `ns2.t`.x FROM `ns2.t` AS o JOIN u2 AS u ON 1;

SELECT '-- 7. a table path binds its side although the other side has such a subcolumn';
SELECT ns2.t.x FROM u3 JOIN `ns2.t` AS o ON 1 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE `ns.t`;
DROP TABLE u;
DROP TABLE `ns2.t`;
DROP TABLE u2;
DROP TABLE u3;
