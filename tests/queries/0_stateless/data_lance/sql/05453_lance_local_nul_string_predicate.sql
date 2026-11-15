-- A string constant with a zero byte cannot be passed to `Lance` in a predicate, so the filter must
-- not be pushed down, and the results must be the same as with the pushdown disabled.
DROP TABLE IF EXISTS lance_local_nul_string_predicate;

CREATE TABLE lance_local_nul_string_predicate
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT 'pushdown enabled';
SET lance_enable_predicate_pushdown = 1;
SELECT count() FROM lance_local_nul_string_predicate WHERE name = concat('a', char(0), 'b');
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name != concat('a', char(0));
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name IN (concat('a', char(0)), 'b', 'x');
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name NOT IN (concat('a', char(0), 'b'), 'x');

SELECT 'pushdown disabled';
SET lance_enable_predicate_pushdown = 0;
SELECT count() FROM lance_local_nul_string_predicate WHERE name = concat('a', char(0), 'b');
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name != concat('a', char(0));
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name IN (concat('a', char(0)), 'b', 'x');
SELECT arraySort(groupArray(id)) FROM lance_local_nul_string_predicate WHERE name NOT IN (concat('a', char(0), 'b'), 'x');

DROP TABLE lance_local_nul_string_predicate;
