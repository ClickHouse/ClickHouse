-- A `String` constant can contain bytes that are not valid UTF-8, while `Lance` accepts only UTF-8 predicates,
-- so the filter must not be pushed down, and the results must be the same as with the pushdown disabled.
DROP TABLE IF EXISTS lance_local_invalid_utf8_string_predicate;

CREATE TABLE lance_local_invalid_utf8_string_predicate
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT 'pushdown enabled';
SET lance_enable_predicate_pushdown = 1;
SELECT count() FROM lance_local_invalid_utf8_string_predicate WHERE name = unhex('FF');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name != unhex('FF');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name IN (unhex('FF'), 'b', 'x');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name NOT IN (unhex('FF'), 'x');

SELECT 'pushdown disabled';
SET lance_enable_predicate_pushdown = 0;
SELECT count() FROM lance_local_invalid_utf8_string_predicate WHERE name = unhex('FF');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name != unhex('FF');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name IN (unhex('FF'), 'b', 'x');
SELECT arraySort(groupArray(id)) FROM lance_local_invalid_utf8_string_predicate WHERE name NOT IN (unhex('FF'), 'x');

DROP TABLE lance_local_invalid_utf8_string_predicate;
