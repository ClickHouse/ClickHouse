-- Subcolumns are read with their columns and extracted by ClickHouse, both when they are selected
-- and when a filter that is not pushed down to `Lance` needs them.
DROP TABLE IF EXISTS lance_local_subcolumns;

CREATE TABLE lance_local_subcolumns
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/rich_types.lance');

SELECT 'select';
SELECT struct_value.code, struct_value.label, struct_value.label.null, array_value.size0, array_value.null
FROM lance_local_subcolumns
ORDER BY struct_value.code;

SELECT 'filter, pushdown enabled';
SELECT struct_value.code FROM lance_local_subcolumns WHERE struct_value.code = 1 SETTINGS lance_enable_predicate_pushdown = 1;
SELECT struct_value.label FROM lance_local_subcolumns WHERE array_value.size0 = 1 SETTINGS lance_enable_predicate_pushdown = 1;
SELECT count() FROM lance_local_subcolumns WHERE struct_value.code > 1 AND array_value.size0 > 0 SETTINGS lance_enable_predicate_pushdown = 1;

SELECT 'filter, pushdown disabled';
SELECT struct_value.code FROM lance_local_subcolumns WHERE struct_value.code = 1 SETTINGS lance_enable_predicate_pushdown = 0;
SELECT struct_value.label FROM lance_local_subcolumns WHERE array_value.size0 = 1 SETTINGS lance_enable_predicate_pushdown = 0;
SELECT count() FROM lance_local_subcolumns WHERE struct_value.code > 1 AND array_value.size0 > 0 SETTINGS lance_enable_predicate_pushdown = 0;

SELECT 'table function';
SELECT struct_value.code, array_value.size0
FROM lanceLocal('tests/queries/0_stateless/data_lance/rich_types.lance')
WHERE struct_value.code >= 2
ORDER BY struct_value.code;

DROP TABLE lance_local_subcolumns;
