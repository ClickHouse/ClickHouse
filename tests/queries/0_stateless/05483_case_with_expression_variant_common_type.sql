-- CASE expr WHEN ... must respect use_variant_as_common_type like multiIf does.
-- https://github.com/ClickHouse/ClickHouse/issues/74623

SET use_variant_as_common_type = 1;

SELECT CASE number WHEN 0 THEN 'a' ELSE 1 END AS r, toTypeName(r) FROM numbers(2);
SELECT CASE number WHEN 0 THEN toString(number) ELSE number END AS r, toTypeName(r) FROM numbers(2);

-- Constant expression.
SELECT CASE 0 WHEN 0 THEN 'a' ELSE 1 END AS r, toTypeName(r);
SELECT CASE 1 WHEN 0 THEN 'a' ELSE 1 END AS r, toTypeName(r);

-- NULL branch.
SELECT CASE number WHEN 0 THEN 'a' WHEN 1 THEN NULL ELSE 1 END AS r, toTypeName(r) FROM numbers(3);

-- A common supertype still wins over Variant.
SELECT CASE number WHEN 0 THEN 1 WHEN 1 THEN 1000 ELSE -1 END AS r, toTypeName(r) FROM numbers(3);

-- CASE without an expression is multiIf and already worked.
SELECT CASE WHEN number = 0 THEN 'a' ELSE 1 END AS r, toTypeName(r) FROM numbers(2);

SET use_variant_as_common_type = 0;

SELECT CASE number WHEN 0 THEN 'a' ELSE 1 END AS r, toTypeName(r) FROM numbers(2); -- { serverError NO_COMMON_TYPE }
