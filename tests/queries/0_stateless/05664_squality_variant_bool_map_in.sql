SELECT
    map('a', 'true') IN CAST(map('a', true) AS Variant(Map(String, Bool), String)),
    map('a', 'true') IN materialize(CAST(map('a', true) AS Variant(Map(String, Bool), String))),
    map('a', 'true') IN (materialize(CAST(map('a', true) AS Variant(Map(String, Bool), String))), NULL),
    map('a', 'true') NOT IN (materialize(CAST(map('a', true) AS Variant(Map(String, Bool), String))), NULL)
SETTINGS in_null_as_unknown = 1;

SELECT count()
FROM (SELECT map('a', 'true') AS m)
WHERE m IN (materialize(CAST(map('a', true) AS Variant(Map(String, Bool), String))), NULL)
SETTINGS in_null_as_unknown = 1;
