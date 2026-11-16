-- An argument of type `Nullable(Nothing)` can still have descendants in the actions DAG.
-- A node shared by it and a lazy argument must stay lazy, otherwise it is evaluated for all rows.

SELECT 'enable';
SET short_circuit_function_evaluation = 'enable';
SELECT coalesce(if(number = 0, toNullable(1), NULL), intDiv(100, number), if(intDiv(100, number) > 0, NULL, NULL)) FROM numbers(2);
SELECT coalesce(if(number = 0, toNullable(1), NULL), if(intDiv(100, number) > 0, NULL, NULL), intDiv(100, number)) FROM numbers(2);
SELECT coalesce(if(intDiv(100, number) > 0, NULL, NULL), if(number = 0, toNullable(1), NULL), intDiv(100, number)) FROM numbers(2);

SELECT 'force_enable';
SET short_circuit_function_evaluation = 'force_enable';
SELECT coalesce(if(number = 0, toNullable(1), NULL), intDiv(100, number), if(intDiv(100, number) > 0, NULL, NULL)) FROM numbers(2);
SELECT coalesce(if(number = 0, toNullable(1), NULL), if(intDiv(100, number) > 0, NULL, NULL), intDiv(100, number)) FROM numbers(2);
SELECT coalesce(if(intDiv(100, number) > 0, NULL, NULL), if(number = 0, toNullable(1), NULL), intDiv(100, number)) FROM numbers(2);
