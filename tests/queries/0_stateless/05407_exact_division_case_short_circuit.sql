SET exact_division_for_integers = 1;
SET short_circuit_function_evaluation = 'force_enable';

SELECT CASE 1 WHEN 0 THEN 1 / 0 WHEN 1 THEN 1 ELSE 2 / 0 END;
SELECT number, CASE number WHEN 0 THEN 1 WHEN 1 THEN 2 ELSE 3 / 0 END FROM numbers(2) ORDER BY number;
SELECT if(number = 0, 1, 3 / 0) FROM numbers(1);
SELECT multiIf(number = 0, 1, number = 1, 2, 3 / 0) FROM numbers(2) ORDER BY number;
SELECT CASE 1 WHEN 1 THEN 5 / 2 ELSE 1 / 0 END, toTypeName(CASE 1 WHEN 1 THEN 5 / 2 ELSE 1 / 0 END);

SELECT 1 / 0; -- { serverError ILLEGAL_DIVISION }
SELECT CASE 0 WHEN 0 THEN 1 / 0 ELSE 1 END; -- { serverError ILLEGAL_DIVISION }

SET short_circuit_function_evaluation = 'disable';
SELECT CASE 1 WHEN 1 THEN 1 ELSE 1 / 0 END; -- { serverError ILLEGAL_DIVISION }
