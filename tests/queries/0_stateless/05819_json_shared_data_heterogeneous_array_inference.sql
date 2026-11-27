-- In shared data (and array elements generally) each value is inferred independently: existing Date/
-- DateTime variants of the dynamic must not be reused for a later element. A date after a date-time must
-- stay a Date (shown as "2024-01-15"), not be coerced to the existing DateTime ("2024-01-15 00:00:00").
-- Inference does not depend on json_use_optimized_type_conversion, so both values are checked.

SET session_timezone = 'UTC';

SELECT toString(CAST('{"a":["2024-01-15 12:00:00","2024-01-15",1]}' AS JSON(max_dynamic_paths = 0)));
SELECT toString(CAST('{"a":["2024-01-15 12:00:00","2024-01-15",1]}' AS JSON(max_dynamic_paths = 0))) SETTINGS json_use_optimized_type_conversion = 0;

SELECT toString(CAST('{"a":["2024-01-15","2024-01-15 12:00:00",1]}' AS JSON(max_dynamic_paths = 0)));
SELECT toString(CAST('{"a":["2024-01-15","2024-01-15 12:00:00",1]}' AS JSON(max_dynamic_paths = 0))) SETTINGS json_use_optimized_type_conversion = 0;
