-- A JSON conversion parses a value it cannot convert exactly, so it rejects what the parser rejects
-- instead of wrapping a number or converting the internal value of a date or an address.
-- Every case runs with the optimized conversion and with format+parse.

SELECT CAST('{"a":300}'::JSON(a Int64), 'JSON(a Int8)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":300}'::JSON(a Int64), 'JSON(a Int8)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":-1}'::JSON(a Int64), 'JSON(a UInt64)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":-1}'::JSON(a Int64), 'JSON(a UInt64)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":1e300}'::JSON(a Float64), 'JSON(a Int64)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":1e300}'::JSON(a Float64), 'JSON(a Int64)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"300"}'::JSON(a String), 'JSON(a Int8)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"300"}'::JSON(a String), 'JSON(a Int8)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":[300]}'::JSON(a Array(Int64)), 'JSON(a Array(Int8))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":[300]}'::JSON(a Array(Int64)), 'JSON(a Array(Int8))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":300}'::JSON(a Nullable(Int64)), 'JSON(a Nullable(Int8))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":300}'::JSON(a Nullable(Int64)), 'JSON(a Nullable(Int8))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- The text of a date or an address is not a number.
SELECT CAST('{"a":"2024-01-15"}'::JSON(a Date), 'JSON(a Int64)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"2024-01-15"}'::JSON(a Date), 'JSON(a Int64)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"1.2.3.4"}'::JSON(a IPv4), 'JSON(a UInt32)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"1.2.3.4"}'::JSON(a IPv4), 'JSON(a UInt32)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- An array is read only from a JSON array, not from a number or from a string holding one.
SELECT CAST('{"a":1}'::JSON(a Int64), 'JSON(a Array(Int64))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":1}'::JSON(a Int64), 'JSON(a Array(Int64))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"[1,2]"}'::JSON(a String), 'JSON(a Array(Int64))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"[1,2]"}'::JSON(a String), 'JSON(a Array(Int64))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- A new typed path filled from a dynamic path.
SELECT CAST('{"a":300}'::JSON, 'JSON(a Int8)'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":300}'::JSON, 'JSON(a Int8)') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
