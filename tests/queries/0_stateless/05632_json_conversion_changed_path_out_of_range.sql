-- A JSON conversion parses a value it cannot convert exactly, so it rejects what the parser rejects
-- instead of wrapping a number or converting the internal value of a date or an address.
-- Every case runs with the optimized conversion and with format+parse.
-- The queries without the setting check the optimized conversion, so pin its default against the settings randomization.
SET json_use_optimized_type_conversion = 1;

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

-- A malformed address is rejected whatever the CAST-only default-on-error settings say (the direct cast
-- raises CANNOT_PARSE_IPV4/6, the parser INCORRECT_DATA; both reject instead of storing a default address).
SELECT CAST('{"a":"bad"}'::JSON(a String), 'JSON(a IPv4)') SETTINGS input_format_ipv4_default_on_conversion_error = 1; -- { serverError CANNOT_PARSE_IPV4 }
SELECT CAST('{"a":"bad"}'::JSON(a String), 'JSON(a IPv4)') SETTINGS input_format_ipv4_default_on_conversion_error = 1, json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"bad"}'::JSON(a String), 'JSON(a IPv6)') SETTINGS input_format_ipv6_default_on_conversion_error = 1; -- { serverError CANNOT_PARSE_IPV6 }
SELECT CAST('{"a":"bad"}'::JSON(a String), 'JSON(a IPv6)') SETTINGS input_format_ipv6_default_on_conversion_error = 1, json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- date_time_overflow_behavior = 'throw' makes an out-of-range date-time string reject on both routes
-- (the direct CAST throws VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE, the parser INCORRECT_DATA).
SELECT CAST('{"a":"2280-01-01 00:00:00"}'::JSON(a String), 'JSON(a DateTime)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST('{"a":"2280-01-01 00:00:00"}'::JSON(a String), 'JSON(a DateTime)') SETTINGS date_time_overflow_behavior = 'throw', json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- Removing a populated enum member rejects a value that is no longer in the destination, instead of keeping
-- its number. Holds for an array of enums and for a numeric source with membership checking disabled.
SELECT CAST('{"a":"y"}'::JSON(a Enum8('x'=1,'y'=2)), 'JSON(a Enum8(''x''=1))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"y"}'::JSON(a Enum8('x'=1,'y'=2)), 'JSON(a Enum8(''x''=1))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":["y"]}'::JSON(a Array(Enum8('x'=1,'y'=2))), 'JSON(a Array(Enum8(''x''=1)))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":["y"]}'::JSON(a Array(Enum8('x'=1,'y'=2))), 'JSON(a Array(Enum8(''x''=1)))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":2}'::JSON, 'JSON(a Enum8(''x''=1))') SETTINGS check_conversion_from_numbers_to_enum = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":2}'::JSON, 'JSON(a Enum8(''x''=1))') SETTINGS check_conversion_from_numbers_to_enum = 0, json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }

-- Malformed text to a Nullable typed path is rejected, not stored as NULL (a CAST to Nullable would turn the
-- parse failure into NULL). The direct CAST raises the type's own parse error, the parser INCORRECT_DATA.
-- Holds for array elements and for a Nullable(String) source, which goes through the parser.
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(UUID))'); -- { serverError CANNOT_PARSE_UUID }
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(UUID))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(IPv4))'); -- { serverError CANNOT_PARSE_IPV4 }
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(IPv4))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(FixedString(3)))'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT CAST('{"a":"badbad"}'::JSON(a String), 'JSON(a Nullable(FixedString(3)))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":["badbad"]}'::JSON(a Array(String)), 'JSON(a Array(Nullable(UUID)))'); -- { serverError CANNOT_PARSE_UUID }
SELECT CAST('{"a":["badbad"]}'::JSON(a Array(String)), 'JSON(a Array(Nullable(UUID)))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"badbad"}'::JSON(a Nullable(String)), 'JSON(a Nullable(UUID))'); -- { serverError INCORRECT_DATA }
SELECT CAST('{"a":"badbad"}'::JSON(a Nullable(String)), 'JSON(a Nullable(UUID))') SETTINGS json_use_optimized_type_conversion = 0; -- { serverError INCORRECT_DATA }
