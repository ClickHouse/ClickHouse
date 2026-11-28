-- Removing a typed `String` path from `JSON` must infer `IPv4`/`IPv6` in the same way
-- with and without `json_use_optimized_type_conversion`.

SET input_format_try_infer_ipv4 = 1, input_format_try_infer_ipv6 = 1;

SELECT 'IPv4';
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'IPv6';
SELECT dynamicType(('{"ip":"2001:db8::1"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"2001:db8::1"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'Array';
SELECT dynamicType(('{"ip":["192.168.1.1","10.0.0.1"]}'::JSON(ip Array(String))::JSON).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":["192.168.1.1","10.0.0.1"]}'::JSON(ip Array(String))::JSON).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'LowCardinality(Nullable(String))';
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip LowCardinality(Nullable(String)))::JSON).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip LowCardinality(Nullable(String)))::JSON).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'Shared data';
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON(max_dynamic_paths = 0)).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON(max_dynamic_paths = 0)).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'Not an address';
SELECT dynamicType(('{"ip":"hello"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"hello"}'::JSON(ip String)::JSON).ip) SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'Mixed rows';
SELECT dynamicType((json::JSON(ip String)::JSON).ip) FROM values('json String', '{"ip":"192.168.1.1"}', '{"ip":"hello"}', '{"ip":"10.0.0.1"}') SETTINGS json_use_optimized_type_conversion = 0;
SELECT dynamicType((json::JSON(ip String)::JSON).ip) FROM values('json String', '{"ip":"192.168.1.1"}', '{"ip":"hello"}', '{"ip":"10.0.0.1"}') SETTINGS json_use_optimized_type_conversion = 1;

SELECT 'Inference disabled';
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON).ip) SETTINGS input_format_try_infer_ipv4 = 0, input_format_try_infer_ipv6 = 0, json_use_optimized_type_conversion = 0;
SELECT dynamicType(('{"ip":"192.168.1.1"}'::JSON(ip String)::JSON).ip) SETTINGS input_format_try_infer_ipv4 = 0, input_format_try_infer_ipv6 = 0, json_use_optimized_type_conversion = 1;
