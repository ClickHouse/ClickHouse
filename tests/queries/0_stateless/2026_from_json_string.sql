-- UInt8
select fromJSONString('{"a": 100}', 'Map(String, UInt8)');
select fromJSONString('{"a": -100}', 'Map(String, UInt8)');
select fromJSONString('{"a": "100"}', 'Map(String, UInt8)');
select fromJSONString('{"a": "-100"}', 'Map(String, UInt8)');
select fromJSONString('{"a": "-100.123"}', 'Map(String, UInt8)');
select fromJSONString('{"a": true}', 'Map(String, UInt8)');
select fromJSONString('{"a": false}', 'Map(String, UInt8)');
select fromJSONString('{"a": null}', 'Map(String, UInt8)');

-- Int8
select fromJSONString('{"a": -100}', 'Map(String, Int8)');
select fromJSONString('{"a": "-100"}', 'Map(String, Int8)');
select fromJSONString('{"a": "-100.123"}', 'Map(String, Int8)');
select fromJSONString('{"a": true}', 'Map(String, Int8)');
select fromJSONString('{"a": false}', 'Map(String, Int8)');
select fromJSONString('{"a": null}', 'Map(String, Int8)');

-- String
select fromJSONString('{"a": "-100"}', 'Map(String, String)');
select fromJSONString('{"a": -100}', 'Map(String, String)');
select fromJSONString('{"a": true}', 'Map(String, String)');
select fromJSONString('{"a": false}', 'Map(String, String)');
select fromJSONString('{"a": null}', 'Map(String, String)');

-- FixedString
select fromJSONString('{"a": "-100"}', 'Map(String, FixedString(4))');
select fromJSONString('{"a": "-100"}', 'Map(String, FixedString(5))');
select fromJSONString('{"a": "-100"}', 'Map(String, FixedString(3))');
select fromJSONString('{"a": true}', 'Map(String, FixedString(5))');
select fromJSONString('{"a": false}', 'Map(String, FixedString(5))');
select fromJSONString('{"a": null}', 'Map(String, FixedString(5))');

-- Array
select fromJSONString('{"a": [1,2,3]}', 'Map(String, Array(Int8))');
select fromJSONString('{"a": ["1","2","3"]}', 'Map(String, Array(Int8))');

-- Map 
select fromJSONString('{"a": {"b": 100}}', 'Map(String, Map(String, Int8))');
select fromJSONString('{"a": {"b": "100"}}', 'Map(String, Map(String, Int8))');

-- Tuple
select fromJSONString('{"a": [1,2,3]}', 'Map(String, Tuple(Int8, Float64, String))');
select fromJSONString('{"a": [1,2,3]}', 'Map(String, Tuple(i Int8, f Float64, s String))');
select fromJSONString('{"a": {"i": 1, "f": "2.22", "s": "hello"}}', 'Map(String, Tuple(f Float64, s String, i Int8))');
select fromJSONString('{"a": {"i": 1, "f": "2.22", "s": "hello"}}', 'Map(String, Tuple(f Float64, s String, i Int8, x FixedString(3)))');

-- Nullable
select fromJSONString('{"a": -100}', 'Map(String, Nullable(Int8))');

-- LowCardinality
select fromJSONString('{"a": -100}', 'Map(String, LowCardinality(Int8))');
