-- Tags: no-fasttest
-- RapidJSON, used with `allow_simdjson = 0`, is not built in the fast test.

-- The two JSON parsers differ on an integer too large for `UInt64`: simdjson gives `String`, RapidJSON `Double`.
-- So a key on `JSONType` must not be used by a session that changes `allow_simdjson`.

DROP TABLE IF EXISTS t_json_parser;
CREATE TABLE t_json_parser (j String) ENGINE = MergeTree ORDER BY JSONType(j, 'a') SETTINGS index_granularity = 1;
INSERT INTO t_json_parser SELECT if(number = 0, '{"a":18446744073709551616}', '{"a":"x"}') FROM numbers(20);

SET allow_simdjson = 0;
SELECT 'json parser expected', count() FROM t_json_parser WHERE JSONType(j, 'a') = 'Double' SETTINGS use_primary_key = 0;
SELECT 'json parser', count() FROM t_json_parser WHERE JSONType(j, 'a') = 'Double';

DROP TABLE t_json_parser;
