-- Name resolution tries the shortest prefix first, so beside a Map `a.b` a JSON column `a` claims the
-- name `a.b.key_zzz` as one of its paths: a predicate on that name reads the JSON, not the map. Skip
-- indexes over the map used to read the name as key `zzz` of `a.b` and prune the granule, because the
-- map has neither that key nor that value. Every count must find the inserted row.

SET use_skip_indexes = 1;

DROP TABLE IF EXISTS t_json_claimant_bloom_filter;
CREATE TABLE t_json_claimant_bloom_filter
(
    `a.b` Map(String, String),
    a JSON,
    INDEX idx mapKeys(`a.b`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_claimant_bloom_filter VALUES ({'abc' : 'y'}, '{"b" : {"key_zzz" : "x"}}');
SELECT 'bloom_filter over mapKeys', count() FROM t_json_claimant_bloom_filter WHERE a.b.key_zzz = 'x';

DROP TABLE IF EXISTS t_json_claimant_tokenbf;
CREATE TABLE t_json_claimant_tokenbf
(
    `a.b` Map(String, String),
    a JSON,
    INDEX idx mapKeys(`a.b`) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_claimant_tokenbf VALUES ({'abc' : 'y'}, '{"b" : {"key_zzz" : "x"}}');
SELECT 'tokenbf_v1 over mapKeys', count() FROM t_json_claimant_tokenbf WHERE a.b.key_zzz = 'x';

DROP TABLE IF EXISTS t_json_claimant_text_keys;
CREATE TABLE t_json_claimant_text_keys
(
    `a.b` Map(String, String),
    a JSON,
    INDEX idx mapKeys(`a.b`) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_claimant_text_keys VALUES ({'abc' : 'y'}, '{"b" : {"key_zzz" : "x"}}');
SELECT 'text over mapKeys', count() FROM t_json_claimant_text_keys WHERE a.b.key_zzz = 'x';

DROP TABLE IF EXISTS t_json_claimant_text_values;
CREATE TABLE t_json_claimant_text_values
(
    `a.b` Map(String, String),
    a JSON,
    INDEX idx mapValues(`a.b`) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_claimant_text_values VALUES ({'abc' : 'y'}, '{"b" : {"key_zzz" : "x"}}');
SELECT 'text over mapValues', count() FROM t_json_claimant_text_values WHERE a.b.key_zzz = 'x';

-- A typed Map path of the JSON claims the name the same way: `a.b.key_zzz` reads key `zzz` of the path `b`.
DROP TABLE IF EXISTS t_json_claimant_typed_map;
CREATE TABLE t_json_claimant_typed_map
(
    `a.b` Map(String, String),
    a JSON(b Map(String, String)),
    INDEX idx mapKeys(`a.b`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_claimant_typed_map VALUES ({'abc' : 'y'}, '{"b" : {"zzz" : "x"}}');
SELECT 'bloom_filter over mapKeys, typed JSON path', count() FROM t_json_claimant_typed_map WHERE a.b.key_zzz = 'x';

-- The claimant can also live in the Map's own storage column: in the Tuple below the element `j.m` answers
-- `c.j.m`, while the path `m` of the JSON element `j` answers `c.j.m.key_nokey`. Only their substreams differ.
DROP TABLE IF EXISTS t_sibling_bloom_filter_keys;
CREATE TABLE t_sibling_bloom_filter_keys
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx mapKeys(c.`j.m`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_bloom_filter_keys SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, bloom_filter over mapKeys', count() FROM t_sibling_bloom_filter_keys WHERE c.j.m.key_nokey = 'json';

DROP TABLE IF EXISTS t_sibling_bloom_filter_values;
CREATE TABLE t_sibling_bloom_filter_values
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx mapValues(c.`j.m`) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_bloom_filter_values SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, bloom_filter over mapValues', count() FROM t_sibling_bloom_filter_values WHERE c.j.m.key_nokey = 'json';

DROP TABLE IF EXISTS t_sibling_tokenbf;
CREATE TABLE t_sibling_tokenbf
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx mapKeys(c.`j.m`) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_tokenbf SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, tokenbf_v1 over mapKeys', count() FROM t_sibling_tokenbf WHERE c.j.m.key_nokey = 'json';

DROP TABLE IF EXISTS t_sibling_text_keys;
CREATE TABLE t_sibling_text_keys
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx mapKeys(c.`j.m`) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_text_keys SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, text over mapKeys', count() FROM t_sibling_text_keys WHERE c.j.m.key_nokey = 'json';

DROP TABLE IF EXISTS t_sibling_text_values;
CREATE TABLE t_sibling_text_values
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx mapValues(c.`j.m`) TYPE text(tokenizer = 'array') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_text_values SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, text over mapValues', count() FROM t_sibling_text_values WHERE c.j.m.key_nokey = 'json';

DROP TABLE IF EXISTS t_sibling_text_kv;
CREATE TABLE t_sibling_text_kv
(
    c Tuple(`j.m` Map(String, String), j JSON(m Map(String, String))),
    INDEX idx c.`j.m` TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_sibling_text_kv SELECT tuple(map('abc', 'physical'), CAST('{"m" : {"nokey" : "json"}}', 'JSON(m Map(String, String))'));
SELECT 'sibling JSON path, text over the map, keyValuePairs', count() FROM t_sibling_text_kv WHERE c.j.m.key_nokey = 'json';

DROP TABLE t_json_claimant_bloom_filter;
DROP TABLE t_json_claimant_tokenbf;
DROP TABLE t_json_claimant_text_keys;
DROP TABLE t_json_claimant_text_values;
DROP TABLE t_json_claimant_typed_map;
DROP TABLE t_sibling_bloom_filter_keys;
DROP TABLE t_sibling_bloom_filter_values;
DROP TABLE t_sibling_tokenbf;
DROP TABLE t_sibling_text_keys;
DROP TABLE t_sibling_text_values;
DROP TABLE t_sibling_text_kv;
