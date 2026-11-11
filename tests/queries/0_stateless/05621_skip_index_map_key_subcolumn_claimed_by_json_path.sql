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

DROP TABLE t_json_claimant_bloom_filter;
DROP TABLE t_json_claimant_tokenbf;
DROP TABLE t_json_claimant_text_keys;
DROP TABLE t_json_claimant_text_values;
DROP TABLE t_json_claimant_typed_map;
