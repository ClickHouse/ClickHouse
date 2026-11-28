-- A text index on `mapKeys(m)` or `JSONAllPaths(d)` only skips granules: it does not change how a token search reads a value.
-- With a `mapValues(m)` or `JSONAllValues(d)` index next to it, the search uses that index, as without the key index.
-- Random settings limits: query_plan_direct_read_from_text_index=(1, None); query_plan_text_index_add_hint=(1, None); optimize_functions_to_subcolumns=(1, None)

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_keys;
DROP TABLE IF EXISTS tab_both;
DROP TABLE IF EXISTS tab_keys_post;
DROP TABLE IF EXISTS tab_both_post;
DROP TABLE IF EXISTS tab_both_post_nullable;
DROP TABLE IF EXISTS tab_keys_nullable;
DROP TABLE IF EXISTS tab_json_paths;
DROP TABLE IF EXISTS tab_json_both;
DROP TABLE IF EXISTS tab_fixed;

CREATE TABLE tab_keys (id UInt32, m Map(String, String), INDEX ik mapKeys(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tab_both (id UInt32, m Map(String, String), INDEX ik mapKeys(m) TYPE text(tokenizer = array), INDEX iv mapValues(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_keys VALUES (1, {'k': 'Hello there'}), (2, {'k': 'Hello'}), (3, {'x': 'Hello'});
INSERT INTO tab_both VALUES (1, {'k': 'Hello there'}), (2, {'k': 'Hello'}), (3, {'x': 'Hello'});

SELECT '-- mapKeys';
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0);
SELECT groupArray(hasAnyTokens(m['k'], 'Hello')) FROM (SELECT m FROM tab_keys ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE hasAllTokens(m['k'], 'there Hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE hasPhrase(m['k'], 'there') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE m['k'] != '' AND if(id > 0, hasAnyTokens(m['k'], 'Hello'), 0) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE m['k'] != '' AND if(hasAnyTokens(m['k'], 'Hello'), toUInt8(2), toUInt8(0)) = id ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys WHERE m['k'] != '' AND if(hasAnyTokens(m['k'], 'Hello'), toUInt8(2), toUInt8(0)) = id ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- mapKeys and mapValues';
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasAnyTokens(m['k'], 'Hello') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0);
SELECT groupArray(hasAnyTokens(m['k'], 'Hello')) FROM (SELECT m FROM tab_both ORDER BY id) SETTINGS enable_parallel_replicas = 0;
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasAllTokens(m['k'], 'there Hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasAllTokens(m['k'], 'there Hello') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasPhrase(m['k'], 'there') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both WHERE hasPhrase(m['k'], 'there') ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- mapKeys with a postprocessor';
CREATE TABLE tab_keys_post (id UInt32, m Map(String, String), INDEX ik mapKeys(m) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(mapKeys(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_keys_post VALUES (1, {'key': 'Hello world'}), (2, {'key': 'hello world'});
SELECT groupArray(id) FROM (SELECT id FROM tab_keys_post WHERE hasToken(m['key'], 'hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys_post WHERE hasToken(m['key'], 'hello') ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- mapKeys and mapValues with a postprocessor';
CREATE TABLE tab_both_post (id UInt32, m Map(String, String), INDEX ik mapKeys(m) TYPE text(tokenizer = array), INDEX iv mapValues(m) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_both_post VALUES (1, {'k': 'Hello there'}), (2, {'k': 'HELLO'}), (3, {'x': 'hello'});
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE hasAnyTokens(m['k'], 'hello') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE hasToken(m['k'], 'hello') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE hasToken(m['k'], 'hello') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE if(hasAnyTokens(m['k'], 'hello'), toUInt8(2), toUInt8(0)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE if(hasAnyTokens(m['k'], 'hello'), toUInt8(2), toUInt8(0)) ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE toString(hasAnyTokens(m['k'], 'hello')) = '1' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post WHERE toString(hasAnyTokens(m['k'], 'hello')) = '1' ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- mapKeys and mapValues with a postprocessor, Nullable values';
CREATE TABLE tab_both_post_nullable (id UInt32, m Map(String, Nullable(String)), INDEX ik mapKeys(m) TYPE text(tokenizer = array), INDEX iv mapValues(m) TYPE text(tokenizer = splitByNonAlpha, postprocessor = lower(mapValues(m))))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_both_post_nullable VALUES (1, {'x': 'hello'}), (2, {'k': 'Hello'}), (3, {'k': NULL});
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post_nullable WHERE isNotNull(hasAnyTokens(m['k'], 'hello')) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post_nullable WHERE isNotNull(hasAnyTokens(m['k'], 'hello')) ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post_nullable WHERE hasAnyTokens(m['k'], 'hello') = 0 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_both_post_nullable WHERE hasAnyTokens(m['k'], 'hello') = 0 ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- mapKeys, Nullable values, a tokenizer argument';
CREATE TABLE tab_keys_nullable (id UInt32, expected UInt8, m Map(String, Nullable(String)), INDEX ik mapKeys(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_keys_nullable SELECT number + 1, number = 1, multiIf(number = 0, map('j', 'ok'), number = 1, map('k', 'hello'), map('x', 'other')) FROM numbers(12);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys_nullable WHERE (m['k'] = 'hello' OR m['j'] = 'ok') AND hasAnyTokens(m['k'], 'hello', 'splitByNonAlpha') = expected ORDER BY id SETTINGS optimize_move_to_prewhere = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_keys_nullable WHERE (m['k'] = 'hello' OR m['j'] = 'ok') AND hasAnyTokens(m['k'], 'hello', 'splitByNonAlpha') = expected ORDER BY id SETTINGS optimize_move_to_prewhere = 0, use_skip_indexes = 0);

SELECT '-- JSONAllPaths';
CREATE TABLE tab_json_paths (id UInt32, d JSON, INDEX ip JSONAllPaths(d) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE tab_json_both (id UInt32, d JSON, INDEX ip JSONAllPaths(d) TYPE text(tokenizer = array), INDEX iv JSONAllValues(d) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_json_paths VALUES (1, '{"msg": "connection timeout"}'), (2, '{"msg": "timeout"}'), (3, '{"other": "timeout"}');
INSERT INTO tab_json_both VALUES (1, '{"msg": "connection timeout"}'), (2, '{"msg": "timeout"}'), (3, '{"other": "timeout"}');
SELECT groupArray(id) FROM (SELECT id FROM tab_json_paths WHERE hasAnyTokens(d.msg::String, 'timeout') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_json_paths WHERE hasAnyTokens(d.msg::String, 'timeout') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT '-- JSONAllPaths and JSONAllValues';
SELECT groupArray(id) FROM (SELECT id FROM tab_json_both WHERE hasAnyTokens(d.msg::String, 'timeout') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_json_both WHERE hasAnyTokens(d.msg::String, 'timeout') ORDER BY id SETTINGS use_skip_indexes = 0);

SELECT '-- the values postprocessor makes a missing key match';
CREATE TABLE tab_fixed (id UInt32, m Map(String, FixedString(6)),
    INDEX ik mapKeys(m) TYPE text(tokenizer = array),
    INDEX iv mapValues(m) TYPE text(tokenizer = ngrams(3), postprocessor = if(match(mapValues(m), '[a-z]'), mapValues(m), '#')))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_fixed VALUES (1, map()), (2, map('k', 'hello'));
SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], '123') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], '123') ORDER BY id SETTINGS use_skip_indexes = 0);
SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM tab_fixed WHERE hasAnyTokens(m['k'], '123', 'ngrams(3)') ORDER BY id SETTINGS use_skip_indexes = 0);

DROP TABLE tab_keys;
DROP TABLE tab_both;
DROP TABLE tab_keys_post;
DROP TABLE tab_both_post;
DROP TABLE tab_both_post_nullable;
DROP TABLE tab_keys_nullable;
DROP TABLE tab_json_paths;
DROP TABLE tab_json_both;
DROP TABLE tab_fixed;
