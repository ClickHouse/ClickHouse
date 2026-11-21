-- https://github.com/ClickHouse/ClickHouse/issues/79592
-- A tokenbf_v1 index on `lowerUTF8(v)` is used by `hasTokenCaseInsensitive` like one on `lower(v)`,
-- but only for an ASCII needle: `lowerUTF8` maps non-ASCII letters differently (e.g. a context-dependent final sigma).
-- An ngrambf_v1 index on `lowerUTF8(v)` is not used: `lowerUTF8` can shift the n-grams of invalid UTF-8.

SET explain_query_plan_default = 'legacy';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_tok_lower;
DROP TABLE IF EXISTS t_tok_lower_utf8;
DROP TABLE IF EXISTS t_ngram_lower;
DROP TABLE IF EXISTS t_ngram_lower_utf8;

CREATE TABLE t_tok_lower (id UInt64, v String, INDEX idx lower(v) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
CREATE TABLE t_tok_lower_utf8 (id UInt64, v String, INDEX idx lowerUTF8(v) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
CREATE TABLE t_ngram_lower (id UInt64, v String, INDEX idx lower(v) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
CREATE TABLE t_ngram_lower_utf8 (id UInt64, v String, INDEX idx lowerUTF8(v) TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';

-- `lowerUTF8('ΑΣ')` is 'ας', but the token 'ΑΣ' of 'ΑΣ''Α' is stored as 'ασ'.
INSERT INTO t_tok_lower SELECT number, multiIf(number = 3, 'xx Hello World yy', number = 5, 'xx ÄBC yy', number = 7, 'xx ΑΣ''Α yy', 'aaa bbb') FROM numbers(10);
INSERT INTO t_tok_lower_utf8 SELECT * FROM t_tok_lower;
INSERT INTO t_ngram_lower SELECT * FROM t_tok_lower;
INSERT INTO t_ngram_lower_utf8 SELECT * FROM t_tok_lower;

SELECT '-- expected counts without skip indexes';
SELECT
    countIf(hasTokenCaseInsensitive(v, 'HeLLo')),
    countIf(hasTokenCaseInsensitiveOrNull(v, 'WORLD')),
    countIf(hasTokenCaseInsensitive(v, 'ÄBC')),
    countIf(hasTokenCaseInsensitive(v, 'äbc')),
    countIf(hasTokenCaseInsensitive(v, 'ΑΣ'))
FROM t_tok_lower SETTINGS use_skip_indexes = 0;

SELECT '-- tokenbf_v1 on lower(v)';
SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'HeLLo');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'HeLLo')));
SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitiveOrNull(v, 'WORLD');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitiveOrNull(v, 'WORLD')));
SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'ÄBC');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'ÄBC')));
SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'äbc');
SELECT count() FROM t_tok_lower WHERE hasTokenCaseInsensitive(v, 'ΑΣ');

SELECT '-- tokenbf_v1 on lowerUTF8(v)';
SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'HeLLo');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'HeLLo')));
SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitiveOrNull(v, 'WORLD');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitiveOrNull(v, 'WORLD')));
SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ÄBC');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ÄBC')));
SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'äbc');
SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ΑΣ');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tok_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ΑΣ')));

SELECT '-- ngrambf_v1 on lower(v)';
SELECT count() FROM t_ngram_lower WHERE hasTokenCaseInsensitive(v, 'HeLLo');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_lower WHERE hasTokenCaseInsensitive(v, 'HeLLo')));
SELECT count() FROM t_ngram_lower WHERE hasTokenCaseInsensitive(v, 'ÄBC');

SELECT '-- ngrambf_v1 on lowerUTF8(v)';
SELECT count() FROM t_ngram_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'HeLLo');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'HeLLo')));
SELECT count() FROM t_ngram_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ÄBC');
SELECT if(i = 0, 'idx not used', arrayFirst(x -> startsWith(x, 'Granules:'), arraySlice(e, i))) FROM (SELECT groupArray(trimBoth(explain)) AS e, indexOf(e, 'Name: idx') AS i FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ngram_lower_utf8 WHERE hasTokenCaseInsensitive(v, 'ÄBC')));

DROP TABLE t_tok_lower;
DROP TABLE t_tok_lower_utf8;
DROP TABLE t_ngram_lower;
DROP TABLE t_ngram_lower_utf8;
