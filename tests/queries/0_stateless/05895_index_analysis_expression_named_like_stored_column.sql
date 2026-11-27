-- a stored column with the name of an expression is not that expression in index analysis
-- in each table the column `lower(s)` holds 'zzz' and the expression lower(s) is 'hello'

SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_named_pk;
CREATE TABLE t_named_pk (s String, `lower(s)` String) ENGINE = MergeTree ORDER BY `lower(s)`;
INSERT INTO t_named_pk VALUES ('HELLO', 'zzz');
SELECT 'primary key', count() FROM t_named_pk WHERE lower(s) = 'hello';
SELECT 'primary key, the column', count() FROM t_named_pk WHERE `lower(s)` = 'zzz';
SELECT 'primary key, indexHint', count() FROM t_named_pk WHERE indexHint(lower(s) = 'hello');
SELECT 'primary key, nested', count() FROM t_named_pk WHERE length(lower(s)) = 5;
ALTER TABLE t_named_pk DELETE WHERE lower(s) = 'hello' SETTINGS mutations_sync = 2;
SELECT 'mutation', count() FROM t_named_pk;
DROP TABLE t_named_pk;

DROP TABLE IF EXISTS t_named_partition;
CREATE TABLE t_named_partition (s String, `lower(s)` String) ENGINE = MergeTree PARTITION BY `lower(s)` ORDER BY tuple();
INSERT INTO t_named_partition VALUES ('HELLO', 'zzz');
SELECT 'partition key', count() FROM t_named_partition WHERE lower(s) = 'hello';
DROP TABLE t_named_partition;

DROP TABLE IF EXISTS t_named_projection;
CREATE TABLE t_named_projection (s String, `lower(s)` String, PROJECTION p (SELECT * ORDER BY `lower(s)`)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_named_projection VALUES ('HELLO', 'zzz');
SELECT 'projection key', count() FROM t_named_projection WHERE lower(s) = 'hello';
DROP TABLE t_named_projection;

DROP TABLE IF EXISTS t_named_index;
CREATE TABLE t_named_index
(
    s String,
    `lower(s)` String,
    INDEX i_minmax `lower(s)` TYPE minmax GRANULARITY 1,
    INDEX i_set `lower(s)` TYPE set(0) GRANULARITY 1,
    INDEX i_set_function length(`lower(s)`) TYPE set(0) GRANULARITY 1,
    INDEX i_bloom `lower(s)` TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX i_token `lower(s)` TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1,
    INDEX i_ngram `lower(s)` TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1,
    INDEX i_text `lower(s)` TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_named_index VALUES ('HELLO', 'zzz');
SELECT 'minmax', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_set, i_set_function, i_bloom, i_token, i_ngram, i_text';
SELECT 'set', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set_function, i_bloom, i_token, i_ngram, i_text';
SELECT 'set on a function', count() FROM t_named_index WHERE length(lower(s)) = 5 SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set, i_bloom, i_token, i_ngram, i_text';
SELECT 'bloom_filter', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set, i_set_function, i_token, i_ngram, i_text';
SELECT 'tokenbf_v1', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set, i_set_function, i_bloom, i_ngram, i_text';
SELECT 'ngrambf_v1', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set, i_set_function, i_bloom, i_token, i_text';
SELECT 'text', count() FROM t_named_index WHERE lower(s) = 'hello' SETTINGS ignore_data_skipping_indices = 'i_minmax, i_set, i_set_function, i_bloom, i_token, i_ngram';
SELECT 'all indexes, the column', count() FROM t_named_index WHERE `lower(s)` = 'zzz';
DROP TABLE t_named_index;

-- the expression is in the body of a lambda
DROP TABLE IF EXISTS t_named_lambda;
CREATE TABLE t_named_lambda (arr Array(UInt64), `plus(x, 1)` UInt64)
ENGINE = MergeTree PARTITION BY arraySum(arrayMap(x -> `plus(x, 1)`, arr)) ORDER BY tuple();
INSERT INTO t_named_lambda VALUES ([1], 99);
SELECT 'lambda', count() FROM t_named_lambda WHERE arraySum(arrayMap(x -> x + 1, arr)) = 2;
DROP TABLE t_named_lambda;

-- arrayJoin
DROP TABLE IF EXISTS t_named_array_join;
CREATE TABLE t_named_array_join (ids Array(String), `arrayJoin(ids)` String) ENGINE = MergeTree ORDER BY `arrayJoin(ids)` SETTINGS index_granularity = 1;
INSERT INTO t_named_array_join SELECT ['hit' || toString(number)], 'miss' || toString(number) FROM numbers(100);
SELECT 'ARRAY JOIN', count() FROM t_named_array_join ARRAY JOIN ids AS x WHERE x = 'hit5';
SELECT 'arrayJoin', count() FROM t_named_array_join WHERE arrayJoin(ids) = 'hit5';
DROP TABLE t_named_array_join;
