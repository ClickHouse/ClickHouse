-- Tags: no-parallel, no-fasttest, no-parallel-replicas
-- no-parallel: the replays assert cache hits, and a concurrent ATTACH TABLE or CREATE TABLE with an explicit UUID clears the whole query condition cache
-- no-fasttest: prettyPrintJSON needs rapidjson
-- no-parallel-replicas: parallel replicas move the query condition cache lookups to the replicas

-- prettyPrintJSON and JSONArrayLength capture the format settings, also behind the Dynamic and Variant adaptors.
-- A query with other format settings must not get the verdict cached by the first one.

SELECT 'JSONArrayLength of a named Tuple';
DROP TABLE IF EXISTS t_qcc_tuple;
CREATE TABLE t_qcc_tuple (id UInt64, t Tuple(x UInt8, y String)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_qcc_tuple;
INSERT INTO t_qcc_tuple SELECT number, (1, 'a') FROM numbers(20000);
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_tuple WHERE JSONArrayLength(t) IS NULL
SETTINGS use_query_condition_cache = 1, output_format_json_named_tuples_as_objects = 0, log_comment = '05875_tuple_1_populate';
SELECT count() FROM t_qcc_tuple WHERE JSONArrayLength(t) IS NULL
SETTINGS use_query_condition_cache = 1, output_format_json_named_tuples_as_objects = 0, log_comment = '05875_tuple_2_replay';
SELECT count() FROM t_qcc_tuple WHERE JSONArrayLength(t) IS NULL
SETTINGS use_query_condition_cache = 1, output_format_json_named_tuples_as_objects = 1, log_comment = '05875_tuple_3_other';
DROP TABLE t_qcc_tuple;

SELECT 'prettyPrintJSON of a Dynamic subcolumn and of a Variant';
-- startsWith and not `=`: a comparison hashes the format settings by itself.
DROP TABLE IF EXISTS t_qcc;
CREATE TABLE t_qcc (id UInt64, j JSON, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES t_qcc;
INSERT INTO t_qcc SELECT number, '{"d":18446744073709551615}', 18446744073709551615::UInt64 FROM numbers(20000);
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(j.d), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 0, log_comment = '05875_dynamic_1_populate';
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(j.d), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 0, log_comment = '05875_dynamic_2_replay';
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(j.d), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 1, log_comment = '05875_dynamic_3_other';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(v), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 0, log_comment = '05875_variant_1_populate';
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(v), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 0, log_comment = '05875_variant_2_replay';
SELECT count() FROM t_qcc WHERE startsWith(prettyPrintJSON(v), '"')
SETTINGS use_query_condition_cache = 1, output_format_json_quote_64bit_integers = 1, log_comment = '05875_variant_3_other';
DROP TABLE t_qcc;

SYSTEM FLUSH LOGS query_log;
-- The replay reuses the entry, the other setting does not.
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^05875_(tuple|dynamic|variant)_')
ORDER BY log_comment;
