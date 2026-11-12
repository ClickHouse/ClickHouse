-- Bernoulli sampling (`SAMPLE` on a table without a `SAMPLE BY` key) with `bernoulli_sample_seed = 0` draws a fresh
-- sample for every query. The query result cache must treat it as non-deterministic, otherwise a later execution
-- returns the cached sample instead of a fresh one. An explicit nonzero seed is deterministic and stays cacheable.
-- The cache is shared between test runs, so the queries name the table with its database and only their entries are counted.

SET allow_experimental_bernoulli_sample = 1;
SET use_query_cache = 0, query_cache_for_subqueries = 0;
SET enable_reads_from_query_cache = 0, enable_writes_to_query_cache = 1;
SET query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;
SET query_cache_nondeterministic_function_handling = 'throw';
SET query_cache_tag = '05622_bernoulli_sample_query_result_cache';

DROP TABLE IF EXISTS t_bernoulli_qrc;
CREATE TABLE t_bernoulli_qrc (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_bernoulli_qrc SELECT number FROM numbers(10000);

SELECT 'zero seed, throw';
SELECT sum(id) > 0 FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1 SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() > 0 FROM (SELECT id FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1) SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() > 0 FROM (SELECT id FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 0) SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SELECT 'zero seed, ignore';
SELECT sum(id) > 0 FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1 SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count() FROM system.query_cache WHERE tag = '05622_bernoulli_sample_query_result_cache' AND query LIKE '%' || currentDatabase() || '.t_bernoulli_qrc%';

SELECT 'zero seed, save';
SELECT sum(id) > 0 FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1 SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0, query_cache_nondeterministic_function_handling = 'save';
SELECT count() FROM system.query_cache WHERE tag = '05622_bernoulli_sample_query_result_cache' AND query LIKE '%' || currentDatabase() || '.t_bernoulli_qrc%';

SELECT 'nonzero seed: cached';
SELECT sum(id) > 0 FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1 SETTINGS use_query_cache = 1, bernoulli_sample_seed = 42;
SELECT count() > 0 FROM (SELECT id FROM {CLICKHOUSE_DATABASE:Identifier}.t_bernoulli_qrc SAMPLE 0.1) SETTINGS use_query_cache = 1, bernoulli_sample_seed = 42;
SELECT count() FROM system.query_cache WHERE tag = '05622_bernoulli_sample_query_result_cache' AND query LIKE '%' || currentDatabase() || '.t_bernoulli_qrc%';

DROP TABLE t_bernoulli_qrc;
