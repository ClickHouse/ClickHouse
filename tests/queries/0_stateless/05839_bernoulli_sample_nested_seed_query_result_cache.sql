-- Tags: no-parallel
-- no-parallel: settings profiles are server-global and their names cannot be made unique per run.

-- The query result cache treats Bernoulli sampling (`SAMPLE` on a table without a `SAMPLE BY` key) with
-- `bernoulli_sample_seed = 0` as non-deterministic. It must judge that by the seed the sampled (sub)query really
-- runs with: a nested `SETTINGS` clause can set the zero seed with any spelling of the value, or through a profile,
-- and it can also replace an outer zero seed with a deterministic one.

SET allow_experimental_bernoulli_sample = 1;
SET use_query_cache = 0, query_cache_for_subqueries = 0;
SET enable_reads_from_query_cache = 0, enable_writes_to_query_cache = 1;
SET query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;
SET query_cache_nondeterministic_function_handling = 'throw';
SET query_cache_tag = '05839_bernoulli_sample_nested_seed';

DROP TABLE IF EXISTS t_bernoulli_nested_seed;
CREATE TABLE t_bernoulli_nested_seed (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_bernoulli_nested_seed SELECT number FROM numbers(10000);

DROP SETTINGS PROFILE IF EXISTS profile_05839_zero_seed;
DROP SETTINGS PROFILE IF EXISTS profile_05839_fixed_seed;
CREATE SETTINGS PROFILE profile_05839_zero_seed SETTINGS bernoulli_sample_seed = 0;
CREATE SETTINGS PROFILE profile_05839_fixed_seed SETTINGS bernoulli_sample_seed = 42;

SELECT 'nested quoted zero seed';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1 SETTINGS bernoulli_sample_seed = '0')
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SELECT 'nested profile with the zero seed';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1 SETTINGS profile = 'profile_05839_zero_seed')
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SELECT 'the profile applied last wins over an explicit seed';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 1, profile = 'profile_05839_zero_seed')
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SELECT 'nested nonzero seed under an outer zero seed';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1 SETTINGS bernoulli_sample_seed = 42)
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0;

SELECT 'nested profile with a nonzero seed under an outer zero seed';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1 SETTINGS profile = 'profile_05839_fixed_seed')
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 0;

SELECT 'sampled subquery next to a subquery with the zero seed and no sample';
SELECT count() > 0 FROM (SELECT id FROM t_bernoulli_nested_seed SAMPLE 0.1) AS a, (SELECT 1 SETTINGS bernoulli_sample_seed = 0) AS b
SETTINGS use_query_cache = 1, bernoulli_sample_seed = 42;

SYSTEM DROP QUERY CACHE TAG '05839_bernoulli_sample_nested_seed';
DROP SETTINGS PROFILE profile_05839_zero_seed;
DROP SETTINGS PROFILE profile_05839_fixed_seed;
DROP TABLE t_bernoulli_nested_seed;
