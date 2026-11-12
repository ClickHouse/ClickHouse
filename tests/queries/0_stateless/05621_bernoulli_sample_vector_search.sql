-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Bernoulli sampling (`SAMPLE` on a table without a `SAMPLE BY` key) drops rows inside the reader, after the vector
-- similarity index shortlisted its candidates. It must count as an additional filter of the vector search: otherwise
-- the shortlist can be discarded entirely by the sample, and `vector_search_filter_strategy = 'prefilter'` is ignored.

SET enable_parallel_replicas = 0;
SET allow_experimental_bernoulli_sample = 1;
SET bernoulli_sample_seed = 42;

DROP TABLE IF EXISTS t_bernoulli_vec;

CREATE TABLE t_bernoulli_vec (id UInt32, vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO t_bernoulli_vec SELECT number, [toFloat32(number), 0] FROM numbers(20000);
OPTIMIZE TABLE t_bernoulli_vec FINAL;

SELECT 'no sample, prefilter: uses the index';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_bernoulli_vec ORDER BY L2Distance(vec, [0., 0.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

SELECT 'sample, prefilter: exact search';
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_bernoulli_vec SAMPLE 0.01 ORDER BY L2Distance(vec, [0., 0.]) LIMIT 1
        SETTINGS vector_search_filter_strategy = 'prefilter')
WHERE explain LIKE '%vector_similarity%';

SET vector_search_filter_strategy = 'prefilter';

SELECT 'sample, prefilter: same result with and without the index';
SELECT id FROM t_bernoulli_vec SAMPLE 0.01 ORDER BY L2Distance(vec, [0., 0.]) LIMIT 1;
SELECT id FROM t_bernoulli_vec SAMPLE 0.01 ORDER BY L2Distance(vec, [0., 0.]) LIMIT 1 SETTINGS use_skip_indexes = 0;

DROP TABLE t_bernoulli_vec;
