-- Tags: no-fasttest
-- no-fasttest: the vector similarity index (USearch) is not built in the fast test.

-- Without rescoring, a query on a `dotProduct` vector similarity index takes the values from the index, which stores
-- `1 - dotProduct`. The query must return the rows with the largest dot product, in descending order, with the real values.

SET enable_parallel_replicas = 0;
SET vector_search_with_rescoring = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'dotProduct', 2)
)
ENGINE = MergeTree PARTITION BY id >= 10 ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO tab SELECT number, [0, number + 1] FROM numbers(20);

SELECT id, dotProduct(vec, [0., 1.]) FROM tab ORDER BY dotProduct(vec, [0., 1.]) DESC LIMIT 3;
SELECT id, dotProduct(vec, CAST([0., 1.] AS Array(Float32))) FROM tab ORDER BY dotProduct(vec, CAST([0., 1.] AS Array(Float32))) DESC LIMIT 3;

-- The vector column is replaced by `_distance`, i.e. the values above come from the index.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT id, dotProduct(vec, [0., 1.]) FROM tab ORDER BY dotProduct(vec, [0., 1.]) DESC LIMIT 3)
WHERE explain LIKE '%\\_distance%';

DROP TABLE tab;
