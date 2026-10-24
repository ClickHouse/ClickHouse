-- An outer query cached with `query_cache_use_only_when_data_was_not_changed = 1` certifies its result
-- with the current hash of the tables it reads. A subquery that opts into the query cache without that
-- setting must then not be served from its own, unvalidated entry: the outer query would store the
-- stale rows under the current hash.

DROP TABLE IF EXISTS src_05413;
CREATE TABLE src_05413 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_05413 VALUES (1);

SELECT value FROM
(
    SELECT sum(x) AS value FROM src_05413
    SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600,
        query_cache_use_only_when_data_was_not_changed = 0
)
SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600,
    query_cache_use_only_when_data_was_not_changed = 1;

INSERT INTO src_05413 VALUES (2);

-- The fresh sum, not the one of the subquery's stale entry.
SELECT value FROM
(
    SELECT sum(x) AS value FROM src_05413
    SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600,
        query_cache_use_only_when_data_was_not_changed = 0
)
SETTINGS use_query_cache = 1, query_cache_min_query_duration = 0, query_cache_min_query_runs = 0, query_cache_ttl = 3600,
    query_cache_use_only_when_data_was_not_changed = 1;

DROP TABLE src_05413;
