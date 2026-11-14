SET enable_analyzer = 1;
SET use_query_cache = 0;
SET use_query_condition_cache = 0;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_text_index_add_hint = 1;
SET text_index_hint_max_selectivity = 0.2;
SET use_skip_indexes_on_data_read = 1;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS early_candidates;
CREATE TABLE early_candidates
(
    id UInt64,
    n UInt64,
    k LowCardinality(Nullable(String)),
    s String,
    INDEX k_idx k TYPE text(tokenizer = 'array'),
    INDEX s_idx s TYPE text(tokenizer = ngrams(3))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024;
INSERT INTO early_candidates SELECT number, number + 1, if(number % 2 = 0, 'hit', 'miss'),
    if(number % 16 = 1, concat('needle phrase ', repeat('x', 200)), repeat('z', 200))
FROM numbers(32768);

-- The phrase occurs on 1/16 of rows, below the default hint bypass threshold.
-- Both postings cover every mark but their intersection is empty. The numeric
-- condition still reads n, but the phrase hint can eliminate the remaining
-- rows before the reader fetches s for verification.
SELECT count() FROM early_candidates
PREWHERE n > 0 AND k = 'hit' AND s LIKE '%needle phrase%'
SETTINGS log_comment = 'text_index_hint_prewhere_empty', log_queries = 1,
    log_queries_probability = 1, log_queries_min_query_duration_ms = 0;
SYSTEM FLUSH LOGS query_log;
SELECT read_bytes <= 32768 * (8 + 2) FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = 'text_index_hint_prewhere_empty' AND event_time >= now() - 60
ORDER BY event_time_microseconds DESC LIMIT 1;

-- An OR does not imply that both candidate predicates hold.
SELECT count() FROM early_candidates PREWHERE k = 'hit' OR s LIKE '%needle phrase%';
DROP TABLE early_candidates;

-- The first part lacks indexes. Preserve exact fallback, hint verification,
-- Nullable predicates and aliases selected downstream in both parts.
DROP TABLE IF EXISTS partial_candidates;
CREATE TABLE partial_candidates (id UInt64, k String, s Nullable(String))
ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES partial_candidates;
INSERT INTO partial_candidates VALUES (1, 'hit', 'needle phrase'), (2, 'miss', NULL);
ALTER TABLE partial_candidates ADD INDEX k_idx k TYPE text(tokenizer = 'splitByNonAlpha');
ALTER TABLE partial_candidates ADD INDEX s_idx s TYPE text(tokenizer = ngrams(3));
-- Row 4 contains every query trigram but not the requested phrase.
INSERT INTO partial_candidates VALUES (3, 'hit', 'needle phrase'),
    (4, 'hit', 'nee|eed|edl|dle|le |e p| ph|phr|hra|ras|ase');
SELECT id, hasToken(k, 'hit') AS x, s LIKE '%needle phrase%' AS y
FROM partial_candidates PREWHERE x AND y ORDER BY id;
SELECT id, hasToken(k, 'hit') AS x, s LIKE '%needle phrase%' AS y
FROM partial_candidates PREWHERE x AND y ORDER BY id
SETTINGS query_plan_direct_read_from_text_index = 0;
DROP TABLE partial_candidates;
