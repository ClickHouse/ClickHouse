SET enable_analyzer = 1, max_threads = 1, max_block_size = 65536;
SET enable_multiple_prewhere_read_steps = 1, use_query_cache = 0, use_query_condition_cache = 0;
SET optimize_use_projections = 1, prefer_optimize_projection = 1;

DROP TABLE IF EXISTS aliased_conjunction;
CREATE TABLE aliased_conjunction
(
    id UInt64, g UInt8, z String,
    PROJECTION p (SELECT id, g, z ORDER BY (id, g))
) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO aliased_conjunction SELECT number, number % 2,
    if(number % 16 = 1, 'hit', repeat('z', 200)) FROM numbers(32768);

-- The projection reader names its filter with an alias. The first conjunct
-- rejects every row, so only the one-byte g column needs to be read.
SELECT count() FROM aliased_conjunction PREWHERE g = g + 2 AND z = 'hit'
SETTINGS force_optimize_projection_name = 'p', log_comment = 'aliased_conjunction_read',
    log_queries = 1, log_queries_probability = 1, log_queries_min_query_duration_ms = 0;
SYSTEM FLUSH LOGS query_log;
SELECT read_bytes <= 32768 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'aliased_conjunction_read'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- A NULL input to AND still needs its second argument to decide the result.
SELECT count() FROM aliased_conjunction PREWHERE nullIf(g, 1) AND intDiv(1, 1 - g) > 0
SETTINGS force_optimize_projection_name = 'p', short_circuit_function_evaluation = 'enable'; -- { serverError ILLEGAL_DIVISION }

-- The row numbers must refer to the block before filtering g.
SELECT count() FROM aliased_conjunction PREWHERE g = 1 AND rowNumberInBlock() % 2 = 0
SETTINGS force_optimize_projection_name = 'p';
DROP TABLE aliased_conjunction;

-- The hint's false positives cannot be parsed as dates. The LIKE verification
-- must reject them before a later condition on the same column sees them.
DROP TABLE IF EXISTS guarded_dates;
CREATE TABLE guarded_dates (id UInt64, s String, INDEX txt s TYPE text(tokenizer = ngrams(3)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1024;
INSERT INTO guarded_dates SELECT number,
    multiIf(number % 16 = 0, '1970-01-02', number % 16 = 1, '197|970|70-|0-0|-01|01-|1-0|-02', 'unrelated')
FROM numbers(32768);
SELECT count() FROM guarded_dates
PREWHERE s LIKE '%1970-01-02%' AND addDays(s, 0) < toDateTime('2020-01-01')
SETTINGS query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1,
    text_index_hint_max_selectivity = 0.2, use_skip_indexes_on_data_read = 1;
DROP TABLE guarded_dates;
