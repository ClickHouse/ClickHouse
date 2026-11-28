-- A JOIN runtime filter prunes granules by a `text` index with the `array` tokenizer.

SET enable_join_runtime_filters = 1, enable_join_runtime_filters_index_analysis = 1, use_skip_indexes_on_data_read = 1,
    join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    enable_parallel_replicas = 0;

CREATE TABLE tab_a (id UInt64, s String, INDEX idx s TYPE text(tokenizer = 'array')) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE tab_b (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO tab_a SELECT number, toString(number) FROM numbers(1000);
INSERT INTO tab_b VALUES ('1'), ('2');

SELECT count() FROM tab_a AS a INNER JOIN tab_b AS b ON a.s = b.s SETTINGS log_comment = '05830_text_index';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RuntimeFilterGranulesDropped'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05830_text_index';
