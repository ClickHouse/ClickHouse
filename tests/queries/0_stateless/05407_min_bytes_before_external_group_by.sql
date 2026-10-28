SET max_threads = 1;
SET max_block_size = 65536;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_bytes_before_external_group_by = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

-- The query is over the threshold from the first block, so without a minimum every block becomes a separate file.
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers(2000000) GROUP BY k)
SETTINGS min_bytes_before_external_group_by = 0, log_comment = '05407_no_minimum';

-- With a minimum, the state of the thread grows to the minimum before it is written, so there are fewer files.
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers(2000000) GROUP BY k)
SETTINGS min_bytes_before_external_group_by = '16Mi', log_comment = '05407_minimum_16Mi';

-- A minimum larger than the whole state means nothing is written at all.
SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers(2000000) GROUP BY k)
SETTINGS min_bytes_before_external_group_by = '1Gi', log_comment = '05407_minimum_1Gi';

SYSTEM FLUSH LOGS query_log;

WITH
    (SELECT ProfileEvents['ExternalAggregationWritePart'] FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_no_minimum') AS no_minimum,
    (SELECT ProfileEvents['ExternalAggregationWritePart'] FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_minimum_16Mi') AS minimum_16Mi,
    (SELECT ProfileEvents['ExternalAggregationWritePart'] FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_minimum_1Gi') AS minimum_1Gi
SELECT no_minimum > 10, minimum_16Mi > 0, minimum_16Mi < no_minimum, minimum_1Gi;
