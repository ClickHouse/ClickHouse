-- Tags: no-async-insert, no-random-settings
-- One insert_deduplication_token covers several input blocks, and a repeat adds no rows.
-- Identical blocks without a token stay distinct.
-- A failed block keeps its token, and a retry fills the gap.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dedup, ts_tokenless, ts_partial;

SELECT '--- three blocks with one token reach the tags and samples tables ---';

CREATE TABLE ts_dedup UUID '05403000-0000-4000-8000-000000000001' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS non_replicated_deduplication_window = 100, max_bytes_to_merge_at_max_space_in_pool = 1
TAGS INNER ENGINE = ReplacingMergeTree
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM `.inner_id.tags.05403000-0000-4000-8000-000000000001`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000001`;

SELECT '--- the same token inserts no duplicate rows ---';

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM `.inner_id.tags.05403000-0000-4000-8000-000000000001`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000001`;

SELECT '--- a different token inserts the samples again ---';

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch2', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM `.inner_id.tags.05403000-0000-4000-8000-000000000001`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000001`;

DROP TABLE ts_dedup;

SELECT '--- three identical blocks without a token all land ---';

CREATE TABLE ts_tokenless UUID '05403000-0000-4000-8000-000000000002' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS non_replicated_deduplication_window = 100, max_bytes_to_merge_at_max_space_in_pool = 1
TAGS INNER ENGINE = ReplacingMergeTree
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;

SELECT '--- a different insert is not collapsed ---';

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('other', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 9.)]);

SELECT count() FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;

SELECT '--- a retry of the first insert adds no rows ---';

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;
SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000002`;

DROP TABLE ts_tokenless;

SELECT '--- the failing insert keeps tags for the committed blocks ---';

CREATE TABLE ts_partial UUID '05403000-0000-4000-8000-000000000003' ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS non_replicated_deduplication_window = 100, max_bytes_to_merge_at_max_space_in_pool = 1
TAGS INNER ENGINE = ReplacingMergeTree
SETTINGS non_replicated_deduplication_window = 100;

ALTER TABLE `.inner_id.samples.05403000-0000-4000-8000-000000000003` ADD CONSTRAINT c CHECK arrayAll(x -> x.2 != 2, samples);

INSERT INTO ts_partial (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('a', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('b', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]),
    ('c', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 3.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT arraySort(groupArray(metric_name)) FROM `.inner_id.tags.05403000-0000-4000-8000-000000000003`;

SELECT '--- the same token inserts the rejected block after the constraint is gone ---';

ALTER TABLE `.inner_id.samples.05403000-0000-4000-8000-000000000003` DROP CONSTRAINT c;

INSERT INTO ts_partial (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('a', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('b', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]),
    ('c', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 3.)]);

SELECT arraySort(groupArray(samples[1].2)) FROM `.inner_id.samples.05403000-0000-4000-8000-000000000003`;
SELECT arraySort(groupArray(metric_name)) FROM `.inner_id.tags.05403000-0000-4000-8000-000000000003`;
SELECT metric_name FROM ts_partial ORDER BY metric_name;

DROP TABLE ts_partial;
