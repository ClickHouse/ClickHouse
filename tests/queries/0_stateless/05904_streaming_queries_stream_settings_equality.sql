-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: STREAM table expressions are not read through parallel replicas; every streaming-queries test carries this tag.

-- Two table expressions over the same table may carry different STREAM modifiers. The query tree
-- compares them through `StreamSettings::operator==` (src/Core/Streaming/Settings.cpp), and the
-- result of that comparison decides a user-visible outcome here: an unqualified identifier that
-- resolves on both sides of a comma join is ambiguous unless the two resolved columns are equal.
-- `IdentifierResolver::tryResolveIdentifierFromCrossJoin` compares the two column sources with
-- `compare_aliases = false`, so the table aliases do not decide it -- the STREAM settings do.
-- Two sides whose STREAM settings are equal deliver the same rows, so either side answers the
-- identifier and the query returns a count. Two sides whose STREAM settings differ deliver different
-- rows, so the identifier is genuinely ambiguous and the query is rejected (AMBIGUOUS_IDENTIFIER).
-- `single_join_prefer_left_table` (default 1, docs/reference/settings/session-settings/other.mdx:1733)
-- is turned off, because it resolves such an ambiguity by preferring the left table before it is
-- reported. Each pair of modifier sets below stops the comparison at a different field: subscription,
-- ordering, cursor presence, cursor content, watermark presence, watermark expression.

SET enable_streaming_queries = 1;
SET single_join_prefer_left_table = 0;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_stream_settings_eq;

CREATE TABLE t_stream_settings_eq (k UInt32, ts DateTime)
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_stream_settings_eq VALUES (1, '2024-01-01 00:00:00'), (2, '2024-01-01 00:00:01');

-- Two sides with equal modifiers really do deliver the same two keys: 4 pairs, 2 of them matching.
SELECT 'snapshot', count(), countIf(a.k = b.k)
FROM t_stream_settings_eq AS a STREAM BOUNDED, t_stream_settings_eq AS b STREAM BOUNDED;

-- Equal settings: every field compares equal, so the unqualified `k` is answered by either side.
SELECT 'equal', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED, t_stream_settings_eq AS b STREAM BOUNDED;

-- Equal cursors: the two cursor trees are compared by content and match.
SELECT 'equal cursors', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED CURSOR {'all': {'block_number': 0}},
     t_stream_settings_eq AS b STREAM BOUNDED CURSOR {'all': {'block_number': 0}};

-- Equal watermarks: the time attribute and the watermark expression match.
SELECT 'equal watermarks', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED WATERMARK FOR ts AS ts,
     t_stream_settings_eq AS b STREAM BOUNDED WATERMARK FOR ts AS ts;

-- The subscription differs: a bounded stream delivers one snapshot, an unbounded one never ends.
SELECT 'subscription', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED,
     t_stream_settings_eq AS b STREAM; -- { serverError AMBIGUOUS_IDENTIFIER }

-- The ordering differs: UNORDERED drops the order inside a snapshot.
SELECT 'ordering', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED,
     t_stream_settings_eq AS b STREAM BOUNDED UNORDERED; -- { serverError AMBIGUOUS_IDENTIFIER }

-- A cursor on one side only: that side resumes, the other starts from the beginning.
SELECT 'cursor presence', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED,
     t_stream_settings_eq AS b STREAM BOUNDED CURSOR {'all': {'block_number': 0}}; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Two cursors that resume at different positions.
SELECT 'cursor content', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED CURSOR {'all': {'block_number': 0}},
     t_stream_settings_eq AS b STREAM BOUNDED CURSOR {'all': {'block_number': 1}}; -- { serverError AMBIGUOUS_IDENTIFIER }

-- A watermark on one side only.
SELECT 'watermark presence', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED,
     t_stream_settings_eq AS b STREAM BOUNDED WATERMARK FOR ts AS ts; -- { serverError AMBIGUOUS_IDENTIFIER }

-- Two watermarks with different expressions over the same time attribute.
SELECT 'watermark expression', count(k)
FROM t_stream_settings_eq AS a STREAM BOUNDED WATERMARK FOR ts AS ts,
     t_stream_settings_eq AS b STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 1 SECOND; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE t_stream_settings_eq;
