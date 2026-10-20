#!/usr/bin/env bash

# Regression test: a merge builds its result part (and the parts of the merged projections) with the settings
# snapshot its memory reservation was priced against, including a projection's own WITH SETTINGS. The nested
# projection merge used to construct its part from the live table settings without the projection's override,
# so a compact projection overriding write_marks_for_substreams_in_compact_parts = 0 on a table where it is 1
# came out of a merge with per-substream marks, although the reservation priced (and inserts wrote) per-column
# marks.
#
# The observable is the size of one mark of the projection part (compress_marks = 0, so it is exact): it must be
# the same for the inserted projection parts and for the merged one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_projection_marks (k UInt64, v Tuple(a UInt64, b String, c Array(UInt64)),
        PROJECTION p (SELECT k, v ORDER BY v.a) WITH SETTINGS (write_marks_for_substreams_in_compact_parts = 0))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000,
             write_marks_for_substreams_in_compact_parts = 1, compress_marks = 0, index_granularity = 100;

    SYSTEM STOP MERGES t_merge_projection_marks;
    INSERT INTO t_merge_projection_marks SELECT number, (number, toString(number), range(number % 5)) FROM numbers(1000);
    INSERT INTO t_merge_projection_marks SELECT number, (number, toString(number), range(number % 5)) FROM numbers(1000, 1000);

    CREATE TABLE inserted_mark_sizes ENGINE = Memory AS
        SELECT DISTINCT part_type, marks_bytes / marks AS mark_size FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_projection_marks' AND active;

    SYSTEM START MERGES t_merge_projection_marks;
    OPTIMIZE TABLE t_merge_projection_marks FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_projection_marks' AND active;
    SELECT (SELECT groupArray((part_type, mark_size)) FROM inserted_mark_sizes)
         = (SELECT groupArray((part_type, marks_bytes / marks)) FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_projection_marks' AND active);
"
