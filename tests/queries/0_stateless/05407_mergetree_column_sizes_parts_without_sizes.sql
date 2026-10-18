-- Table-level column sizes must stay equal to the sum over active parts when parts without computed
-- sizes are committed (attached, cloned) and removed before the sizes are read again.

DROP TABLE IF EXISTS t_sizes;
DROP TABLE IF EXISTS t_sizes_src;
DROP TABLE IF EXISTS t_sizes_snapshot;

CREATE TABLE t_sizes (a UInt64, s String) ENGINE = MergeTree ORDER BY a
SETTINGS columns_and_secondary_indices_sizes_lazy_calculation = 1, min_bytes_for_wide_part = 0;
CREATE TABLE t_sizes_src AS t_sizes;
CREATE TABLE t_sizes_snapshot (name String, compressed UInt64, uncompressed UInt64, marks UInt64) ENGINE = Memory;

SYSTEM STOP MERGES t_sizes;

INSERT INTO t_sizes SELECT number, toString(number) FROM numbers(1000);
INSERT INTO t_sizes SELECT number, repeat('x', number % 100) FROM numbers(2000);

-- Each check snapshots `system.columns` first, so the parts that are not accounted for yet are
-- measured by that read and not by the read of `system.parts_columns`.
SELECT 'initial';
INSERT INTO t_sizes_snapshot SELECT name, data_compressed_bytes, data_uncompressed_bytes, marks_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_sizes';
SELECT s.name, s.compressed = p.compressed, s.uncompressed = p.uncompressed, s.marks = p.marks, s.compressed > 0
FROM t_sizes_snapshot AS s
INNER JOIN
(
    SELECT column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS uncompressed, sum(column_marks_bytes) AS marks
    FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sizes' AND active GROUP BY column
) AS p ON s.name = p.column
ORDER BY s.name;

SELECT 'attach partition from';
INSERT INTO t_sizes_src SELECT number, repeat('y', number % 50) FROM numbers(3000);
ALTER TABLE t_sizes ATTACH PARTITION tuple() FROM t_sizes_src;
TRUNCATE TABLE t_sizes_snapshot;
INSERT INTO t_sizes_snapshot SELECT name, data_compressed_bytes, data_uncompressed_bytes, marks_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_sizes';
SELECT s.name, s.compressed = p.compressed, s.uncompressed = p.uncompressed, s.marks = p.marks, s.compressed > 0
FROM t_sizes_snapshot AS s
INNER JOIN
(
    SELECT column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS uncompressed, sum(column_marks_bytes) AS marks
    FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sizes' AND active GROUP BY column
) AS p ON s.name = p.column
ORDER BY s.name;

-- The attached part is removed by the merge before anything reads the sizes again.
SELECT 'attach part, then merge';
ALTER TABLE t_sizes DETACH PART 'all_1_1_0';
ALTER TABLE t_sizes ATTACH PART 'all_1_1_0';
SYSTEM START MERGES t_sizes;
OPTIMIZE TABLE t_sizes FINAL;
TRUNCATE TABLE t_sizes_snapshot;
INSERT INTO t_sizes_snapshot SELECT name, data_compressed_bytes, data_uncompressed_bytes, marks_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_sizes';
SELECT s.name, s.compressed = p.compressed, s.uncompressed = p.uncompressed, s.marks = p.marks, s.compressed > 0
FROM t_sizes_snapshot AS s
INNER JOIN
(
    SELECT column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS uncompressed, sum(column_marks_bytes) AS marks
    FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sizes' AND active GROUP BY column
) AS p ON s.name = p.column
ORDER BY s.name;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sizes' AND active;

DROP TABLE t_sizes;
DROP TABLE t_sizes_src;
DROP TABLE t_sizes_snapshot;
