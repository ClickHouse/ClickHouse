DROP TABLE IF EXISTS t_ren;
DROP TABLE IF EXISTS t_key;

CREATE TABLE t_ren
(
    j Tuple(a Date, b Int64),
    v Int64,
    x Int64 MATERIALIZED j.b,
    INDEX idx j.b TYPE minmax GRANULARITY 1,
    CONSTRAINT c CHECK j.b > 0
) ENGINE = MergeTree ORDER BY v TTL j.a + INTERVAL 100 YEAR;
INSERT INTO t_ren (j, v) VALUES (('2026-01-01', 1), 1);

ALTER TABLE t_ren RENAME COLUMN j TO jj;

SELECT default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_ren' AND name = 'x';
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_ren' AND name = 'idx';
SELECT create_table_query LIKE '%CHECK jj.b > 0%', create_table_query LIKE '%TTL jj.a + toIntervalYear(100)%' FROM system.tables WHERE database = currentDatabase() AND name = 't_ren';

INSERT INTO t_ren (jj, v) VALUES (('2026-01-02', 3), 2);
SELECT x, jj.b FROM t_ren ORDER BY v;

-- A column whose subcolumn is in a key is a key column: the rename is refused, not broken.
CREATE TABLE t_key (j Tuple(a Int64, b Int64), v Int64) ENGINE = MergeTree PARTITION BY j.a ORDER BY j.b;
ALTER TABLE t_key RENAME COLUMN j TO jj; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_ren;
DROP TABLE t_key;
