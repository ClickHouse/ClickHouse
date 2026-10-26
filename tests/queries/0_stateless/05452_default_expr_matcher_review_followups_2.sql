-- Regression tests for column matchers in default and index expressions.

DROP TABLE IF EXISTS t_05452_index;
DROP TABLE IF EXISTS t_05452_lambda;
DROP TABLE IF EXISTS t_05452_replacing;
DROP TABLE IF EXISTS t_05452_collapsing;
DROP TABLE IF EXISTS t_05452_missing;

SELECT '-- a tuple-valued skip index expression stays one key after reparsing';
CREATE TABLE t_05452_index
(
    a UInt64,
    b UInt64,
    INDEX idx tuple(tuple(a, b)) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05452_index VALUES (1, 100), (200, 300);
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_05452_index';
ALTER TABLE t_05452_index MODIFY COMMENT 'c';
DETACH TABLE t_05452_index;
ATTACH TABLE t_05452_index;
SELECT expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_05452_index';
SELECT count() FROM t_05452_index WHERE (a, b) = (200, 300) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_05452_index WHERE (a, b) = (200, 300);

SELECT '-- a nested lambda parameter does not capture the substituted column';
CREATE TABLE t_05452_lambda
(
    a UInt8,
    b Array(UInt8) DEFAULT tupleElement(tuple(COLUMNS('^a$') APPLY (x -> arrayMap(a -> x, [1]))), 1),
    c Array(UInt8) DEFAULT tupleElement(tuple(COLUMNS('^a$') APPLY (x -> x + 0, 'p_') REPLACE (arrayMap(a -> p_a, [1]) AS p_a)), 1)
) ENGINE = Memory;
INSERT INTO t_05452_lambda (a) VALUES (7);
SELECT a, b, c FROM t_05452_lambda;

SELECT '-- the version and sign columns are not rematerialized automatically';
CREATE TABLE t_05452_replacing
(
    k UInt64,
    src1 UInt64,
    v UInt64 MATERIALIZED greatest(COLUMNS('^src'))
) ENGINE = ReplacingMergeTree(v) ORDER BY k;
INSERT INTO t_05452_replacing (k, src1) VALUES (1, 1), (1, 2);
ALTER TABLE t_05452_replacing ADD COLUMN src2 UInt64 DEFAULT 100 - src1 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT src1, v FROM t_05452_replacing FINAL;

CREATE TABLE t_05452_collapsing
(
    k UInt64,
    src1 Int8,
    s Int8 MATERIALIZED if(greatest(COLUMNS('^src')) > 0, 1, -1)
) ENGINE = CollapsingMergeTree(s) ORDER BY k;
INSERT INTO t_05452_collapsing (k, src1) VALUES (1, 1);
ALTER TABLE t_05452_collapsing ADD COLUMN src2 Int8 DEFAULT -src1 SETTINGS mutations_sync = 2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT k, s FROM t_05452_collapsing FINAL;

SELECT '-- the automatic rematerialization recomputes a column omitted on insert';
CREATE TABLE t_05452_missing (a UInt64, m UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_05452_missing VALUES (0, 0);
ALTER TABLE t_05452_missing MODIFY COLUMN m UInt64 MATERIALIZED greatest(COLUMNS('^(a|b)$'));
ALTER TABLE t_05452_missing ADD COLUMN b UInt64 DEFAULT a + 1000 SETTINGS mutations_sync = 2;
INSERT INTO t_05452_missing (a) VALUES (0);
SELECT a, b, m FROM t_05452_missing ORDER BY m;

DROP TABLE t_05452_index;
DROP TABLE t_05452_lambda;
DROP TABLE t_05452_replacing;
DROP TABLE t_05452_collapsing;
DROP TABLE t_05452_missing;
