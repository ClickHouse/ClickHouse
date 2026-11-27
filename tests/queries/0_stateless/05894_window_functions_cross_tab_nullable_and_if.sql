-- `cramersV`, `cramersVBiasCorrected`, `theilsU` and `contingency` over a window that starts at the partition start
-- give the same values as the same aggregates over the rows of the frame when the arguments are `Nullable` and when
-- the function has the `-If` combinator. A row with a NULL argument, a NULL condition or a false condition is not
-- counted.

CREATE TABLE t_cross_tab_rows
(
    t UInt64,
    p UInt64,
    a UInt64,
    b UInt64,
    c UInt8,
    an Nullable(UInt64),
    bn Nullable(UInt64),
    cn Nullable(UInt8),
    s LowCardinality(Nullable(String))
)
ENGINE = MergeTree ORDER BY t;

-- Every `Nullable` column has NULL in a third of the rows, each column in its own rows.
INSERT INTO t_cross_tab_rows
SELECT
    number,
    number % 3,
    number % 7,
    intDiv(number, 5) % 4,
    cityHash64(number, 1) % 2,
    if(cityHash64(number, 2) % 3 = 0, NULL, number % 7),
    if(cityHash64(number, 3) % 3 = 0, NULL, intDiv(number, 5) % 4),
    if(cityHash64(number, 4) % 3 = 0, NULL, cityHash64(number, 1) % 2),
    if(cityHash64(number, 5) % 3 = 0, NULL, toString(number % 7))
FROM numbers(300);

-- Each view prints the number of rows and, for every call, the number of rows where the window value differs from
-- the aggregate over the rows of the frame.
CREATE VIEW v_cross_tab_running AS
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv_first, arrayReduce('cramersV', frame.4, frame.2))),
    countIf(NOT same(cv_second, arrayReduce('cramersV', frame.1, frame.5))),
    countIf(NOT same(cv_both, arrayReduce('cramersV', frame.4, frame.5))),
    countIf(NOT same(cv_no_null, arrayReduce('cramersV', frame.1, frame.2))),
    countIf(NOT same(cv_if, arrayReduce('cramersVIf', frame.1, frame.2, frame.3))),
    countIf(NOT same(cv_if_argument, arrayReduce('cramersVIf', frame.4, frame.2, frame.3))),
    countIf(NOT same(cv_if_condition, arrayReduce('cramersVIf', frame.1, frame.2, frame.6))),
    countIf(NOT same(cv_if_all, arrayReduce('cramersVIf', frame.4, frame.5, frame.6))),
    countIf(NOT same(cv_string, arrayReduce('cramersV', frame.7, frame.2))),
    countIf(NOT same(tu_first, arrayReduce('theilsU', frame.4, frame.2))),
    countIf(NOT same(tu_second, arrayReduce('theilsU', frame.1, frame.5))),
    countIf(NOT same(tu_both, arrayReduce('theilsU', frame.4, frame.5))),
    countIf(NOT same(tu_no_null, arrayReduce('theilsU', frame.1, frame.2))),
    countIf(NOT same(tu_if, arrayReduce('theilsUIf', frame.1, frame.2, frame.3))),
    countIf(NOT same(tu_if_argument, arrayReduce('theilsUIf', frame.4, frame.2, frame.3))),
    countIf(NOT same(tu_if_condition, arrayReduce('theilsUIf', frame.1, frame.2, frame.6))),
    countIf(NOT same(tu_if_all, arrayReduce('theilsUIf', frame.4, frame.5, frame.6))),
    countIf(NOT same(tu_string, arrayReduce('theilsU', frame.7, frame.2))),
    countIf(NOT same(cvb_both, arrayReduce('cramersVBiasCorrected', frame.4, frame.5))),
    countIf(NOT same(cvb_if, arrayReduce('cramersVBiasCorrectedIf', frame.1, frame.2, frame.3))),
    countIf(NOT same(cvb_if_all, arrayReduce('cramersVBiasCorrectedIf', frame.4, frame.5, frame.6))),
    countIf(NOT same(ct_both, arrayReduce('contingency', frame.4, frame.5))),
    countIf(NOT same(ct_if, arrayReduce('contingencyIf', frame.1, frame.2, frame.3))),
    countIf(NOT same(ct_if_all, arrayReduce('contingencyIf', frame.4, frame.5, frame.6)))
FROM
(
    SELECT
        cramersV(an, b) OVER w AS cv_first,
        cramersV(a, bn) OVER w AS cv_second,
        cramersV(an, bn) OVER w AS cv_both,
        cramersV(toNullable(a), toNullable(b)) OVER w AS cv_no_null,
        cramersVIf(a, b, c) OVER w AS cv_if,
        cramersVIf(an, b, c) OVER w AS cv_if_argument,
        cramersVIf(a, b, cn) OVER w AS cv_if_condition,
        cramersVIf(an, bn, cn) OVER w AS cv_if_all,
        cramersV(s, b) OVER w AS cv_string,
        theilsU(an, b) OVER w AS tu_first,
        theilsU(a, bn) OVER w AS tu_second,
        theilsU(an, bn) OVER w AS tu_both,
        theilsU(toNullable(a), toNullable(b)) OVER w AS tu_no_null,
        theilsUIf(a, b, c) OVER w AS tu_if,
        theilsUIf(an, b, c) OVER w AS tu_if_argument,
        theilsUIf(a, b, cn) OVER w AS tu_if_condition,
        theilsUIf(an, bn, cn) OVER w AS tu_if_all,
        theilsU(s, b) OVER w AS tu_string,
        cramersVBiasCorrected(an, bn) OVER w AS cvb_both,
        cramersVBiasCorrectedIf(a, b, c) OVER w AS cvb_if,
        cramersVBiasCorrectedIf(an, bn, cn) OVER w AS cvb_if_all,
        contingency(an, bn) OVER w AS ct_both,
        contingencyIf(a, b, c) OVER w AS ct_if,
        contingencyIf(an, bn, cn) OVER w AS ct_if_all,
        groupArray((a, b, c, an, bn, cn, CAST(s AS Nullable(String)))) OVER w AS frame
    FROM t_cross_tab_rows
    WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
);

CREATE VIEW v_cross_tab_other_frames AS
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv_whole, arrayReduce('cramersV', whole_frame.4, whole_frame.5))),
    countIf(NOT same(cv_if_whole, arrayReduce('cramersVIf', whole_frame.4, whole_frame.5, whole_frame.6))),
    countIf(NOT same(tu_whole, arrayReduce('theilsU', whole_frame.4, whole_frame.5))),
    countIf(NOT same(tu_if_whole, arrayReduce('theilsUIf', whole_frame.4, whole_frame.5, whole_frame.6))),
    countIf(NOT same(cv_default, arrayReduce('cramersV', running_frame.4, running_frame.5))),
    countIf(NOT same(cv_if_default, arrayReduce('cramersVIf', running_frame.4, running_frame.5, running_frame.6))),
    countIf(NOT same(tu_default, arrayReduce('theilsU', running_frame.4, running_frame.5))),
    countIf(NOT same(tu_if_default, arrayReduce('theilsUIf', running_frame.4, running_frame.5, running_frame.6)))
FROM
(
    SELECT
        cramersV(an, bn) OVER whole AS cv_whole,
        cramersVIf(an, bn, cn) OVER whole AS cv_if_whole,
        theilsU(an, bn) OVER whole AS tu_whole,
        theilsUIf(an, bn, cn) OVER whole AS tu_if_whole,
        cramersV(an, bn) OVER (ORDER BY t) AS cv_default,
        cramersVIf(an, bn, cn) OVER (ORDER BY t) AS cv_if_default,
        theilsU(an, bn) OVER (ORDER BY t) AS tu_default,
        theilsUIf(an, bn, cn) OVER (ORDER BY t) AS tu_if_default,
        groupArray((a, b, c, an, bn, cn)) OVER whole AS whole_frame,
        groupArray((a, b, c, an, bn, cn)) OVER (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_frame
    FROM t_cross_tab_rows
    WINDOW whole AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
);

SELECT 'frame from the partition start to the current row';
SELECT * FROM v_cross_tab_running;
SELECT * FROM v_cross_tab_running SETTINGS max_block_size = 7;

SELECT 'frame of the whole partition and the default frame';
SELECT * FROM v_cross_tab_other_frames;
SELECT * FROM v_cross_tab_other_frames SETTINGS max_block_size = 7;

-- The first two rows of every partition have a NULL argument, a NULL condition or a false condition.
-- The second partition has no other rows.
CREATE TABLE t_cross_tab_first_rows (p UInt64, t UInt64, a UInt64, b UInt64, c UInt8, an Nullable(UInt64), cn Nullable(UInt8))
ENGINE = MergeTree ORDER BY (p, t);

INSERT INTO t_cross_tab_first_rows VALUES
    (1, 1, 1, 1, 0, NULL, NULL), (1, 2, 2, 2, 0, NULL, 0), (1, 3, 1, 1, 1, 1, 1), (1, 4, 2, 2, 1, 2, 1), (1, 5, 1, 2, 1, 1, 1),
    (2, 1, 1, 1, 0, NULL, NULL), (2, 2, 2, 2, 0, NULL, 0);

SELECT 'before the first counted row: NULL with a Nullable argument, nan without one';
SELECT
    p,
    t,
    round(cramersV(an, b) OVER w, 6),
    round(cramersVIf(a, b, c) OVER w, 6),
    round(cramersVIf(an, b, c) OVER w, 6),
    round(cramersVIf(a, b, cn) OVER w, 6),
    round(theilsU(an, b) OVER w, 6),
    round(theilsUIf(a, b, c) OVER w, 6),
    round(theilsUIf(an, b, c) OVER w, 6),
    round(theilsUIf(a, b, cn) OVER w, 6)
FROM t_cross_tab_first_rows
WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY p, t;

SELECT 'result types';
SELECT
    toTypeName(cramersV(an, b) OVER ()),
    toTypeName(cramersVIf(a, b, c) OVER ()),
    toTypeName(cramersVIf(an, b, c) OVER ()),
    toTypeName(cramersVIf(a, b, cn) OVER ())
FROM t_cross_tab_first_rows
LIMIT 1;
