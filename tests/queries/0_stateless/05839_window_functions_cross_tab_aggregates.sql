-- `cramersV`, `cramersVBiasCorrected`, `theilsU` and `contingency` over a window give the same values as the
-- same aggregates over the rows of the frame: for frames that start at the partition start, for moving frames,
-- with combinators and `Nullable` arguments, and when a state leaves the window with `-State` and is merged later.
-- Every query prints the number of rows and the number of rows where the two values differ.

CREATE TABLE t_cross_tab (t UInt64, p UInt64, a UInt64, b UInt64, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY t;
INSERT INTO t_cross_tab
SELECT number, number % 3, number % 7, intDiv(number, 5) % 4, if(number % 11 = 0, NULL, number % 5) FROM numbers(300);

SELECT 'frame from the partition start to the current row';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv, arrayReduce('cramersV', frame.1, frame.2))),
    countIf(NOT same(cvb, arrayReduce('cramersVBiasCorrected', frame.1, frame.2))),
    countIf(NOT same(tu, arrayReduce('theilsU', frame.1, frame.2))),
    countIf(NOT same(ct, arrayReduce('contingency', frame.1, frame.2)))
FROM
(
    SELECT
        cramersV(a, b) OVER w AS cv,
        cramersVBiasCorrected(a, b) OVER w AS cvb,
        theilsU(a, b) OVER w AS tu,
        contingency(a, b) OVER w AS ct,
        groupArray((a, b)) OVER w AS frame
    FROM t_cross_tab
    WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
);

SELECT 'frame of the whole partition';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv, arrayReduce('cramersV', frame.1, frame.2))),
    countIf(NOT same(cvb, arrayReduce('cramersVBiasCorrected', frame.1, frame.2))),
    countIf(NOT same(tu, arrayReduce('theilsU', frame.1, frame.2))),
    countIf(NOT same(ct, arrayReduce('contingency', frame.1, frame.2)))
FROM
(
    SELECT
        cramersV(a, b) OVER w AS cv,
        cramersVBiasCorrected(a, b) OVER w AS cvb,
        theilsU(a, b) OVER w AS tu,
        contingency(a, b) OVER w AS ct,
        groupArray((a, b)) OVER w AS frame
    FROM t_cross_tab
    WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
);

SELECT 'frame from the partition start to a following row';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv, arrayReduce('cramersV', frame.1, frame.2))),
    countIf(NOT same(cvb, arrayReduce('cramersVBiasCorrected', frame.1, frame.2))),
    countIf(NOT same(tu, arrayReduce('theilsU', frame.1, frame.2))),
    countIf(NOT same(ct, arrayReduce('contingency', frame.1, frame.2)))
FROM
(
    SELECT
        cramersV(a, b) OVER w AS cv,
        cramersVBiasCorrected(a, b) OVER w AS cvb,
        theilsU(a, b) OVER w AS tu,
        contingency(a, b) OVER w AS ct,
        groupArray((a, b)) OVER w AS frame
    FROM t_cross_tab
    WINDOW w AS (ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING)
);

SELECT 'moving frame';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv, arrayReduce('cramersV', frame.1, frame.2))),
    countIf(NOT same(cvb, arrayReduce('cramersVBiasCorrected', frame.1, frame.2))),
    countIf(NOT same(tu, arrayReduce('theilsU', frame.1, frame.2))),
    countIf(NOT same(ct, arrayReduce('contingency', frame.1, frame.2)))
FROM
(
    SELECT
        cramersV(a, b) OVER w AS cv,
        cramersVBiasCorrected(a, b) OVER w AS cvb,
        theilsU(a, b) OVER w AS tu,
        contingency(a, b) OVER w AS ct,
        groupArray((a, b)) OVER w AS frame
    FROM t_cross_tab
    WINDOW w AS (ORDER BY t ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
);

SELECT 'combinators and Nullable arguments';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv_if, arrayReduce('cramersVIf', frame.1, frame.2, frame.3))),
    countIf(NOT same(cv_or_null, arrayReduce('cramersVOrNull', frame.1, frame.2))),
    countIf(NOT same(tu_nullable, arrayReduce('theilsU', frame.1, frame.4))),
    countIf(NOT same(ct_nullable, arrayReduce('contingency', frame.4, frame.2)))
FROM
(
    SELECT
        cramersVIf(a, b, t % 2 = 0) OVER w AS cv_if,
        cramersVOrNull(a, b) OVER w AS cv_or_null,
        theilsU(a, n) OVER w AS tu_nullable,
        contingency(n, b) OVER w AS ct_nullable,
        groupArray((a, b, t % 2 = 0, n)) OVER w AS frame
    FROM t_cross_tab
    WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
);

SELECT 'states returned by a window function';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(finalizeAggregation(cv_state), cv)),
    countIf(NOT same(finalizeAggregation(tu_state), tu))
FROM
(
    SELECT
        t,
        cramersVState(a, b) OVER w AS cv_state,
        theilsUState(a, b) OVER w AS tu_state,
        cramersV(a, b) OVER w AS cv,
        theilsU(a, b) OVER w AS tu
    FROM t_cross_tab
    WINDOW w AS (PARTITION BY p ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ORDER BY t DESC
)
SETTINGS query_plan_remove_redundant_sorting = 0, max_bytes_before_external_sort = 1000, max_bytes_ratio_before_external_sort = 0,
    max_block_size = 8;

-- One state per ten rows: the state of row `t` holds the rows of its ten.
CREATE TABLE t_cross_tab_states
(
    t UInt64,
    cv AggregateFunction(cramersV, UInt64, UInt64),
    tu AggregateFunction(theilsU, UInt64, UInt64)
)
ENGINE = MergeTree ORDER BY t;

INSERT INTO t_cross_tab_states
SELECT t, cramersVState(a, b) OVER w, theilsUState(a, b) OVER w
FROM t_cross_tab
WINDOW w AS (PARTITION BY intDiv(t, 10) ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING);

SELECT 'stored states merged in GROUP BY';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    same((SELECT cramersVMerge(cv) FROM t_cross_tab_states WHERE t % 10 = 0), (SELECT cramersV(a, b) FROM t_cross_tab)),
    same((SELECT theilsUMerge(tu) FROM t_cross_tab_states WHERE t % 10 = 0), (SELECT theilsU(a, b) FROM t_cross_tab));

SELECT 'stored states merged over a window';
WITH (x, y) -> ifNull(abs(x - y) < 1e-9 OR (isNaN(x) AND isNaN(y)), isNull(x) AND isNull(y)) AS same
SELECT
    count(),
    countIf(NOT same(cv_merged, cv_plain)),
    countIf(NOT same(tu_merged, tu_plain))
FROM
(
    SELECT
        t + 9 AS last_row,
        cramersVMerge(cv) OVER (ORDER BY t) AS cv_merged,
        theilsUMerge(tu) OVER (ORDER BY t) AS tu_merged
    FROM t_cross_tab_states
    WHERE t % 10 = 0
) AS merged
INNER JOIN
(
    SELECT t, cramersV(a, b) OVER (ORDER BY t) AS cv_plain, theilsU(a, b) OVER (ORDER BY t) AS tu_plain FROM t_cross_tab
) AS plain ON merged.last_row = plain.t;
