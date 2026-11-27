DROP TABLE IF EXISTS squality_correlated_order_by;

SET empty_result_for_aggregation_by_empty_set = 0;
SET aggregate_functions_null_for_empty = 0;
SET join_use_nulls = 0;

CREATE TABLE squality_correlated_order_by (a Int32, b Int32) ENGINE = Memory;
INSERT INTO squality_correlated_order_by VALUES (1, 2), (2, 1), (3, 3);

SELECT
    coalesce((SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b), 0) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY 1;

SELECT
    coalesce((SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b), 0) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY cnt;

SELECT
    EXISTS(SELECT 1 FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS present
FROM squality_correlated_order_by AS outer_table
ORDER BY 1;

SELECT cnt
FROM
(
    SELECT
        outer_table.a,
        coalesce((SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b), 0) AS cnt
    FROM squality_correlated_order_by AS outer_table
    ORDER BY cnt
)
ORDER BY cnt;

SELECT
    coalesce((SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b), 0) AS cnt,
    coalesce((SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b), 0) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY cnt;

SELECT number IN (1, 2) AS present
FROM numbers(3)
ORDER BY 1;

SELECT tuple(
    coalesce((SELECT max(i.number) FROM numbers(3) AS i WHERE i.number < o.number), 0),
    number) AS k
FROM numbers(3) AS o
ORDER BY 1;

SELECT tuple(
    coalesce((SELECT max(i.number) FROM numbers(3) AS i WHERE i.number < o.number), 0),
    number) AS k
FROM numbers(3) AS o
ORDER BY k;

SELECT coalesce(
    (SELECT max(i.number) FROM numbers(3) AS i WHERE i.number < o.number), 0) AS k
FROM numbers(3) AS o
GROUP BY number WITH ROLLUP
ORDER BY 1
SETTINGS group_by_use_nulls = 1;

DROP TABLE squality_correlated_order_by;
