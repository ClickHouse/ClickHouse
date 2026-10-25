-- `x op ALL (subquery)` is TRUE for an empty right-hand side (vacuous truth), also when the aggregation over the
-- empty right-hand side produces no row at all.

SELECT 'empty_result_for_aggregation_by_empty_set';
SELECT 5 = ALL (SELECT number FROM numbers(0)) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 5 < ALL (SELECT number FROM numbers(0)) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 5 <= ALL (SELECT number FROM numbers(0)) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 5 > ALL (SELECT number FROM numbers(0)) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 5 >= ALL (SELECT number FROM numbers(0)) SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 5 = ALL (SELECT 5), 5 < ALL (SELECT 5), 5 <= ALL (SELECT 5), 5 > ALL (SELECT 5), 5 >= ALL (SELECT 5) SETTINGS empty_result_for_aggregation_by_empty_set = 1;

SELECT 'correlated';
SELECT
    o.number,
    o.number < ALL (SELECT i.number FROM numbers(1) AS i WHERE i.number = o.number),
    o.number > ALL (SELECT i.number FROM numbers(1) AS i WHERE i.number = o.number),
    o.number + 1 = ALL (SELECT i.number FROM numbers(1) AS i WHERE i.number = o.number)
FROM numbers(2) AS o
ORDER BY o.number;

SELECT 'comparison with a constant of another type';
SELECT number, toDate('2020-01-01') + number = ALL (SELECT '2020-01-01'), toDate('2020-01-01') + number < ALL (SELECT '2020-01-02') FROM numbers(2) ORDER BY number;

-- The rewrite does not copy the right-hand side, so nested quantifiers keep the query size linear in the nesting depth.
SELECT 'nesting';
SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1 = ALL (SELECT 1)))))))))))))))))))))))))))))) SETTINGS max_ast_elements = 5000;
SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0 < ALL (SELECT 0)))))))))))))))))))))))))))))) SETTINGS max_ast_elements = 5000;
SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1 >= ALL (SELECT 1)))))))))))))))))))))))))))))) SETTINGS max_ast_elements = 5000;
