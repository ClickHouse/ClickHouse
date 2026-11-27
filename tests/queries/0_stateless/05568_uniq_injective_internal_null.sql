SELECT uniq(tuple(v)), uniqExact(tuple(v)), uniqHLL12(tuple(v)), uniqCombined(tuple(v)), uniqCombined64(tuple(v)), uniqExact(v), count(v)
FROM
(
    SELECT if(number % 2 = 0, NULL, number::Dynamic) AS v
    FROM numbers(4)
)
SETTINGS optimize_injective_functions_inside_uniq = 1,
         optimize_uniq_exact_radix_partitioning = 0,
         count_distinct_optimization = 0;

SELECT uniq(tuple(v)), uniqExact(tuple(v)), uniqHLL12(tuple(v)), uniqCombined(tuple(v)), uniqCombined64(tuple(v)), uniqExact(v), count(v)
FROM
(
    SELECT if(number % 2 = 0, NULL, number::Dynamic) AS v
    FROM numbers(4)
)
SETTINGS optimize_injective_functions_inside_uniq = 0,
         optimize_uniq_exact_radix_partitioning = 0,
         count_distinct_optimization = 0;

SELECT uniq(tuple(v)), uniqExact(tuple(v)), uniqHLL12(tuple(v)), uniqCombined(tuple(v)), uniqCombined64(tuple(v)), uniqExact(v), count(v)
FROM
(
    SELECT if(number % 2 = 0, NULL, number::Variant(UInt64, String)) AS v
    FROM numbers(4)
)
SETTINGS optimize_injective_functions_inside_uniq = 1,
         optimize_uniq_exact_radix_partitioning = 0,
         count_distinct_optimization = 0;

SELECT uniq(tuple(v)), uniqExact(tuple(v)), uniqHLL12(tuple(v)), uniqCombined(tuple(v)), uniqCombined64(tuple(v)), uniqExact(v), count(v)
FROM
(
    SELECT if(number % 2 = 0, NULL, number::Variant(UInt64, String)) AS v
    FROM numbers(4)
)
SETTINGS optimize_injective_functions_inside_uniq = 0,
         optimize_uniq_exact_radix_partitioning = 0,
         count_distinct_optimization = 0;
