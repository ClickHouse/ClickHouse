SET allow_experimental_time_decay_aggregate_functions = 1;

CREATE TABLE time_decay_nullable_row_order
(
    k UInt8,
    v64 Nullable(ExponentialTimeDecaying64(10, direct)),
    v128 Nullable(ExponentialTimeDecaying128(10, direct))
)
ENGINE = MergeTree ORDER BY k
SETTINGS optimize_row_order = 1;

-- Equal sorting keys require cardinality estimates for the nullable columns.
INSERT INTO time_decay_nullable_row_order
SELECT
    1,
    if(number = 3, NULL, CAST((2., toFloat64(number % 2)), 'ExponentialTimeDecaying64(10, direct)')),
    if(number = 3, NULL, CAST((2., toFloat64(number % 2)), 'ExponentialTimeDecaying128(10, direct)'))
FROM numbers(4);

SELECT count(), count(v64), count(v128), uniqExact(v64), uniqExact(v128)
FROM time_decay_nullable_row_order;

DROP TABLE time_decay_nullable_row_order;
