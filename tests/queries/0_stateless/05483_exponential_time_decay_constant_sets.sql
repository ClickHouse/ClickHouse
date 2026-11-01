SET allow_experimental_time_decay_aggregate_functions = 1;

WITH CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS v
SELECT v IN [v], (v, toUInt8(1)) IN ((v, toUInt8(1))), v IN (v, v);

-- Untyped null members use the set builder's null handling, including inside tuples.
SET transform_null_in = 1;
WITH CAST(NULL, 'Nullable(ExponentialTimeDecaying64(10))') AS v
SELECT (v, toUInt8(1)) IN ((NULL, toUInt8(1)));

WITH CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS v
SELECT v IN [NULL, v];

SET enable_nullable_tuple_type = 1;
WITH CAST(NULL, 'Nullable(Tuple(Nullable(ExponentialTimeDecaying64(10)), Nullable(UInt8)))') AS v
SELECT v IN (NULL, NULL);

WITH CAST((1., 0.), 'ExponentialTimeDecaying128(10)') AS v
SELECT v IN [v], (v, toUInt8(1)) IN ((v, toUInt8(1))), v IN (v, v);

-- Shape selection must not remove compatibility checks before conversion.
WITH CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS v,
     CAST((1., 0.), 'ExponentialTimeDecaying64(20)') AS w
SELECT v IN [w]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS v,
     CAST((1., 0.), 'ExponentialTimeDecaying64(20)') AS w
SELECT (v, toUInt8(1)) IN ((w, toUInt8(1))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

WITH CAST((1., 0.), 'ExponentialTimeDecaying64(10)') AS v,
     CAST((1., 0.), 'ExponentialTimeDecaying128(10)') AS w
SELECT v IN [w]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
