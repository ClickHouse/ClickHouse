SET validate_experimental_and_suspicious_types_inside_nested_types = 1;
SET allow_experimental_time_decay_aggregate_functions = 0;

-- The time-decay gate does not introduce unrelated validation of aggregate arguments.
SET allow_suspicious_low_cardinality_types = 0;
CREATE TABLE time_decay_ordinary_state (s AggregateFunction(any, LowCardinality(UInt64))) ENGINE = Memory;
DROP TABLE time_decay_ordinary_state;

CREATE TABLE time_decay_nested_curve64
(
    s AggregateFunction(any, ExponentialTimeDecaying64(10))
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE time_decay_nested_curve128
(
    s AggregateFunction(any, ExponentialTimeDecaying128(10))
)
ENGINE = Memory; -- { serverError ILLEGAL_COLUMN }

CREATE TABLE time_decay_nested_state
(
    s AggregateFunction(any, AggregateFunction(exponentialTimeDecayedSum(10), Float64, Float64))
)
ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

SET allow_experimental_time_decay_aggregate_functions = 1;

CREATE TABLE time_decay_nested_curve64
(
    s AggregateFunction(any, ExponentialTimeDecaying64(10))
)
ENGINE = Memory;

CREATE TABLE time_decay_nested_curve128
(
    s AggregateFunction(any, ExponentialTimeDecaying128(10))
)
ENGINE = Memory;

CREATE TABLE time_decay_nested_state
(
    s AggregateFunction(any, AggregateFunction(exponentialTimeDecayedSum(10), Float64, Float64))
)
ENGINE = Memory;

-- Recovering existing metadata remains allowed without enabling fresh declarations.
DETACH TABLE time_decay_nested_curve64;
DETACH TABLE time_decay_nested_curve128;
DETACH TABLE time_decay_nested_state;
SET allow_experimental_time_decay_aggregate_functions = 0;
ATTACH TABLE time_decay_nested_curve64;
ATTACH TABLE time_decay_nested_curve128;
ATTACH TABLE time_decay_nested_state;
SELECT count() FROM time_decay_nested_curve64;
SELECT count() FROM time_decay_nested_curve128;
SELECT count() FROM time_decay_nested_state;

DROP TABLE time_decay_nested_curve64;
DROP TABLE time_decay_nested_curve128;
DROP TABLE time_decay_nested_state;
