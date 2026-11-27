-- Tags: no-parallel
-- Tag no-parallel: user-defined types live in a single process-wide namespace.

-- A formal parameter can be used as a value argument of an aggregate function inside the definition.
-- Such a use can only be checked with actual arguments, so replacing a type it is passed to must not
-- try to instantiate it with the unresolved parameter.

DROP TABLE IF EXISTS t_udt_aggregate_parameters;
DROP TYPE IF EXISTS AggParamB;
DROP TYPE IF EXISTS AggParamA;

CREATE TYPE AggParamA(T) AS Tuple(T);
CREATE TYPE AggParamB(Q) AS AggParamA(AggregateFunction(quantiles(Q), UInt8));

CREATE TABLE t_udt_aggregate_parameters (x AggParamB(0.5)) ENGINE = Memory;
SELECT toTypeName(x) FROM t_udt_aggregate_parameters;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_udt_aggregate_parameters';

-- A compatible replacement, identical or not, succeeds.
CREATE TYPE OR REPLACE AggParamA(T) AS Tuple(T);
CREATE TYPE OR REPLACE AggParamA(T) AS Tuple(T, UInt8);
SELECT toTypeName(CAST(tuple(1, 2), 'AggParamA(UInt32)'));

DROP TABLE t_udt_aggregate_parameters;
DROP TYPE AggParamB;
DROP TYPE AggParamA;
