-- The name of an `AggregateFunction` type does not keep an explicit state version 0 of a function whose default state version
-- is not 0, so such a type is refused unless `allow_suspicious_aggregate_function_version_0 = 1`. A table that already has one
-- keeps loading, replaying its mutations and restoring.

SET allow_suspicious_variant_types = 1;
SET allow_suspicious_aggregate_function_version_0 = 0;

CREATE TABLE t (k UInt8, s String) ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_plain (s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_avg (v Variant(AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)), AggregateFunction(avg, UInt64))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_pair (v Variant(AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)), AggregateFunction(1, sumMap, Array(UInt8), Array(UInt8)))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bitmap (s AggregateFunction(0, groupBitmapOr, AggregateFunction(groupBitmap, UInt32))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_if (s AggregateFunction(0, sumMapIf, Array(UInt8), Array(UInt8), UInt8)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_simple (s SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_argument (s AggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)))) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t ADD COLUMN s2 AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t MODIFY COLUMN s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT CAST(sumMapState([1::UInt8], [2::UInt8]), 'AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))'); -- { serverError BAD_ARGUMENTS }

-- The default version, and version 0 of a function whose default state version is 0.
CREATE TABLE t_allowed (a AggregateFunction(sumMap, Array(UInt8), Array(UInt8)), b AggregateFunction(1, sumMap, Array(UInt8), Array(UInt8)), c AggregateFunction(0, uniq, UInt64), d AggregateFunction(0, quantileDeterministic, UInt64, UInt64), e AggregateFunction(0, groupBitmap, UInt32)) ENGINE = Memory;
-- Like the other checks of a type, this one does not look into nested types when their validation is turned off.
SELECT toTypeName(CAST([sumMapState([1::UInt8], [2::UInt8])], 'Array(AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)))')) SETTINGS validate_experimental_and_suspicious_types_inside_nested_types = 0;

SET allow_suspicious_aggregate_function_version_0 = 1;
CREATE TABLE t_v0 (k UInt8, s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_v0 SELECT 1, sumMapState([1::UInt8], [2::UInt8]);
ALTER TABLE t MODIFY COLUMN s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8));
CREATE TABLE tf AS file(currentDatabase() || '_05628.rb', RowBinary, 's AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8))');
BACKUP TABLE t_v0 TO Memory('b_05628') FORMAT Null;

SET allow_suspicious_aggregate_function_version_0 = 0;
DETACH TABLE t;
ATTACH TABLE t;
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't';
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 's';
DETACH TABLE t_v0;
ATTACH TABLE t_v0;
SELECT sumMapMerge(s) FROM t_v0;
DETACH TABLE tf;
ATTACH TABLE tf;
RESTORE TABLE t_v0 AS t_v0_restored FROM Memory('b_05628') FORMAT Null;
SELECT sumMapMerge(s) FROM t_v0_restored;

DROP TABLE t;
DROP TABLE t_allowed;
DROP TABLE t_v0;
DROP TABLE tf;
DROP TABLE t_v0_restored;
