-- Query parameters of the decimal-backed `DateTime64` / `Time64` types keep their value when the substituted
-- query is stored and parsed again: a row policy filter and a column `DEFAULT`.

-- The 'basic' parser happens to read a decimal string as a Unix timestamp.
SET cast_string_to_date_time_mode = 'best_effort';

DROP TABLE IF EXISTS t_rp_time_params;
DROP TABLE IF EXISTS t_def_time_params;

CREATE TABLE t_rp_time_params (ts DateTime64(3, 'UTC'), t Time64(3)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rp_time_params VALUES ('1969-12-31 23:59:58.500', '01:00:00'), ('1970-01-01 00:00:00', '12:34:56.789'), ('2000-01-01 00:00:00', '23:00:00');

-- 1730615400 is the second 2024-11-03 01:30:00 in America/New_York (DST overlap).
SET param_ts = '1970-08-23 06:15:01', param_neg = '1969-12-31 23:59:59', param_t = '12:34:56.789', param_arr = '[\'1970-01-01 00:00:00\', \'2000-01-01 00:00:00\']', param_map = '{\'a\':\'1969-12-31 23:59:58.500\'}', param_tup = '(1730615400, \'1970-01-01 00:00:00\')';

CREATE ROW POLICY p_time_params ON t_rp_time_params USING ts <= {ts:DateTime64(3, 'UTC')} TO ALL;
SELECT 'DateTime64', ts FROM t_rp_time_params ORDER BY ts;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING ts > {neg:DateTime64(3, 'UTC')};
SELECT 'negative DateTime64', ts FROM t_rp_time_params ORDER BY ts;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING t = {t:Time64(3)};
SELECT 'Time64', t FROM t_rp_time_params ORDER BY t;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING t = {t:Nullable(Time64(3))};
SELECT 'Nullable(Time64)', t FROM t_rp_time_params ORDER BY t;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING has({arr:Array(DateTime64(3, 'UTC'))}, ts);
SELECT 'Array(DateTime64)', ts FROM t_rp_time_params ORDER BY ts;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING ts = {map:Map(String, DateTime64(3, 'UTC'))}['a'];
SELECT 'Map(String, DateTime64)', ts FROM t_rp_time_params ORDER BY ts;
ALTER ROW POLICY p_time_params ON t_rp_time_params USING toUnixTimestamp(tupleElement({tup:Tuple(DateTime('America/New_York'), DateTime64(3, 'UTC'))}, 1)) = 1730615400 AND ts = tupleElement({tup:Tuple(DateTime('America/New_York'), DateTime64(3, 'UTC'))}, 2);
SELECT 'Tuple(DateTime, DateTime64)', ts FROM t_rp_time_params ORDER BY ts;
SET param_saf = '(1, [(\'1969-12-31 23:59:58.500\', 1.5)])';
ALTER ROW POLICY p_time_params ON t_rp_time_params USING has(arrayMap(x -> x.1, tupleElement({saf:Tuple(UInt8, SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64))))}, 2)), ts);
SELECT 'SimpleAggregateFunction', ts FROM t_rp_time_params ORDER BY ts;
SET param_ring = '(\'1969-12-31 23:59:58.500\', [(1, 2)])';
ALTER ROW POLICY p_time_params ON t_rp_time_params USING ts = tupleElement({ring:Tuple(DateTime64(3, 'UTC'), Ring)}, 1) AND ts = tupleElement({ring:SimpleAggregateFunction(any, Tuple(DateTime64(3, 'UTC'), Ring))}, 1);
SELECT 'Ring beside DateTime64', ts FROM t_rp_time_params ORDER BY ts;
-- 19 significant digits, at the same DST overlap.
SET param_ns = '1730615400.123456789';
ALTER ROW POLICY p_time_params ON t_rp_time_params USING toUnixTimestamp64Nano({ns:DateTime64(9, 'America/New_York')}) = 1730615400123456789;
SELECT 'DateTime64(9) DST overlap', count() FROM t_rp_time_params;
DROP ROW POLICY p_time_params ON t_rp_time_params;

CREATE TABLE t_def_time_params (k UInt8, ts DateTime64(3, 'UTC') DEFAULT {ts:DateTime64(3, 'UTC')}, t Time64(3) DEFAULT {t:Time64(3)}) ENGINE = MergeTree ORDER BY k;
DETACH TABLE t_def_time_params;
ATTACH TABLE t_def_time_params;
INSERT INTO t_def_time_params (k) VALUES (1);
INSERT INTO t_def_time_params VALUES (2, {ts:DateTime64(3, 'UTC')}, {t:Time64(3)}), (3, {neg:DateTime64(3, 'UTC')}, {t:Time64(3)});
SELECT 'DEFAULT and VALUES', * FROM t_def_time_params ORDER BY k;

SELECT 'in query', {ts:DateTime64(3, 'UTC')}, {neg:DateTime64(3, 'UTC')}, {t:Time64(3)}, {arr:Array(DateTime64(3, 'UTC'))}, {map:Map(String, DateTime64(3, 'UTC'))}, toUnixTimestamp(tupleElement({tup:Tuple(DateTime('America/New_York'), DateTime64(3, 'UTC'))}, 1));
SET param_f = '1.14', param_ftup = '(1.14, 1500)', param_fmap = '{1.14:1500}';
SELECT 'Float64 beside Time64', {f:Float64} = tupleElement({ftup:Tuple(Float64, Time64(3))}, 1), {f:Float64} = mapKeys({fmap:Map(Float64, Time64(3))})[1] SETTINGS precise_float_parsing = 0;
SET param_saf_top = '[(1700000000.123, 1.5)]', param_saf_empty = '[]';
SELECT 'SimpleAggregateFunction in query', {saf_top:SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))}, {saf_empty:SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3, 'UTC'), Float64)))};
SELECT 'remote', {ts:DateTime64(3, 'UTC')} AS x, toTypeName(x) FROM remote('127.0.0.{1,2}', system.one);
SELECT 'remote', {t:Time64(3)} FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE t_rp_time_params;
DROP TABLE t_def_time_params;
