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
DROP ROW POLICY p_time_params ON t_rp_time_params;

CREATE TABLE t_def_time_params (k UInt8, ts DateTime64(3, 'UTC') DEFAULT {ts:DateTime64(3, 'UTC')}, t Time64(3) DEFAULT {t:Time64(3)}) ENGINE = MergeTree ORDER BY k;
DETACH TABLE t_def_time_params;
ATTACH TABLE t_def_time_params;
INSERT INTO t_def_time_params (k) VALUES (1);
INSERT INTO t_def_time_params VALUES (2, {ts:DateTime64(3, 'UTC')}, {t:Time64(3)}), (3, {neg:DateTime64(3, 'UTC')}, {t:Time64(3)});
SELECT 'DEFAULT and VALUES', * FROM t_def_time_params ORDER BY k;

SELECT 'in query', {ts:DateTime64(3, 'UTC')}, {neg:DateTime64(3, 'UTC')}, {t:Time64(3)}, {arr:Array(DateTime64(3, 'UTC'))}, {map:Map(String, DateTime64(3, 'UTC'))}, toUnixTimestamp(tupleElement({tup:Tuple(DateTime('America/New_York'), DateTime64(3, 'UTC'))}, 1));
SELECT 'remote', {ts:DateTime64(3, 'UTC')} AS x, toTypeName(x) FROM remote('127.0.0.{1,2}', system.one);
SELECT 'remote', {t:Time64(3)} FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE t_rp_time_params;
DROP TABLE t_def_time_params;
