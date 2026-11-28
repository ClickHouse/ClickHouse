-- A name in the storage `SETTINGS` clause that is not a setting at all is rejected by default,
-- and ignored when `throw_on_unknown_storage_setting` is disabled, also through `compatibility`.

DROP TABLE IF EXISTS t_unknown_compat;

CREATE TABLE t_unknown_compat (a UInt64) ENGINE = File(CSV)
SETTINGS input_format_csv_allow_single_quotes = 1; -- { serverError UNKNOWN_SETTING }

CREATE TABLE t_unknown_compat (a UInt64) ENGINE = Log
SETTINGS replicated_deduplication_window = 100; -- { serverError UNKNOWN_SETTING }

SELECT '--- disabled explicitly ---';
SET throw_on_unknown_storage_setting = 0;
CREATE TABLE t_unknown_compat (a UInt64) ENGINE = File(CSV)
SETTINGS input_format_csv_allow_single_quotes = 1;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 't_unknown_compat';
DROP TABLE t_unknown_compat;
SET throw_on_unknown_storage_setting = DEFAULT;

SELECT '--- disabled by compatibility ---';
SET compatibility = '26.9';
SELECT value FROM system.settings WHERE name = 'throw_on_unknown_storage_setting';
CREATE TABLE t_unknown_compat (a UInt64) ENGINE = Log
SETTINGS replicated_deduplication_window = 100;
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 't_unknown_compat';
DROP TABLE t_unknown_compat;

SELECT '--- enabled by compatibility of the release that introduced the check ---';
SET compatibility = '26.10';
SELECT value FROM system.settings WHERE name = 'throw_on_unknown_storage_setting';
CREATE TABLE t_unknown_compat (a UInt64) ENGINE = Log
SETTINGS replicated_deduplication_window = 100; -- { serverError UNKNOWN_SETTING }
