-- A column TTL is refused on a column that is not stored (ALIAS, EPHEMERAL), on CREATE and on the ALTERs that write one.
-- 05896_column_ttl_on_non_stored_column_stored_definition covers a table an older server stored with such a TTL.

CREATE TABLE t_alias (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_ephemeral (id UInt64, dt Date, e UInt64 EPHEMERAL 0 TTL dt + INTERVAL 1 MONTH, v UInt64 DEFAULT e)
ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_replicated (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY id; -- { serverError ILLEGAL_COLUMN }
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE t_deprecated (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree(dt, id, 8192); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_function (id UInt64, dt Date, al UInt64 ALIAS id TTL dt + INTERVAL 1 MONTH) AS null(); -- { serverError ILLEGAL_COLUMN }
CREATE MATERIALIZED VIEW t_mv (id UInt64, dt Date, al UInt64 ALIAS id TTL dt + INTERVAL 1 MONTH) ENGINE = MergeTree ORDER BY id
AS SELECT 1 AS id, toDate('2000-01-01') AS dt; -- { serverError ILLEGAL_COLUMN }
SET enable_time_series_table = 1;
CREATE TABLE t_time_series ENGINE = TimeSeries TAGS INNER COLUMNS (d Date, x UInt64, a UInt64 ALIAS x TTL d + INTERVAL 1 MONTH); -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_time_series ENGINE = TimeSeries TAGS INNER COLUMNS (d Date, e UInt64 EPHEMERAL 0 TTL d + INTERVAL 1 MONTH); -- { serverError ILLEGAL_COLUMN }

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, dt Date, arr Array(UInt64) TTL dt + INTERVAL 1 MONTH, s String TTL dt + INTERVAL 1 MONTH,
    m UInt64 MATERIALIZED id TTL dt + INTERVAL 1 MONTH, al Array(UInt64) ALIAS arr)
ENGINE = MergeTree ORDER BY id;

ALTER TABLE t ADD COLUMN al2 Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t ADD COLUMN e UInt64 EPHEMERAL 0 TTL dt + INTERVAL 1 MONTH; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t MODIFY COLUMN al TTL dt + INTERVAL 1 MONTH; -- { serverError ILLEGAL_COLUMN }
-- `MODIFY COLUMN` keeps the TTL of the column it changes.
ALTER TABLE t MODIFY COLUMN s String ALIAS 'x'; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE t ADD COLUMN c UInt64 ALIAS id TTL dt + INTERVAL 1 MONTH, RENAME COLUMN c TO c2; -- { serverError ILLEGAL_COLUMN }

ALTER TABLE t ADD COLUMN al3 Array(UInt64) ALIAS arr;
ALTER TABLE t MODIFY COLUMN arr TTL dt + INTERVAL 2 MONTH;
ALTER TABLE t MODIFY COLUMN s REMOVE TTL;
ALTER TABLE t MODIFY COLUMN s String ALIAS 'x';

SELECT name, default_kind FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY name;

DROP TABLE t;
