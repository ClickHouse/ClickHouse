-- A table `AS` the table function `null` with an explicit structure creates the storage of the function when it is first
-- read, not when it is loaded, as on master, so a structure that needs a setting of the query that created the table does not
-- fail the loading of the table, also at the startup of the server.
-- https://github.com/ClickHouse/ClickHouse/issues/129062

DROP TABLE IF EXISTS t_null_low_cardinality;
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_null_low_cardinality (a LowCardinality(UInt8)) AS null('a LowCardinality(UInt8)');
DETACH TABLE t_null_low_cardinality;
SET allow_suspicious_low_cardinality_types = 0;
ATTACH TABLE t_null_low_cardinality;
SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name = 't_null_low_cardinality';
DROP TABLE t_null_low_cardinality;
