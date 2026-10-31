-- `data_lake_delete_data_on_drop` must not resolve a table function that is not a data lake, so the unavailable endpoint is never contacted.
CREATE TABLE t (x Int64) AS url('http://127.0.0.1:1/data.csv', 'CSV');
DROP TABLE t SYNC SETTINGS data_lake_delete_data_on_drop = 1;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't';
