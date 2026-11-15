-- Tags: no-fasttest
-- Tag no-fasttest: the MongoDB storage and the Protobuf format are not built in the Fast test build.

-- The input side is used because clickhouse-client applies the output format itself, where the schema query is refused.
-- GROUP BY makes the MongoDB storage throw before any connection attempt to the dummy host.
SELECT * FROM format(Protobuf, 'x Int32', '') SETTINGS format_schema_source = 'query', format_schema_message_name = 'M', format_schema = 'SELECT toString((SELECT count() FROM mongodb(''mongodb://some-cluster:27017/db'', ''collection'', ''x Int32'') GROUP BY x))'; -- { serverError NOT_IMPLEMENTED }
