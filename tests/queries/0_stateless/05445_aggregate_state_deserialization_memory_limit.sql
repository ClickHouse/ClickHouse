-- Tags: shard, no-fasttest, no-asan, no-tsan, no-msan
-- Tag no-fasttest: the Protobuf formats are not built in the Fast test.
-- Tag no-asan, no-tsan, no-msan: the sanitizers replace `operator new`, so the read states are not counted against `max_memory_usage`

-- Reading a state of `anyLast` over a `FixedString` allocates its `Field` with `operator new`, which the memory tracker does not
-- refuse, so reading states checks the limit itself.
SELECT s FROM remote('127.0.0.2', view(SELECT initializeAggregation('anyLastState', toFixedString('\0', 1048577)) AS s FROM numbers(400)))
SETTINGS max_memory_usage = '100M', max_block_size = 65536, prefer_localhost_replica = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- It is stopped near the limit, not after the whole block of 400 MiB. The peak in the log never exceeds the limit, so the error tells.
SYSTEM FLUSH LOGS query_log;
SELECT count(), max(parseReadableSize(extract(exception, 'would use ([0-9.]+ [KMGTPE]?i?B)'))) < 150000000 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND is_initial_query
    AND type = 'ExceptionWhileProcessing' AND exception_code = 241 AND query LIKE '%numbers(400)%';

-- The same in a text format, whose reader allocates the text of each state before the state.
SELECT s FROM url('http://127.0.0.1:8123/?query=SELECT+initializeAggregation(%27anyLastState%27,+toFixedString(%27%27,+1000000))+FROM+numbers(200)+FORMAT+TSV',
    TSV, 's AggregateFunction(anyLast, FixedString(1000000))')
SETTINGS max_memory_usage = '100M', max_block_size = 65536, input_format_parallel_parsing = 0 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- The same in RowBinary and Protobuf.
SELECT s FROM url('http://127.0.0.1:8123/?query=SELECT+initializeAggregation(%27anyLastState%27,+toFixedString(%27%27,+1000000))+FROM+numbers(200)+FORMAT+RowBinary',
    RowBinary, 's AggregateFunction(anyLast, FixedString(1000000))')
SETTINGS max_memory_usage = '100M', max_block_size = 65536 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT s FROM url('http://127.0.0.1:8123/?query=SELECT+initializeAggregation(%27anyLastState%27,+toFixedString(%27%27,+1000000))+AS+s+FROM+numbers(200)+FORMAT+Protobuf&format_schema_source=string&format_schema=syntax%3D%22proto3%22%3Bmessage+M%7Bbytes+s%3D1%3B%7D&format_schema_message_name=M',
    Protobuf, 's AggregateFunction(anyLast, FixedString(1000000))')
SETTINGS max_memory_usage = '100M', max_block_size = 65536, format_schema_source = 'string',
    format_schema = 'syntax = "proto3"; message M { bytes s = 1; }', format_schema_message_name = 'M' FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }

-- A result below the limit is read.
SELECT s FROM remote('127.0.0.2', view(SELECT initializeAggregation('anyLastState', toFixedString('\0', 1048577)) AS s FROM numbers(20)))
SETTINGS max_memory_usage = '100M', max_block_size = 65536, prefer_localhost_replica = 0 FORMAT Null;
