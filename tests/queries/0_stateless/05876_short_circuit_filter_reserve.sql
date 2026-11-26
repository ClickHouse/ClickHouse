DROP TABLE IF EXISTS t_lazy;
CREATE TABLE t_lazy (k UInt32, s String) ENGINE = Memory;
INSERT INTO t_lazy SELECT number, concat('{"k":"', toString(number), '","b":"', repeat('x', 100000), '"}') FROM numbers(1000)
  SETTINGS max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
-- one 1000-row block of ~95 MiB; the lazy branch is needed for one row
SELECT sum(length(if(k = 0, JSONExtractString(s, 'k'), ''))) FROM t_lazy
  SETTINGS max_threads = 1, short_circuit_function_evaluation = 'enable', max_memory_usage = '40Mi';
SELECT countIf(k = 0 AND JSONExtractString(s, 'k') = '0') FROM t_lazy
  SETTINGS max_threads = 1, short_circuit_function_evaluation = 'enable', max_memory_usage = '40Mi';
SELECT sum(length(coalesce(if(k = 0, NULL, 'a'), JSONExtractString(s, 'k')))) FROM t_lazy
  SETTINGS max_threads = 1, short_circuit_function_evaluation = 'enable', max_memory_usage = '40Mi';
DROP TABLE t_lazy;

DROP TABLE IF EXISTS t_lazy_fixed;
SET allow_suspicious_fixed_string_types = 1;
CREATE TABLE t_lazy_fixed (k UInt32, f FixedString(100000)) ENGINE = Memory;
INSERT INTO t_lazy_fixed SELECT number, toFixedString(toString(number), 100000) FROM numbers(1000)
  SETTINGS max_block_size = 65536, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SELECT countIf(k = 0 AND f = toFixedString('0', 100000)) FROM t_lazy_fixed
  SETTINGS max_threads = 1, short_circuit_function_evaluation = 'force_enable', max_memory_usage = '40Mi';
DROP TABLE t_lazy_fixed;
