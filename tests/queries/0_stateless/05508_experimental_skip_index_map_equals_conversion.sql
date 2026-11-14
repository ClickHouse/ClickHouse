-- `m['k'] = c` with a `cuckoo_filter` or `binary_fuse_filter` index on `mapKeys(m)` or `mapValues(m)` must convert
-- the constant to the type of the map values the way `equals` does:
-- * a `String` constant compared with `FixedString` values is zero-padded, and the reverse comparison
--   cannot be answered from the hashes of the exact bytes;
-- * a constant that converts to the default value of the map values matches the rows where the key is missing.
-- Every query is run with and without the skip indexes, the results must be the same.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;
SET optimize_functions_to_subcolumns = 0;

SELECT 'cuckoo_filter';

DROP TABLE IF EXISTS t_fixed_values;
CREATE TABLE t_fixed_values (id UInt64, m Map(String, FixedString(2)), INDEX idx mapValues(m) TYPE cuckoo_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fixed_values VALUES (1, {'k': 'a'}), (2, {'k': 'bb'});
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'a' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'a' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'bb' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'bb' ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_fixed_values;

DROP TABLE IF EXISTS t_string_values;
CREATE TABLE t_string_values (id UInt64, m Map(String, String), INDEX idx mapValues(m) TYPE cuckoo_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_string_values VALUES (1, {'k': 'a'}), (2, {'k': 'bb'});
SELECT groupArray(id) FROM (SELECT id FROM t_string_values WHERE m['k'] = toFixedString('a', 2) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_string_values WHERE m['k'] = toFixedString('a', 2) ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_string_values;

DROP TABLE IF EXISTS t_missing_key;
CREATE TABLE t_missing_key (id UInt64, m Map(String, UInt64), INDEX idx mapKeys(m) TYPE cuckoo_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_missing_key VALUES (1, {'present': 1}), (2, {'missing': 5});
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '0' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '0' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '5' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '5' ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_missing_key;

SELECT 'binary_fuse_filter';

DROP TABLE IF EXISTS t_fixed_values;
CREATE TABLE t_fixed_values (id UInt64, m Map(String, FixedString(2)), INDEX idx mapValues(m) TYPE binary_fuse_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_fixed_values VALUES (1, {'k': 'a'}), (2, {'k': 'bb'});
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'a' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'a' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'bb' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_fixed_values WHERE m['k'] = 'bb' ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_fixed_values;

DROP TABLE IF EXISTS t_string_values;
CREATE TABLE t_string_values (id UInt64, m Map(String, String), INDEX idx mapValues(m) TYPE binary_fuse_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_string_values VALUES (1, {'k': 'a'}), (2, {'k': 'bb'});
SELECT groupArray(id) FROM (SELECT id FROM t_string_values WHERE m['k'] = toFixedString('a', 2) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_string_values WHERE m['k'] = toFixedString('a', 2) ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_string_values;

DROP TABLE IF EXISTS t_missing_key;
CREATE TABLE t_missing_key (id UInt64, m Map(String, UInt64), INDEX idx mapKeys(m) TYPE binary_fuse_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_missing_key VALUES (1, {'present': 1}), (2, {'missing': 5});
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '0' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '0' ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '5' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_missing_key WHERE m['missing'] = '5' ORDER BY id) SETTINGS use_skip_indexes = 0;
DROP TABLE t_missing_key;
