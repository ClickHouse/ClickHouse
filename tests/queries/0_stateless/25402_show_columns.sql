-- Tags: no-parallel
-- no-parallel: creates a custom database schema and expects to use it exclusively

-- Create a test table and verify that the output of SHOW COLUMNS is sane.
-- The matching of actual/expected results relies on the fact that the output of SHOW COLUMNS is sorted.
CREATE OR REPLACE TABLE tab
(
    `uint64` UInt64,
    `int32` Nullable(Int32) COMMENT 'example comment',
    `str` String,
    INDEX idx str TYPE set(1000)
)
ENGINE = MergeTree
PRIMARY KEY (uint64)
ORDER BY (uint64, str);

SHOW COLUMNS FROM tab;

SELECT '---';

SHOW EXTENDED COLUMNS FROM tab;

SELECT '---';

SHOW FULL COLUMNS FROM tab;

SELECT '---';

SHOW COLUMNS FROM tab LIKE '%int%';

SELECT '---';

SHOW COLUMNS FROM tab NOT LIKE '%int%';

SELECT '---';

SHOW COLUMNS FROM tab ILIKE '%INT%';

SELECT '---';

SHOW COLUMNS FROM tab NOT ILIKE '%INT%';

SELECT '---';

SHOW COLUMNS FROM tab WHERE field LIKE '%int%';

SELECT '---';

SHOW COLUMNS FROM tab LIMIT 1;

SELECT '---';


-- Create a table in a different database. Intentionally useing the same table/column names as above so
-- we notice if something is buggy in the implementation of SHOW COLUMNS.
DROP DATABASE database_123456789abcde;
CREATE DATABASE database_123456789abcde; -- pseudo-random database name

CREATE OR REPLACE TABLE database_123456789abcde.tab
(
    `uint64` UInt64,
    `int32` Int32,
    `str` String
)
ENGINE = MergeTree
ORDER BY uint64;

SHOW COLUMNS FROM tab;

SELECT '---';

SHOW COLUMNS FROM tab FROM database_123456789abcde;

SELECT '---';

SHOW COLUMNS FROM database_123456789abcde.tab;

DROP DATABASE database_123456789abc;

DROP TABLE tab;
