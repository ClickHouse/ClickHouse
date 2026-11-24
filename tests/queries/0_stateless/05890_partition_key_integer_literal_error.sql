-- an integer literal in a partition key gives the error of the key check, not an error about a positional argument

DROP TABLE IF EXISTS t_partition_key_literal;

CREATE TABLE t_partition_key_literal (a UInt8) ENGINE = MergeTree PARTITION BY 1 ORDER BY a; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE t_partition_key_literal (a UInt8) ENGINE = MergeTree PARTITION BY (a, 2) ORDER BY a; -- { serverError ILLEGAL_COLUMN }
