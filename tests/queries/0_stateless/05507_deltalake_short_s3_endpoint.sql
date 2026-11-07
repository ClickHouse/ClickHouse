-- Tags: no-fasttest, no-msan
-- no-msan: DeltaLake is not built with MSan.

-- Opening a DeltaLake table on an S3 endpoint of at most 10 characters (`http://abc`) used to throw `std::out_of_range`.
-- The invalid snapshot version stops the query right after the table is opened; host `abc` does not resolve, so the region probe fails fast.
SET delta_lake_snapshot_version = -2;
DESCRIBE TABLE deltaLake('http://abc/bbb/c/', NOSIGN); -- { serverError BAD_ARGUMENTS }
