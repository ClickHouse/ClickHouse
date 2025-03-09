-- Tags: shard

SET output_format_write_statistics = 0, max_rows_to_read = 50_000_000;
SELECT EventDate, count() FROM remote('127.0.0.1', test.hits) WHERE UserID GLOBAL IN (SELECT UserID FROM test.hits) GROUP BY EventDate ORDER BY EventDate LIMIT 5 FORMAT JSONCompact;
