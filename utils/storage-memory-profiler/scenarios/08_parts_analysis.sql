-- Verify we have 1000 parts and analyze memory per part
SELECT
    count() as part_count,
    sum(rows) as total_rows,
    sum(bytes_on_disk) as bytes_on_disk,
    sum(data_uncompressed_bytes) as data_uncompressed
FROM system.parts
WHERE table = 'test_many_parts' AND active;

-- Keep parts for memory measurement (no optimization)
