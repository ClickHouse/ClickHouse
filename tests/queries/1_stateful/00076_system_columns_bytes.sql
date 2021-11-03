SELECT sum(data_compressed_bytes) > 0, sum(data_uncompressed_bytes) > 0, sum(marks_bytes) > 0 FROM system.columns;
