-- With `exact_rows_before_limit`, the `LIMIT` has to read the whole input, so it must not be pushed
-- into the `Lance` scanner, and `rows_before_limit_at_least` must count all eight rows of the dataset.
SET output_format_write_statistics = 0;

SELECT id > 0 FROM lanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance') LIMIT 1
SETTINGS exact_rows_before_limit = 1 FORMAT JSONCompact;

SELECT id > 0 FROM lanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance') WHERE id >= 1 LIMIT 1
SETTINGS exact_rows_before_limit = 1 FORMAT JSONCompact;
