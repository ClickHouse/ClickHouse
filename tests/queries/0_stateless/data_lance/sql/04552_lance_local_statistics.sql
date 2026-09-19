DROP TABLE IF EXISTS lance_local_statistics;

CREATE TABLE lance_local_statistics
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT total_bytes IS NULL
FROM system.tables
WHERE database = currentDatabase() AND name = 'lance_local_statistics';

SELECT count() FROM lance_local_statistics;

DROP TABLE lance_local_statistics;
