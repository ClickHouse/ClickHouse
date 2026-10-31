DROP TABLE IF EXISTS lance_local_count_pushdown;

CREATE TABLE lance_local_count_pushdown
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT count() FROM lance_local_count_pushdown;
SELECT count() FROM lance_local_count_pushdown WHERE id IN (1, 3, 5);
SELECT count() FROM lance_local_count_pushdown WHERE score IS NULL;
SELECT count() FROM lance_local_count_pushdown WHERE id = 1 OR id = 3;
SELECT count() FROM (SELECT * FROM lance_local_count_pushdown) WHERE id IN (1, 3, 5);
SELECT count() FROM (SELECT * FROM lance_local_count_pushdown) WHERE score IS NULL;
SELECT count() FROM (SELECT * FROM lance_local_count_pushdown) WHERE id = 1 OR id = 3;
SELECT count() FROM lance_local_count_pushdown WHERE lower(name) = 'x';

DROP TABLE lance_local_count_pushdown;
