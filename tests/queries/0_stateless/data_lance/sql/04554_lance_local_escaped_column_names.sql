DROP TABLE IF EXISTS lance_local_escaped_column_names;

CREATE TABLE lance_local_escaped_column_names
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT id, `odd``name` FROM lance_local_escaped_column_names ORDER BY id;
SELECT id FROM lance_local_escaped_column_names WHERE `odd``name` = 30;
SELECT id FROM lance_local_escaped_column_names WHERE `odd``name` IN (20, 70) ORDER BY id;
SELECT count() FROM lance_local_escaped_column_names WHERE `odd``name` IS NULL;

DROP TABLE lance_local_escaped_column_names;
