DROP TABLE IF EXISTS lance_local_versions;

CREATE TABLE lance_local_versions
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/versions.lance');

SELECT count() FROM lance_local_versions;
SELECT id, name, score FROM lance_local_versions ORDER BY id;

DROP TABLE lance_local_versions;
