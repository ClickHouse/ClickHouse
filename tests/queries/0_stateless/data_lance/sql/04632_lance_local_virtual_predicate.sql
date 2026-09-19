DROP TABLE IF EXISTS lance_local_virtual_predicate;

CREATE TABLE lance_local_virtual_predicate
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/versions.lance');

SELECT throwIf(count() != 4)
FROM lance_local_virtual_predicate
WHERE _data_lake_snapshot_version > 0
FORMAT Null;
SELECT throwIf(count() != 4)
FROM lance_local_virtual_predicate
WHERE notEmpty(_path)
FORMAT Null;
SELECT throwIf(count() != 4)
FROM lance_local_virtual_predicate
WHERE notEmpty(_file)
FORMAT Null;

DROP TABLE lance_local_virtual_predicate;
