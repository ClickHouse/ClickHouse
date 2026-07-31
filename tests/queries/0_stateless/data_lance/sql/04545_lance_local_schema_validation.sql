DROP TABLE IF EXISTS lance_local_explicit;
DROP TABLE IF EXISTS lance_local_mismatch;

CREATE TABLE lance_local_explicit
(
    id Int32,
    name String,
    score Nullable(Float32)
)
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance');

SELECT toTypeName(id), toTypeName(score) FROM lance_local_explicit LIMIT 1;
SELECT id, name, score FROM lance_local_explicit ORDER BY id;

CREATE TABLE lance_local_mismatch
(
    id String,
    name String,
    score Nullable(Float32)
)
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance'); -- { serverError BAD_ARGUMENTS }

DROP TABLE lance_local_explicit;
