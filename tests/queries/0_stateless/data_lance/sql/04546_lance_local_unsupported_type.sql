DROP TABLE IF EXISTS lance_local_unsupported_type;

CREATE TABLE lance_local_unsupported_type
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/extension_unsupported.lance'); -- { serverError BAD_ARGUMENTS }
