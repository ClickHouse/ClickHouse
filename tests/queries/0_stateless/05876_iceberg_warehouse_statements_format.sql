-- Statements for warehouses of the Iceberg REST catalog parse and format back to the same text.

SELECT formatQuerySingleLine('create iceberg warehouse if not exists wh settings base_location = \'s3://bucket/wh\', role_arn = \'arn:aws:iam::123456789012:role/catalog\', external_id = \'id\', role_session_name = \'session\'') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('drop iceberg warehouse if exists wh') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;

SELECT formatQuerySingleLine('CREATE ICEBERG WAREHOUSE wh'); -- { serverError SYNTAX_ERROR }
