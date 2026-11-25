-- Statements for warehouses of the Iceberg REST catalog parse and format back to the same text.
-- TODO(asya): Reference was written by hand, without a run. Run this test and 01271_show_privileges, then remove this line.

SELECT formatQuerySingleLine('create iceberg warehouse if not exists wh settings base_location = \'s3://bucket/wh\', role_arn = \'arn:aws:iam::123456789012:role/catalog\', external_id = \'id\', role_session_name = \'session\'') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('alter iceberg warehouse wh set role_arn = \'arn:aws:iam::123456789012:role/other\', external_id = \'id\' delete role_session_name') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('alter iceberg warehouse wh delete external_id, role_session_name') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('drop iceberg warehouse if exists wh') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('show iceberg warehouses') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;
SELECT formatQuerySingleLine('show create iceberg warehouse wh') AS f, formatQuerySingleLine(f) = f FORMAT TSVRaw;

SELECT formatQuerySingleLine('CREATE ICEBERG WAREHOUSE wh'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('ALTER ICEBERG WAREHOUSE wh'); -- { serverError SYNTAX_ERROR }
