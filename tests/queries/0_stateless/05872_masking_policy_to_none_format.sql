-- A masking policy `TO NONE` must keep `TO NONE` after format and parse, because a missing `TO` means `TO ALL`.
SELECT formatQuery('CREATE MASKING POLICY p ON db.t UPDATE x = 1 TO NONE');
SELECT formatQuery(formatQuery('CREATE MASKING POLICY p ON db.t UPDATE x = 1 TO NONE'));
SELECT formatQuery('CREATE MASKING POLICY p ON db.t UPDATE x = 1');
SELECT formatQuery('ALTER MASKING POLICY p ON db.t UPDATE x = 1');
SELECT formatQuery('ALTER MASKING POLICY p ON db.t TO NONE');
-- In debug builds this used to throw `LOGICAL_ERROR` from the format/parse consistency check.
CREATE MASKING POLICY p ON db.t UPDATE x = 1 TO NONE; -- { serverError SUPPORT_IS_DISABLED }
