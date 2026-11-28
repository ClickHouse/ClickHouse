-- An explicit `PRIORITY 0` in `ALTER MASKING POLICY` must be kept in the AST and formatted,
-- so that it can be told apart from an `ALTER` that does not change the priority.
SELECT formatQuery('ALTER MASKING POLICY p ON db.t PRIORITY 5');
SELECT formatQuery('ALTER MASKING POLICY p ON db.t PRIORITY 0');
SELECT formatQuery('ALTER MASKING POLICY p ON db.t');
SELECT formatQuery('CREATE MASKING POLICY p ON db.t UPDATE email = \'***\' TO ALL PRIORITY 0');
SELECT formatQuery('CREATE MASKING POLICY p ON db.t UPDATE email = \'***\' TO ALL');
