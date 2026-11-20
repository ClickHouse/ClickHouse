-- Tags: no-parallel
-- no-parallel: creates a global user and a named collection with fixed names

-- `aiRelevance` requires its own privilege, granted directly or through the `AI FUNCTIONS` group.
-- The user can always access the credentials, so only the function privilege is under test.
-- All queries read zero rows, so no outbound request is made.

DROP USER IF EXISTS u_05838;
DROP NAMED COLLECTION IF EXISTS nc_05838;

CREATE NAMED COLLECTION nc_05838 AS
    provider = 'cohere',
    endpoint = 'http://localhost:1/v2/rerank',
    model = 'm',
    api_key = 'fake-key';

CREATE USER u_05838 IDENTIFIED WITH no_password;
GRANT NAMED COLLECTION ON nc_05838 TO u_05838;

SELECT '--- no grant';
EXECUTE AS u_05838 SELECT aiRelevance('q', x, map('credentials', 'nc_05838')) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_experimental_ai_relevance_function = 1; -- { serverError ACCESS_DENIED }

SELECT '--- GRANT aiRelevance';
GRANT aiRelevance ON *.* TO u_05838;
EXECUTE AS u_05838 SELECT aiRelevance('q', x, map('credentials', 'nc_05838')) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_experimental_ai_relevance_function = 1;

SELECT '--- REVOKE aiRelevance';
REVOKE aiRelevance ON *.* FROM u_05838;
EXECUTE AS u_05838 SELECT aiRelevance('q', x, map('credentials', 'nc_05838')) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_experimental_ai_relevance_function = 1; -- { serverError ACCESS_DENIED }

SELECT '--- GRANT AI FUNCTIONS';
GRANT AI FUNCTIONS ON *.* TO u_05838;
EXECUTE AS u_05838 SELECT aiRelevance('q', x, map('credentials', 'nc_05838')) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_experimental_ai_relevance_function = 1;

SELECT '--- REVOKE aiRelevance from AI FUNCTIONS';
REVOKE aiRelevance ON *.* FROM u_05838;
EXECUTE AS u_05838 SELECT aiRelevance('q', x, map('credentials', 'nc_05838')) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_experimental_ai_relevance_function = 1; -- { serverError ACCESS_DENIED }

DROP USER u_05838;
DROP NAMED COLLECTION nc_05838;
