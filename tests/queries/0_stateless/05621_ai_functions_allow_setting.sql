-- `allow_ai_functions` gates every AI function.

SELECT value FROM system.settings WHERE name = 'allow_ai_functions';

SET allow_ai_functions = 0;

SELECT aiGenerate('x'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiClassify('x', ['a', 'b']); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiFilter('x', 'y'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiExtract('x', 'y'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiTranslate('x', 'de'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiRedact('x'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiEmbed('x', 'm'); -- { serverError SUPPORT_IS_DISABLED }
SELECT aiSimilarity('x', 'y', 'm'); -- { serverError SUPPORT_IS_DISABLED }

-- Enabled: the function is created and fails later, on the missing credentials.
SELECT aiGenerate(x) FROM (SELECT '' AS x WHERE 0) SETTINGS allow_ai_functions = 1; -- { serverError BAD_ARGUMENTS }
