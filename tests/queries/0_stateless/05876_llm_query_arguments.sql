SELECT name FROM system.functions WHERE name = 'llmQuery';

SELECT llmQuery(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmQuery('chat'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmQuery('chat', 'question', map(), 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT llmQuery(1, 'question'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT llmQuery('chat', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT llmQuery('chat', 'question', map('max_tokens', '0')); -- { serverError BAD_ARGUMENTS }
SELECT llmQuery('chat', 'question', map('max_tokens', '1x')); -- { serverError BAD_ARGUMENTS }
SELECT llmQuery('chat', 'question', map('temperature', 'nan')); -- { serverError BAD_ARGUMENTS }
SELECT llmQuery('chat', 'question', map('credentials', 'other')); -- { serverError BAD_ARGUMENTS }
SELECT llmQuery('chat', 'question', map('max_tokens', '1', 'max_tokens', '2')); -- { serverError BAD_ARGUMENTS }

SELECT llmQuery('chat', 'question') SETTINGS allow_ai_functions = 0; -- { serverError SUPPORT_IS_DISABLED }
