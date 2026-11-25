-- `ai_function_enable_result_cache` is enabled since 26.12. A `compatibility` pin to an
-- earlier release disables it, because before 26.12 AI functions sent every request to the provider.

SELECT getSetting('ai_function_enable_result_cache');

SET compatibility = '26.11';
SELECT getSetting('ai_function_enable_result_cache');

SET compatibility = '26.12';
SELECT getSetting('ai_function_enable_result_cache');
