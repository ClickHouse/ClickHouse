-- `ai_function_enable_repeated_output_cache` is enabled since 26.12. A `compatibility` pin to an
-- earlier release disables it, because before 26.12 AI functions sent a request for every row.

SELECT getSetting('ai_function_enable_repeated_output_cache');

SET compatibility = '26.11';
SELECT getSetting('ai_function_enable_repeated_output_cache');

SET compatibility = '26.12';
SELECT getSetting('ai_function_enable_repeated_output_cache');
