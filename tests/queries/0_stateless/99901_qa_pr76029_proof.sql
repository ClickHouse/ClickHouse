SET max_execution_time = 30;

-- Bug: InstrumentationManager.cpp:171 shouldPatchFunction() calls find() once,
-- gets the first match. If that position is inside template <> args, the
-- bracket-depth check rejects it and returns false -- never searching for
-- a second occurrence outside template args. This is a false-negative bug.
--
-- Since shouldPatchFunction is internal C++ with no SQL-visible effect,
-- we reproduce its exact algorithm in SQL to demonstrate the false negative.

DROP TABLE IF EXISTS shouldpatch_cases;

CREATE TABLE shouldpatch_cases
(
    case_id        UInt8,
    needle         String,
    haystack       String,
    expect_correct Bool
)
ENGINE = Memory;

-- Case 1: needle first appears inside <>, but also exists outside => bug (false negative)
-- Case 2: needle only appears outside <> => OK
-- Case 3: needle only appears inside <> => correctly rejected
-- Case 4: longer needle, first match inside <>, second outside => bug (false negative)
INSERT INTO shouldpatch_cases VALUES
    (1, 'startQuery', 'Wrapper<Foo::startQuery>::Bar::startQuery', true),
    (2, 'startQuery', 'DB::something<bool>::QueryMetricLog::startQuery', true),
    (3, 'startQuery', 'Wrapper<Foo::startQuery>::doSomethingElse', false),
    (4, 'QueryMetricLog::startQuery',
        'std::__call_func<DB::QueryMetricLog::startQuery()::$_0>::QueryMetricLog::startQuery(int)', true);

-- Reproduce the buggy single-find algorithm from shouldPatchFunction():
--   1. found_pos = find(haystack, needle)          -- only first occurrence
--   2. count '<' and '>' before found_pos          -- bracket depth via stack
--   3. depth == 0 => return true, else return false -- never retries
--
-- Bracket depth = count('<' in prefix) - count('>' in prefix)
SELECT
    case_id,
    buggy_result,
    expect_correct AS correct_result,
    if(buggy_result != expect_correct, 'BUG', 'OK') AS verdict
FROM
(
    SELECT
        case_id,
        expect_correct,
        if(first_pos = 0, false,
            (length(prefix) - length(replaceAll(prefix, '<', '')))
            - (length(prefix) - length(replaceAll(prefix, '>', '')))
            = 0
        ) AS buggy_result
    FROM
    (
        SELECT
            case_id,
            expect_correct,
            position(haystack, needle) AS first_pos,
            if(first_pos > 0, substring(haystack, 1, toUInt64(first_pos - 1)), '') AS prefix
        FROM shouldpatch_cases
    )
)
ORDER BY case_id;

DROP TABLE shouldpatch_cases;
