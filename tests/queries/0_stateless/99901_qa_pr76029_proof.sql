SET max_execution_time = 30;

-- Bug: InstrumentationManager.cpp:171 shouldPatchFunction() calls std::string::find()
-- once, getting only the first match. If that position is inside template <> args,
-- the bracket-depth check (lines 175-185) rejects it and returns false -- but never
-- searches for additional occurrences outside template args. This is a false negative.
--
-- shouldPatchFunction is internal C++ with no SQL-visible effect, so we reproduce
-- its exact algorithm in SQL to demonstrate the false-negative behavior.

DROP TABLE IF EXISTS shouldpatch_cases;

CREATE TABLE shouldpatch_cases
(
    case_id        UInt8,
    needle         String,
    haystack       String,
    expect_correct Bool   -- what a correct (fixed) implementation should return
)
ENGINE = Memory;

INSERT INTO shouldpatch_cases VALUES
    -- Case 1: needle first inside <>, also outside => BUG (false negative)
    (1, 'startQuery', 'Wrapper<Foo::startQuery>::Bar::startQuery', true),
    -- Case 2: needle only outside <> => correct true
    (2, 'startQuery', 'DB::something<bool>::QueryMetricLog::startQuery', true),
    -- Case 3: needle only inside <> => correctly rejected
    (3, 'startQuery', 'Wrapper<Foo::startQuery>::doSomethingElse', false),
    -- Case 4: longer needle, first inside <>, second outside => BUG (false negative)
    (4, 'QueryMetricLog::startQuery',
        'std::__call_func<DB::QueryMetricLog::startQuery()::$_0>::QueryMetricLog::startQuery(int)', true);

-- Reproduce the buggy single-find algorithm from shouldPatchFunction():
--   1. first_pos = position(haystack, needle)   -- only first match (the bug)
--   2. prefix = chars before first_pos
--   3. bracket_depth = count('<') - count('>') in prefix
--   4. depth == 0 => true; else => false        -- never retries further matches
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
