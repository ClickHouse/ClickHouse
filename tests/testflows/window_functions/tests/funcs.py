from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_FirstValue("1.0")
)
def first_value(self):
    """Check `first_value` function.
    """
    expected = convert_output("""
     first_value | ten | four
    -------------+-----+------
               0 |   0 |    0
               0 |   0 |    0
               0 |   4 |    0
               1 |   1 |    1
               1 |   1 |    1
               1 |   7 |    1
               1 |   9 |    1
               0 |   0 |    2
               1 |   1 |    3
               1 |   3 |    3
    """)

    with Example("using first_value"):
        execute_query(
            "SELECT first_value(ten) OVER (PARTITION BY four ORDER BY ten) AS first_value, ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("using any equivalent"):
        execute_query(
            "SELECT any(ten) OVER (PARTITION BY four ORDER BY ten) AS first_value, ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_LastValue("1.0")
)
def last_value(self):
    """Check `last_value` function.
    """
    with Example("order by window", description="""
        Check that last_value returns the last row of the frame that is CURRENT ROW in ORDER BY window
        """):
        expected = convert_output("""
         last_value | ten | four
        ------------+-----+------
               0    |  0  |  0
               0    |  0  |  0
               2    |  0  |  2
               1    |  1  |  1
               1    |  1  |  1
               3    |  1  |  3
               3    |  3  |  3
               0    |  4  |  0
               1    |  7  |  1
               1    |  9  |  1
        """)

        with Check("using last_value"):
            execute_query(
                "SELECT last_value(four) OVER (ORDER BY ten, four) AS last_value, ten, four FROM tenk1 WHERE unique2 < 10",
                expected=expected
            )

        with Check("using anyLast() equivalent"):
            execute_query(
                "SELECT anyLast(four) OVER (ORDER BY ten, four) AS last_value, ten, four FROM tenk1 WHERE unique2 < 10",
                expected=expected
            )

    with Example("partition by window", description="""
            Check that last_value returns the last row of the frame that is CURRENT ROW in ORDER BY window
            """):
        expected = convert_output("""
         last_value | ten | four
        ------------+-----+------
                  4 |   0 |    0
                  4 |   0 |    0
                  4 |   4 |    0
                  9 |   1 |    1
                  9 |   1 |    1
                  9 |   7 |    1
                  9 |   9 |    1
                  0 |   0 |    2
                  3 |   1 |    3
                  3 |   3 |    3
        """)

        with Check("using last_value"):
            execute_query(
                """SELECT last_value(ten) OVER (PARTITION BY four) AS last_value, ten, four FROM
                (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)
                ORDER BY four, ten""",
                expected=expected
            )

        with Check("using anyLast() equivalent"):
            execute_query(
                """SELECT anyLast(ten) OVER (PARTITION BY four) AS last_value, ten, four FROM
                (SELECT * FROM tenk1 WHERE unique2 < 10 ORDER BY four, ten)
                ORDER BY four, ten""",
                expected=expected
            )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_Lag_Workaround("1.0")
)
def lag(self):
    """Check `lag` function workaround.
    """
    with Example("anyOrNull"):
        expected = convert_output("""
         lag | ten | four
        -----+-----+------
         \\N |   0 |    0
           0 |   0 |    0
           0 |   4 |    0
         \\N |   1 |    1
           1 |   1 |    1
           1 |   7 |    1
           7 |   9 |    1
         \\N |   0 |    2
         \\N |   1 |    3
           1 |   3 |    3
        """)

        execute_query(
            "SELECT anyOrNull(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS lag , ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("any"):
        expected = convert_output("""
         lag | ten | four
        -----+-----+------
           0 |   0 |    0
           0 |   0 |    0
           0 |   4 |    0
           0 |   1 |    1
           1 |   1 |    1
           1 |   7 |    1
           7 |   9 |    1
           0 |   0 |    2
           0 |   1 |    3
           1 |   3 |    3
        """)

        execute_query(
            "SELECT any(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS lag , ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("anyOrNull with column value as offset"):
        expected = convert_output("""
         lag | ten | four
        -----+-----+------
           0 |   0 |    0
           0 |   0 |    0
           4 |   4 |    0
         \\N |   1 |    1
           1 |   1 |    1
           1 |   7 |    1
           7 |   9 |    1
         \\N |   0 |    2
         \\N |   1 |    3
         \\N |   3 |    3
        """)

        execute_query(
            "SELECT any(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN four PRECEDING AND four PRECEDING) AS lag , ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_Lead_Workaround("1.0")
)
def lead(self):
    """Check `lead` function workaround.
    """
    with Example("anyOrNull"):
        expected = convert_output("""
         lead | ten | four
        ------+-----+------
            0 |   0 |    0
            4 |   0 |    0
          \\N |   4 |    0
            1 |   1 |    1
            7 |   1 |    1
            9 |   7 |    1
          \\N |   9 |    1
          \\N |   0 |    2
            3 |   1 |    3
          \\N |   3 |    3
        """)

        execute_query(
            "SELECT anyOrNull(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS lead, ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("any"):
        expected = convert_output("""
         lead | ten | four
        ------+-----+------
            0 |   0 |    0
            4 |   0 |    0
            0 |   4 |    0
            1 |   1 |    1
            7 |   1 |    1
            9 |   7 |    1
            0 |   9 |    1
            0 |   0 |    2
            3 |   1 |    3
            0 |   3 |    3
        """)

        execute_query(
            "SELECT any(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS lead, ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("any with arithmetic expr"):
        expected = convert_output("""
         lead | ten | four
        ------+-----+------
            0 |   0 |    0
            8 |   0 |    0
            0 |   4 |    0
            2 |   1 |    1
           14 |   1 |    1
           18 |   7 |    1
            0 |   9 |    1
            0 |   0 |    2
            6 |   1 |    3
            0 |   3 |    3
        """)

        execute_query(
            "SELECT any(ten * 2) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS lead, ten, four FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

    with Example("subquery as offset"):
        expected = convert_output("""
         lead
        ------
            0
            0
            4
            1
            7
            9
          \\N
            0
            3
          \\N
        """)

        execute_query(
            "SELECT anyNull(ten) OVER (PARTITION BY four ORDER BY ten ROWS BETWEEN  (SELECT two FROM tenk1 WHERE unique2 = unique2) FOLLOWING AND (SELECT two FROM tenk1 WHERE unique2 = unique2) FOLLOWING) AS lead "
            "FROM tenk1 WHERE unique2 < 10",
            expected=expected
        )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowNumber("1.0")
)
def row_number(self):
    """Check `row_number` function.
    """
    expected = convert_output("""
     row_number
    ------------
              1
              2
              3
              4
              5
              6
              7
              8
              9
             10
    """)

    execute_query(
        "SELECT row_number() OVER (ORDER BY unique2) AS row_number FROM tenk1 WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_Rank("1.0")
)
def rank(self):
    """Check `rank` function.
    """
    expected = convert_output("""
     rank_1 | ten | four
    --------+-----+------
          1 |   0 |    0
          1 |   0 |    0
          3 |   4 |    0
          1 |   1 |    1
          1 |   1 |    1
          3 |   7 |    1
          4 |   9 |    1
          1 |   0 |    2
          1 |   1 |    3
          2 |   3 |    3
    """)

    execute_query(
        "SELECT rank() OVER (PARTITION BY four ORDER BY ten) AS rank_1, ten, four FROM tenk1 WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_DenseRank("1.0")
)
def dense_rank(self):
    """Check `dense_rank` function.
    """
    expected = convert_output("""
     dense_rank | ten | four
    ------------+-----+------
              1 |   0 |    0
              1 |   0 |    0
              2 |   4 |    0
              1 |   1 |    1
              1 |   1 |    1
              2 |   7 |    1
              3 |   9 |    1
              1 |   0 |    2
              1 |   1 |    3
              2 |   3 |    3
    """)

    execute_query(
        "SELECT dense_rank() OVER (PARTITION BY four ORDER BY ten) AS dense_rank, ten, four FROM tenk1 WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
def last_value_with_no_frame(self):
    """Check last_value function with no frame.
    """
    expected = convert_output("""
     four | ten | sum | last_value
    ------+-----+-----+------------
        0 |   0 |   0 |          0
        0 |   2 |   2 |          2
        0 |   4 |   6 |          4
        0 |   6 |  12 |          6
        0 |   8 |  20 |          8
        1 |   1 |   1 |          1
        1 |   3 |   4 |          3
        1 |   5 |   9 |          5
        1 |   7 |  16 |          7
        1 |   9 |  25 |          9
        2 |   0 |   0 |          0
        2 |   2 |   2 |          2
        2 |   4 |   6 |          4
        2 |   6 |  12 |          6
        2 |   8 |  20 |          8
        3 |   1 |   1 |          1
        3 |   3 |   4 |          3
        3 |   5 |   9 |          5
        3 |   7 |  16 |          7
        3 |   9 |  25 |          9
    """)

    execute_query(
        "SELECT four, ten, sum(ten) over (partition by four order by ten) AS sum, "
        "last_value(ten) over (partition by four order by ten) AS last_value "
        "FROM (select distinct ten, four from tenk1)",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_LastValue("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Lag_Workaround("1.0"),
)
def last_value_with_lag_workaround(self):
    """Check last value with lag workaround.
    """
    expected = convert_output("""
     last_value | lag  | salary
    ------------+------+--------
           4500 |    0 |   3500
           4800 | 3500 |   3900
           5200 | 3900 |   4200
           5200 | 4200 |   4500
           5200 | 4500 |   4800
           5200 | 4800 |   4800
           6000 | 4800 |   5000
           6000 | 5000 |   5200
           6000 | 5200 |   5200
           6000 | 5200 |   6000
    """)

    execute_query(
        "select last_value(salary) over(order by salary range between 1000 preceding and 1000 following) AS last_value, "
        "any(salary) over(order by salary rows between 1 preceding and 1 preceding) AS lag, "
        "salary from empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_FirstValue("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Lead_Workaround("1.0")
)
def first_value_with_lead_workaround(self):
    """Check first value with lead workaround.
    """
    expected = convert_output("""
     first_value | lead | salary
    -------------+------+--------
            3500 | 3900 |   3500
            3500 | 4200 |   3900
            3500 | 4500 |   4200
            3500 | 4800 |   4500
            3900 | 4800 |   4800
            3900 | 5000 |   4800
            4200 | 5200 |   5000
            4200 | 5200 |   5200
            4200 | 6000 |   5200
            5000 |    0 |   6000
    """)

    execute_query(
        "select first_value(salary) over(order by salary range between 1000 preceding and 1000 following) AS first_value, "
        "any(salary) over(order by salary rows between 1 following and 1 following) AS lead,"
        "salary from empsalary",
        expected=expected
    )

@TestFeature
@Name("funcs")
def feature(self):
    """Check true window functions.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
