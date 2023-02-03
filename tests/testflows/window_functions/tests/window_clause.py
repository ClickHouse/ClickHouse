from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestScenario
def single_window(self):
    """Check defining a single named window using window clause."""
    expected = convert_output(
        """
      depname  | empno | salary |  sum
    -----------+-------+--------+-------
     develop   |   7   |  4200  | 4200
     develop   |   8   |  6000  | 10200
     develop   |   9   |  4500  | 14700
     develop   |   10  |  5200  | 19900
     develop   |   11  |  5200  | 25100
     personnel |   2   |  3900  | 3900
     personnel |   5   |  3500  | 7400
     sales     |   1   |  5000  | 5000
     sales     |   3   |  4800  | 9800
     sales     |   4   |  4800  | 14600
    """
    )

    execute_query(
        "SELECT depname, empno, salary, sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY empno)",
        expected=expected,
    )


@TestScenario
def unused_window(self):
    """Check unused window."""
    expected = convert_output(
        """
    four
    -------
    """
    )

    execute_query(
        "SELECT four FROM tenk1 WHERE 0 WINDOW w AS (PARTITION BY ten)",
        expected=expected,
    )


@TestScenario
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MultipleWindows("1.0"))
def multiple_identical_windows(self):
    """Check defining multiple windows using window clause."""
    expected = convert_output(
        """
      sum  | count
    -------+-------
      3500 |     1
      7400 |     2
     11600 |     3
     16100 |     4
     25700 |     6
     25700 |     6
     30700 |     7
     41100 |     9
     41100 |     9
     47100 |    10
    """
    )

    execute_query(
        "SELECT sum(salary) OVER w1 AS sum, count(*) OVER w2 AS count "
        "FROM empsalary WINDOW w1 AS (ORDER BY salary), w2 AS (ORDER BY salary)",
        expected=expected,
    )


@TestScenario
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MultipleWindows("1.0"))
def multiple_windows(self):
    """Check defining multiple windows using window clause."""
    expected = convert_output(
        """
      empno | depname   | salary | sum1  | sum2
    --------+-----------+--------+-------+--------
        1   | sales     | 5000   | 5000  | 5000
        2   | personnel | 3900   | 3900  | 8900
        3   | sales     | 4800   | 9800  | 8700
        4   | sales     | 4800   | 14600 | 9600
        5   | personnel | 3500   | 7400  | 8300
        7   | develop   | 4200   | 4200  | 7700
        8   | develop   | 6000   | 10200 | 10200
        9   | develop   | 4500   | 14700 | 10500
        10  | develop   | 5200   | 19900 | 9700
        11  | develop   | 5200   | 25100 | 10400
    """
    )

    execute_query(
        "SELECT empno, depname, salary, sum(salary) OVER w1 AS sum1, sum(salary) OVER w2 AS sum2 "
        "FROM empsalary WINDOW w1 AS (PARTITION BY depname ORDER BY empno), w2 AS (ORDER BY empno ROWS 1 PRECEDING)",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MissingWindowSpec_Error("1.0")
)
def missing_window_spec(self):
    """Check missing window spec in window clause."""
    exitcode = 62
    message = "Exception: Syntax error"

    self.context.node.query(
        "SELECT number,sum(number) OVER w1 FROM values('number Int8', (1),(1),(2),(3)) WINDOW w1",
        exitcode=exitcode,
        message=message,
    )


@TestFeature
@Name("window clause")
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause("1.0"))
def feature(self):
    """Check defining frame clause."""
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
