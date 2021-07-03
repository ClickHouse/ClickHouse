from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_EmptyOverClause("1.0")
)
def empty(self):
    """Check using empty over clause.
    """
    expected = convert_output("""
     count
    -------
        10
        10
        10
        10
        10
        10
        10
        10
        10
        10
    """)

    execute_query(
        "SELECT COUNT(*) OVER () AS count FROM tenk1 WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_EmptyOverClause("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow("1.0")
)
def empty_named_window(self):
    """Check using over clause with empty window.
    """
    expected = convert_output("""
     count
    -------
        10
        10
        10
        10
        10
        10
        10
        10
        10
        10
    """)

    execute_query(
        "SELECT COUNT(*) OVER w AS count FROM tenk1 WHERE unique2 < 10 WINDOW w AS ()",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow("1.0"),
)
def adhoc_window(self):
    """Check running aggregating `sum` function over an adhoc window.
    """
    expected = convert_output("""
      depname  | empno | salary |  sum
    -----------+-------+--------+-------
     develop   |     7 |   4200 | 25100
     develop   |     9 |   4500 | 25100
     develop   |    10 |   5200 | 25100
     develop   |    11 |   5200 | 25100
     develop   |     8 |   6000 | 25100
     personnel |     5 |   3500 |  7400
     personnel |     2 |   3900 |  7400
     sales     |     3 |   4800 | 14600
     sales     |     4 |   4800 | 14600
     sales     |     1 |   5000 | 14600
    """)

    execute_query(
        "SELECT depname, empno, salary, sum(salary) OVER (PARTITION BY depname) AS sum FROM empsalary ORDER BY depname, salary, empno",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow_MissingWindowSpec_Error("1.0")
)
def missing_window_spec(self):
    """Check missing window spec in over clause.
    """
    exitcode = 62
    message = "Exception: Syntax error"

    self.context.node.query("SELECT number,sum(number) OVER FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_InvalidName_Error("1.0")
)
def invalid_window_name(self):
    """Check invalid window name.
    """
    exitcode = 47
    message = "Exception: Window 'w3' is not defined"

    self.context.node.query("SELECT number,sum(number) OVER w3 FROM values('number Int8', (1),(1),(2),(3)) WINDOW w1 AS ()",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_MultipleWindows_Error("1.0")
)
def invalid_multiple_windows(self):
    """Check invalid multiple window names.
    """
    exitcode = 47
    message = "Exception: Missing columns"

    self.context.node.query("SELECT number,sum(number) OVER w1, w2 FROM values('number Int8', (1),(1),(2),(3)) WINDOW w1 AS (), w2 AS (PARTITION BY number)",
        exitcode=exitcode, message=message)


@TestFeature
@Name("over clause")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OverClause("1.0")
)
def feature(self):
    """Check defining frame clause.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
