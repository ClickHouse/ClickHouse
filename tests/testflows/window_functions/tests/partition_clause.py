from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestScenario
def single_expr(self):
    """Check defining of partition clause with single expr."""
    expected = convert_output(
        """
      x | s | sum
    ----+---+------
      1 | a |  2 
      1 | b |  2 
      2 | b |  2 
    """
    )

    execute_query(
        "SELECT x,s, sum(x) OVER (PARTITION BY x) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected,
    )


@TestScenario
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MultipleExpr("1.0"))
def multiple_expr(self):
    """Check defining of partition clause with multiple exprs."""
    expected = convert_output(
        """
     x | s | sum
     --+---+----
     1 | a | 1
     1 | b | 1
     2 | b | 2
    """
    )

    execute_query(
        "SELECT x,s, sum(x) OVER (PARTITION BY x,s) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MissingExpr_Error("1.0")
)
def missing_expr_error(self):
    """Check that defining of partition clause with missing expr returns an error."""
    exitcode = 62
    message = "Exception: Syntax error: failed at position"

    self.context.node.query(
        "SELECT sum(number) OVER (PARTITION BY) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_InvalidExpr_Error("1.0")
)
def invalid_expr_error(self):
    """Check that defining of partition clause with invalid expr returns an error."""
    exitcode = 47
    message = "Exception: Missing columns: 'foo'"

    self.context.node.query(
        "SELECT sum(number) OVER (PARTITION BY foo) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestFeature
@Name("partition clause")
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause("1.0"))
def feature(self):
    """Check defining partition clause."""
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
