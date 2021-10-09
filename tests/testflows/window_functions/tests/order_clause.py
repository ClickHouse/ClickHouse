from testflows.core import *
from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
def single_expr_asc(self):
    """Check defining of order clause with single expr ASC.
    """
    expected = convert_output("""
     x  | s | sum
    ----+---+-----
      1 | a | 2
      1 | b | 2
      2 | b | 4
    """)

    execute_query(
        "SELECT x,s, sum(x) OVER (ORDER BY x ASC) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected
    )

@TestScenario
def single_expr_desc(self):
    """Check defining of order clause with single expr DESC.
    """
    expected = convert_output("""
     x  | s | sum
    ----+---+-----
      2 | b | 2
      1 | a | 4
      1 | b | 4
    """)

    execute_query(
        "SELECT x,s, sum(x) OVER (ORDER BY x DESC) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MultipleExprs("1.0")
)
def multiple_expr_desc_desc(self):
    """Check defining of order clause with multiple exprs.
    """
    expected = convert_output("""
     x | s | sum
     --+---+----
     2 | b | 2
     1 | b | 3
     1 | a | 4
    """)

    execute_query(
        "SELECT x,s, sum(x) OVER (ORDER BY x DESC, s DESC) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MultipleExprs("1.0")
)
def multiple_expr_asc_asc(self):
    """Check defining of order clause with multiple exprs.
    """
    expected = convert_output("""
      x | s | sum
    ----+---+------
      1 | a |  1
      1 | b |  2
      2 | b |  4
    """)

    execute_query(
        "SELECT x,s, sum(x) OVER (ORDER BY x ASC, s ASC) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MultipleExprs("1.0")
)
def multiple_expr_asc_desc(self):
    """Check defining of order clause with multiple exprs.
    """
    expected = convert_output("""
      x | s | sum
    ----+---+------
      1 | b |   1
      1 | a |   2
      2 | b |   4
    """)

    execute_query(
        "SELECT x,s, sum(x) OVER (ORDER BY x ASC, s DESC) AS sum FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MissingExpr_Error("1.0")
)
def missing_expr_error(self):
    """Check that defining of order clause with missing expr returns an error.
    """
    exitcode = 62
    message = "Exception: Syntax error: failed at position"

    self.context.node.query("SELECT sum(number) OVER (ORDER BY) FROM numbers(1,3)", exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_InvalidExpr_Error("1.0")
)
def invalid_expr_error(self):
    """Check that defining of order clause with invalid expr returns an error.
    """
    exitcode = 47
    message = "Exception: Missing columns: 'foo'"

    self.context.node.query("SELECT sum(number) OVER (ORDER BY foo) FROM numbers(1,3)", exitcode=exitcode, message=message)

@TestScenario
def by_column(self):
    """Check order by using a single column.
    """
    expected = convert_output("""
      depname  | empno | salary | rank
    -----------+-------+--------+------
    develop    | 7     | 4200   | 1
    develop    | 8     | 6000   | 1
    develop    | 9     | 4500   | 1
    develop    | 10    | 5200   | 1
    develop    | 11    | 5200   | 1
    personnel  | 2     | 3900   | 1
    personnel  | 5     | 3500   | 1
    sales      | 1     | 5000   | 1
    sales      | 3     | 4800   | 1
    sales      | 4     | 4800   | 1
    """)

    execute_query(
        "SELECT depname, empno, salary, rank() OVER (PARTITION BY depname, empno ORDER BY salary) AS rank FROM empsalary",
        expected=expected,
    )

@TestScenario
def by_expr(self):
    """Check order by with expression.
    """
    expected = convert_output("""
              avg
    ------------------------
     0
     0
     0
     1
     1
     1
     1
     2
     3
     3
    """)

    execute_query(
        "SELECT avg(four) OVER (PARTITION BY four ORDER BY thousand / 100) AS avg FROM tenk1 WHERE unique2 < 10",
        expected=expected,
    )

@TestScenario
def by_expr_with_aggregates(self):
    expected = convert_output("""
     ten |   res    | rank
    -----+----------+------
       0 |  9976146 |    4
       1 | 10114187 |    9
       2 | 10059554 |    8
       3 |  9878541 |    1
       4 |  9881005 |    2
       5 |  9981670 |    5
       6 |  9947099 |    3
       7 | 10120309 |   10
       8 |  9991305 |    6
       9 | 10040184 |    7
    """)

    execute_query(
        "select ten, sum(unique1) + sum(unique2) as res, rank() over (order by sum(unique1) + sum(unique2)) as rank "
        "from tenk1 group by ten order by ten",
        expected=expected,
    )

@TestScenario
def by_a_non_integer_constant(self):
    """Check if it is allowed to use a window with ordering by a non integer constant.
    """
    expected = convert_output("""
     rank
    ------
        1
    """)

    execute_query(
        "SELECT rank() OVER (ORDER BY length('abc')) AS rank",
        expected=expected
    )

@TestFeature
@Name("order clause")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause("1.0")
)
def feature(self):
    """Check defining order clause.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
