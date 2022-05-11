from testflows.core import *
from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
def partition_clause(self):
    """Check window specification that only contains partition clause.
    """
    expected = convert_output("""
     sum 
    -------
    25100
    25100
    25100
    25100
    25100
    7400
    7400
    14600
    14600
    14600
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (PARTITION BY depname)",
        expected=expected
    )

@TestScenario
def orderby_clause(self):
    """Check window specification that only contains order by clause.
    """
    expected = convert_output("""
     sum 
    -------
    25100
    25100
    25100
    25100
    25100
    32500
    32500
    47100
    47100
    47100
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (ORDER BY depname)",
        expected=expected
    )

@TestScenario
def frame_clause(self):
    """Check window specification that only contains frame clause.
    """
    expected = convert_output("""
     sum 
    -------
    5000
    3900
    4800
    4800
    3500
    4200
    6000
    4500
    5200
    5200
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (ORDER BY empno ROWS CURRENT ROW)",
        expected=expected
    )

@TestScenario
def partition_with_order_by(self):
    """Check window specification that contains partition and order by clauses.
    """
    expected = convert_output("""
     sum 
    -------
     4200
     8700
     19100
     19100
     25100
     3500
     7400
     9600
     9600
     14600
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary)",
        expected=expected
    )

@TestScenario
def partition_with_frame(self):
    """Check window specification that contains partition and frame clauses.
    """
    expected = convert_output("""
     sum 
    -------
    4200
    6000
    4500
    5200
    5200
    3900
    3500
    5000
    4800
    4800
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary  WINDOW w AS (PARTITION BY depname, empno ROWS 1 PRECEDING)",
        expected=expected
    )

@TestScenario
def order_by_with_frame(self):
    """Check window specification that contains order by and frame clauses.
    """
    expected = convert_output("""
     sum 
    -------
    4200
    10200
    10500
    9700
    10400
    9100
    7400
    8500
    9800
    9600
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (ORDER BY depname, empno ROWS 1 PRECEDING)",
        expected=expected
    )

@TestScenario
def partition_with_order_by_and_frame(self):
    """Check window specification that contains all clauses.
    """
    expected = convert_output("""
     sum 
    -------
    4200
    8700
    9700
    10400
    11200
    3500
    7400
    4800
    9600
    9800
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary ROWS 1 PRECEDING)",
        expected=expected
    )

@TestScenario
def empty(self):
    """Check defining an empty window specification.
    """
    expected = convert_output("""
     sum 
    -------
     47100
     47100
     47100
     47100
     47100
     47100
     47100
     47100
     47100
     47100
    """)

    execute_query(
        "SELECT sum(salary) OVER w AS sum FROM empsalary WINDOW w AS ()",
        expected=expected
    )

@TestFeature
@Name("window spec")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_WindowSpec("1.0")
)
def feature(self):
    """Check defining window specifications.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
