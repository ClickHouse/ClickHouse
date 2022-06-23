from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MultipleColumnsInOrderBy_Error("1.0")
)
def error_more_than_one_order_by_column(self):
    """Check that using more than one column in order by with range frame
    returns an error.
    """
    exitcode = 36
    message = "DB::Exception: Received from localhost:9000. DB::Exception: The RANGE OFFSET window frame requires exactly one ORDER BY column, 2 given"

    sql = ("select sum(salary) over (order by enroll_date, salary range between 1 preceding and 2 following) AS sum, "
        "salary, enroll_date from empsalary")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestScenario
def error_missing_order_by(self):
    """Check that using range frame with offsets without order by returns an error.
    """
    exitcode = 36
    message = "DB::Exception: The RANGE OFFSET window frame requires exactly one ORDER BY column, 0 given"

    sql = ("select sum(salary) over (range between 1 preceding and 2 following) AS sum, "
        "salary, enroll_date from empsalary")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestScenario
def error_missing_order_by_with_partition_by_clause(self):
    """Check that range frame with offsets used with partition by but
    without order by returns an error.
    """
    exitcode = 36
    message = "DB::Exception: The RANGE OFFSET window frame requires exactly one ORDER BY column, 0 given"

    sql = ("select f1, sum(f1) over (partition by f1 range between 1 preceding and 1 following) AS sum "
        "from t1 where f1 = f2")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestScenario
def error_range_over_non_numerical_column(self):
    """Check that range over non numerical column returns an error.
    """
    exitcode = 48
    message = "DB::Exception: The RANGE OFFSET frame for 'DB::ColumnLowCardinality' ORDER BY column is not implemented"

    sql = ("select sum(salary) over (order by depname range between 1 preceding and 2 following) as sum, "
        "salary, enroll_date from empsalary")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding_ExprValue("1.0")
)
def error_negative_preceding_offset(self):
    """Check that non-positive value of preceding offset returns an error.
    """
    exitcode = 36
    message = "DB::Exception: Frame start offset must be greater than zero, -1 given"

    sql = ("select max(enroll_date) over (order by salary range between -1 preceding and 2 following) AS max, "
        "salary, enroll_date from empsalary")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing_ExprValue("1.0")
)
def error_negative_following_offset(self):
    """Check that non-positive value of following offset returns an error.
    """
    exitcode = 36
    message = "DB::Exception: Frame end offset must be greater than zero, -2 given"

    sql = ("select max(enroll_date) over (order by salary range between 1 preceding and -2 following) AS max, "
        "salary, enroll_date from empsalary")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)

@TestFeature
@Name("range errors")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame("1.0")
)
def feature(self):
    """Check different error conditions when usign range frame.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
