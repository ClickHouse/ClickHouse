from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestFeature
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause_DefaultFrame("1.0"))
def default_frame(self):
    """Check default frame."""
    with Scenario("with order by"):
        expected = convert_output(
            """
          number | sum
        ---------+------
               1 |   2
               1 |   2
               2 |   4
               3 |  10
               3 |  10
        """
        )

        execute_query(
            "select number, sum(number) OVER (ORDER BY number) AS sum FROM values('number Int8', (1),(1),(2),(3),(3))",
            expected=expected,
        )

    with Scenario("without order by"):
        expected = convert_output(
            """
          number | sum
        ---------+------
               1 |  10
               1 |  10
               2 |  10
               3 |  10
               3 |  10
        """
        )

        execute_query(
            "select number, sum(number) OVER () AS sum FROM values('number Int8', (1),(1),(2),(3),(3))",
            expected=expected,
        )


@TestFeature
@Name("frame clause")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Extent("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Start("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Frame_End("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Between("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_CurrentRow("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedPreceding("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedFollowing("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding_ExprValue("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing_ExprValue("1.0"),
)
def feature(self):
    """Check defining frame clause."""
    Feature(run=default_frame, flags=TE)
    Feature(run=load("window_functions.tests.rows_frame", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_frame", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_overflow", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_datetime", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_errors", "feature"), flags=TE)
