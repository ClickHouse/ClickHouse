from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

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
    RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing_ExprValue("1.0")
)
def feature(self):
    """Check defining frame clause.
    """
    Feature(run=load("window_functions.tests.rows_frame", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_frame", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_overflow", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_datetime", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.range_errors", "feature"), flags=TE)