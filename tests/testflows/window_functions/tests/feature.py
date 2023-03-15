from testflows.core import *

from window_functions.tests.common import *
from window_functions.requirements import *


@TestOutline(Feature)
@Name("tests")
@Examples(
    "distributed",
    [
        (
            False,
            Name("non distributed"),
            Requirements(
                RQ_SRS_019_ClickHouse_WindowFunctions_NonDistributedTables("1.0")
            ),
        ),
        (
            True,
            Name("distributed"),
            Requirements(
                RQ_SRS_019_ClickHouse_WindowFunctions_DistributedTables("1.0")
            ),
        ),
    ],
)
def feature(self, distributed, node="clickhouse1"):
    """Check window functions behavior using non-distributed or
    distributed tables.
    """
    self.context.distributed = distributed
    self.context.node = self.context.cluster.node(node)

    with Given("employee salary table"):
        empsalary_table(distributed=distributed)

    with And("tenk1 table"):
        tenk1_table(distributed=distributed)

    with And("numerics table"):
        numerics_table(distributed=distributed)

    with And("datetimes table"):
        datetimes_table(distributed=distributed)

    with And("t1 table"):
        t1_table(distributed=distributed)

    Feature(run=load("window_functions.tests.window_spec", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.partition_clause", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.order_clause", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.frame_clause", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.window_clause", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.over_clause", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.funcs", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.aggregate_funcs", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.errors", "feature"), flags=TE)
    Feature(run=load("window_functions.tests.misc", "feature"), flags=TE)
