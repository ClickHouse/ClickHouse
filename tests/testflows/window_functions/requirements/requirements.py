# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.210505.1133630.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Specification
from testflows.core import Requirement

Heading = Specification.Heading

RQ_SRS_019_ClickHouse_WindowFunctions = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [window functions] that produce a result for each row inside the window.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.1')

RQ_SRS_019_ClickHouse_WindowFunctions_NonDistributedTables = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.NonDistributedTables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support correct operation of [window functions] on non-distributed \n'
        'table engines such as `MergeTree`.\n'
        '\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.2')

RQ_SRS_019_ClickHouse_WindowFunctions_DistributedTables = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.DistributedTables',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support correct operation of [window functions] on\n'
        '[Distributed](https://clickhouse.tech/docs/en/engines/table-engines/special/distributed/) table engine.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.1.3')

RQ_SRS_019_ClickHouse_WindowFunctions_WindowSpec = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowSpec',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support defining a window using window specification clause.\n'
        'The [window_spec] SHALL be defined as\n'
        '\n'
        '```\n'
        'window_spec:\n'
        '   [partition_clause] [order_clause] [frame_clause]\n'
        '```\n'
        '\n'
        'that SHALL specify how to partition query rows into groups for processing by the window function.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.2.1')

RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [partition_clause] that indicates how to divide the query rows into groups.\n'
        'The [partition_clause] SHALL be defined as\n'
        '\n'
        '```\n'
        'partition_clause:\n'
        '    PARTITION BY expr [, expr] ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.1')

RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MultipleExpr = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MultipleExpr',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support partitioning by more than one `expr` in the [partition_clause] definition.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT x,s, sum(x) OVER (PARTITION BY x,s) FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─x─┬─s─┬─sum(x) OVER (PARTITION BY x, s)─┐\n'
        '│ 1 │ a │                               1 │\n'
        '│ 1 │ b │                               1 │\n'
        '│ 2 │ b │                               2 │\n'
        '└───┴───┴─────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.2')

RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MissingExpr_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MissingExpr.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if `expr` is missing in the [partition_clause] definition.\n'
        '\n'
        '```sql\n'
        'SELECT sum(number) OVER (PARTITION BY) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.3')

RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_InvalidExpr_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.InvalidExpr.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if `expr` is invalid in the [partition_clause] definition.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.3.4')

RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [order_clause] that indicates how to sort rows in each window.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.1')

RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MultipleExprs = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MultipleExprs',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return support using more than one `expr` in the [order_clause] definition.\n'
        '\n'
        'For example, \n'
        '\n'
        '```sql\n'
        "SELECT x,s, sum(x) OVER (ORDER BY x DESC, s DESC) FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─x─┬─s─┬─sum(x) OVER (ORDER BY x DESC, s DESC)─┐\n'
        '│ 2 │ b │                                     2 │\n'
        '│ 1 │ b │                                     3 │\n'
        '│ 1 │ a │                                     4 │\n'
        '└───┴───┴───────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.2')

RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MissingExpr_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MissingExpr.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if `expr` is missing in the [order_clause] definition.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.3')

RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_InvalidExpr_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.InvalidExpr.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if `expr` is invalid in the [order_clause] definition.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.4.4')

RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [frame_clause] that SHALL specify a subset of the current window.\n'
        '\n'
        'The `frame_clause` SHALL be defined as\n'
        '\n'
        '```\n'
        'frame_clause:\n'
        '    {ROWS | RANGE } frame_extent\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.5.1')

RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause_DefaultFrame = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause.DefaultFrame',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the default `frame_clause` to be `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. \n'
        '\n'
        'If the `ORDER BY` clause is specified then this SHALL set the frame to be all rows from \n'
        'the partition start up to and including current row and its peers. \n'
        '\n'
        'If the `ORDER BY` clause is not specified then this SHALL set the frame to include all rows\n'
        'in the partition because all the rows are considered to be the peers of the current row.\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.5.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `ROWS` frame to define beginning and ending row positions.\n'
        'Offsets SHALL be differences in row numbers from the current row number.\n'
        '\n'
        '```sql\n'
        'ROWS frame_extent\n'
        '```\n'
        '\n'
        'See [frame_extent] definition.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.3.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_MissingFrameExtent_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.MissingFrameExtent.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `ROWS` frame clause is defined without [frame_extent].\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number ROWS) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.3.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_InvalidFrameExtent_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.InvalidFrameExtent.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `ROWS` frame clause has invalid [frame_extent].\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number ROWS '1') FROM numbers(1,3)\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.3.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include only the current row in the window partition\n'
        'when `ROWS CURRENT ROW` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS CURRENT ROW) FROM numbers(1,2)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                           1 │\n'
        '│      2 │                                                           2 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.4.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows before and including the current row in the window partition\n'
        'when `ROWS UNBOUNDED PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                   1 │\n'
        '│      2 │                                                                   3 │\n'
        '│      3 │                                                                   6 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.5.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include `expr` rows before and including the current row in the window partition \n'
        'when `ROWS expr PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                           1 │\n'
        '│      2 │                                                           3 │\n'
        '│      3 │                                                           5 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.6.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS UNBOUNDED FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.7.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS expr FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.8.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include only the current row in the window partition\n'
        'when `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,2)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                           1 │\n'
        '│      2 │                                                           2 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.9.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING` frame is specified.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.9.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS BETWEEN CURRENT ROW AND expr PRECEDING` frame is specified.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.9.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the current row and all the following rows in the window partition\n'
        'when `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                   6 │\n'
        '│      2 │                                                                   5 │\n'
        '│      3 │                                                                   3 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.9.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the current row and the `expr` rows that are following the current row in the window partition\n'
        'when `ROWS BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                           3 │\n'
        '│      2 │                                                           5 │\n'
        '│      3 │                                                           3 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.9.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all the rows before and including the current row in the window partition\n'
        'when `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                   1 │\n'
        '│      2 │                                                                   3 │\n'
        '│      3 │                                                                   6 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.10.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.10.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all the rows until and including the current row minus `expr` rows preceding it\n'
        'when `ROWS BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)─┐\n'
        '│      1 │                                                                   0 │\n'
        '│      2 │                                                                   1 │\n'
        '│      3 │                                                                   3 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.10.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows in the window partition \n'
        'when `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                           6 │\n'
        '│      2 │                                                                           6 │\n'
        '│      3 │                                                                           6 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.10.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all the rows until and including the current row plus `expr` rows following it\n'
        'when `ROWS BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                                   3 │\n'
        '│      2 │                                                                   6 │\n'
        '│      3 │                                                                   6 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.10.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `UNBOUNDED FOLLOWING` is specified as the start of the frame, including\n'
        '\n'
        '* `ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW`\n'
        '* `ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING`\n'
        '* `ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING`\n'
        '* `ROWS BETWEEN UNBOUNDED FOLLOWING AND expr PRECEDING`\n'
        '* `ROWS BETWEEN UNBOUNDED FOLLOWING AND expr FOLLOWING`\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.11.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `expr FOLLOWING` is specified as the start of the frame\n'
        'and it points to a row that is after the start of the frame inside the window partition such\n'
        'as the following cases\n'
        '\n'
        '* `ROWS BETWEEN expr FOLLOWING AND CURRENT ROW`\n'
        '* `ROWS BETWEEN expr FOLLOWING AND UNBOUNDED PRECEDING`\n'
        '* `ROWS BETWEEN expr FOLLOWING AND expr PRECEDING`\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.12.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `ROWS BETWEEN expr FOLLOWING AND expr FOLLOWING`\n'
        'is specified and the end of the frame specified by the `expr FOLLOWING` is a row that is before the row \n'
        'specified by the frame start.\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.12.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all the rows from and including current row plus `expr` rows following it \n'
        'until and including the last row in the window partition\n'
        'when `ROWS BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                   5 │\n'
        '│      2 │                                                                   3 │\n'
        '│      3 │                                                                   0 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.12.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the rows from and including current row plus `expr` following it \n'
        'until and including the row specified by the frame end when the frame end \n'
        'is the current row plus `expr` following it is right at or after the start of the frame\n'
        'when `ROWS BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)─┐\n'
        '│      1 │                                                           5 │\n'
        '│      2 │                                                           3 │\n'
        '│      3 │                                                           0 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.12.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the rows from and including current row minus `expr` rows\n'
        'preceding it until and including the current row in the window frame\n'
        'when `ROWS BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                           1 │\n'
        '│      2 │                                                           3 │\n'
        '│      3 │                                                           5 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error\n'
        'when `ROWS BETWEEN expr PRECEDING AND UNBOUNDED PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the rows from and including current row minus `expr` rows\n'
        'preceding it until and including the last row in the window partition\n'
        'when `ROWS BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                   6 │\n'
        '│      2 │                                                                   6 │\n'
        '│      3 │                                                                   5 │\n'
        '└────────┴─────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when the frame end specified by the `expr PRECEDING`\n'
        'evaluates to a row that is before the row specified by the frame start in the window partition\n'
        'when `ROWS BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the rows from and including current row minus `expr` rows preceding it\n'
        'until and including the current row minus `expr` rows preceding it if the end\n'
        'of the frame is after the frame start in the window partition \n'
        'when `ROWS BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING)─┐\n'
        '│      1 │                                                           1 │\n'
        '│      2 │                                                           3 │\n'
        '│      3 │                                                           5 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include the rows from and including current row minus `expr` rows preceding it\n'
        'until and including the current row plus `expr` rows following it in the window partition\n'
        'when `ROWS BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                           3 │\n'
        '│      2 │                                                           6 │\n'
        '│      3 │                                                           5 │\n'
        '└────────┴─────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.3.13.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `RANGE` frame to define rows within a value range.\n'
        'Offsets SHALL be differences in row values from the current row value.\n'
        '\n'
        '```sql\n'
        'RANGE frame_extent\n'
        '```\n'
        '\n'
        'See [frame_extent] definition.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_DateAndDateTime = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.DateAndDateTime',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `RANGE` frame over columns with `Date` and `DateTime`\n'
        'data types.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_IntAndUInt = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.IntAndUInt',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `RANGE` frame over columns with numerical data types\n'
        'such `IntX` and `UIntX`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MultipleColumnsInOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MultipleColumnsInOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `RANGE` frame definition is used with `ORDER BY`\n'
        'that uses multiple columns.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MissingFrameExtent_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MissingFrameExtent.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `RANGE` frame definition is missing [frame_extent].\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_InvalidFrameExtent_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.InvalidFrameExtent.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `RANGE` frame definition has invalid [frame_extent].\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_CurrentRow_Peers = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.CurrentRow.Peers',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] for the `RANGE` frame SHALL define the `peers` of the `CURRENT ROW` to be all\n'
        'the rows that are inside the same order bucket.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.4.8')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithoutOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithoutOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows in the window partition\n'
        'when `RANGE CURRENT ROW` frame is specified without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                            6 │\n'
        '│      2 │                                                            6 │\n'
        '│      3 │                                                            6 │\n'
        '└────────┴──────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.9.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows that are [current row peers] in the window partition\n'
        'when `RANGE CURRENT ROW` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                2 │\n'
        '│      1 │                                                                                2 │\n'
        '│      2 │                                                                                2 │\n'
        '│      3 │                                                                                3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.9.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE UNBOUNDED FOLLOWING` frame is specified with or without order by\n'
        'as `UNBOUNDED FOLLOWING` SHALL not be supported as [frame_start].\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.10.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithoutOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithoutOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows in the window partition\n'
        'when `RANGE UNBOUNDED PRECEDING` frame is specified without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                    6 │\n'
        '│      2 │                                                                    6 │\n'
        '│      3 │                                                                    6 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.11.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include rows with values from and including the first row \n'
        'until and including all [current row peers] in the window partition\n'
        'when `RANGE UNBOUNDED PRECEDING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                        1 │\n'
        '│      2 │                                                                                        3 │\n'
        '│      3 │                                                                                        6 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.11.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE expr PRECEDING` frame is specified without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.12.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_OrderByNonNumericalColumn_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.OrderByNonNumericalColumn.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE expr PRECEDING` is used with `ORDER BY` clause\n'
        'over a non-numerical column.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.12.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include rows with values from and including current row value minus `expr`\n'
        'until and including the value for the current row \n'
        'when `RANGE expr PRECEDING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                1 │\n'
        '│      2 │                                                                                3 │\n'
        '│      3 │                                                                                5 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.12.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE expr FOLLOWING` frame is specified without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.13.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE expr FOLLOWING` frame is specified wit the `ORDER BY` clause \n'
        'as the value for the frame start cannot be larger than the value for the frame end.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.13.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all [current row peers] in the window partition \n'
        'when `RANGE BETWEEN CURRENT ROW AND CURRENT ROW` frame is specified with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**  \n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                            6 │\n'
        '│      2 │                                                            6 │\n'
        '│      3 │                                                            6 │\n'
        '└────────┴──────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**  \n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                1 │\n'
        '│      2 │                                                                                2 │\n'
        '│      3 │                                                                                3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING` frame is specified\n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including [current row peers] until and including\n'
        'the last row in the window partition when `RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` frame is specified\n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                    6 │\n'
        '│      2 │                                                                    6 │\n'
        '│      3 │                                                                    6 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                                        6 │\n'
        '│      2 │                                                                                        5 │\n'
        '│      3 │                                                                                        3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified\n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including [current row peers] until and including\n'
        'current row value plus `expr` when `RANGE BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified\n'
        'with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                                                4 │\n'
        '│      1 │                                                                                4 │\n'
        '│      2 │                                                                                5 │\n'
        '│      3 │                                                                                3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND expr PRECEDING` frame is specified\n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.14.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including the first row until and including\n'
        '[current row peers] in the window partition when `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame is specified\n'
        'with and without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                    7 │\n'
        '│      1 │                                                                    7 │\n'
        '│      2 │                                                                    7 │\n'
        '│      3 │                                                                    7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                        2 │\n'
        '│      1 │                                                                                        2 │\n'
        '│      2 │                                                                                        4 │\n'
        '│      3 │                                                                                        7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return and error when `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING` frame is specified\n'
        'with and without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows in the window partition when `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame is specified\n'
        'with and without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                            7 │\n'
        '│      1 │                                                                            7 │\n'
        '│      2 │                                                                            7 │\n'
        '│      3 │                                                                            7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                                                7 │\n'
        '│      1 │                                                                                                7 │\n'
        '│      2 │                                                                                                7 │\n'
        '│      3 │                                                                                                7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified\n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including the first row until and including\n'
        'the value of the current row minus `expr` in the window partition\n'
        'when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)─┐\n'
        '│      1 │                                                                                        0 │\n'
        '│      1 │                                                                                        0 │\n'
        '│      2 │                                                                                        2 │\n'
        '│      3 │                                                                                        4 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified\n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including the first row until and including\n'
        'the value of the current row plus `expr` in the window partition\n'
        'when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                                                        4 │\n'
        '│      1 │                                                                                        4 │\n'
        '│      2 │                                                                                        7 │\n'
        '│      3 │                                                                                        7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.15.7')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_CurrentRow_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.CurrentRow.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.16.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.16.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.16.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND expr PRECEDING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.16.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprFollowing_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprFollowing.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND expr FOLLOWING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.16.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including current row minus `expr` \n'
        'until and including [current row peers] in the window partition\n'
        'when `RANGE BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                2 │\n'
        '│      1 │                                                                                2 │\n'
        '│      2 │                                                                                4 │\n'
        '│      3 │                                                                                5 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified\n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED PRECEDING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including current row minus `expr` \n'
        'until and including the last row in the window partition when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame\n'
        'is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                                        7 │\n'
        '│      1 │                                                                                        7 │\n'
        '│      2 │                                                                                        7 │\n'
        '│      3 │                                                                                        5 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including current row minus preceding `expr` \n'
        'until and including current row plus following `expr` in the window partition \n'
        'when `RANGE BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)─┐\n'
        '│      1 │                                                                                4 │\n'
        '│      1 │                                                                                4 │\n'
        '│      2 │                                                                                7 │\n'
        '│      3 │                                                                                5 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.7')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM numbers(1,3)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.8')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when the value of the [frame_end] specified by the \n'
        'current row minus preceding `expr` is greater than the value of the [frame_start] in the window partition\n'
        'when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.9')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including current row minus preceding `expr` for the [frame_start]\n'
        'until and including current row minus following `expr` for the [frame_end] in the window partition \n'
        'when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause\n'
        'if an only if the [frame_end] value is equal or greater than [frame_start] value.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Greater Than**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING)─┐\n'
        '│      1 │                                                                                2 │\n'
        '│      1 │                                                                                2 │\n'
        '│      2 │                                                                                4 │\n'
        '│      3 │                                                                                5 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        'or **Equal**\n'
        '\n'
        '```sql\n'
        " SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING)─┐\n'
        '│      1 │                                                                                0 │\n'
        '│      1 │                                                                                0 │\n'
        '│      2 │                                                                                2 │\n'
        '│      3 │                                                                                2 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.17.10')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified \n'
        'with the `ORDER BY` clause and `expr` is greater than `0`.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.2')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_ZeroSpecialCase = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.ZeroSpecialCase',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all [current row peers] in the window partition\n'
        'when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified \n'
        'with the `ORDER BY` clause if and only if the `expr` equals to `0`.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW)─┐\n'
        '│      1 │                                                            7 │\n'
        '│      1 │                                                            7 │\n'
        '│      2 │                                                            7 │\n'
        '│      3 │                                                            7 │\n'
        '└────────┴──────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW)─┐\n'
        '│      1 │                                                                                2 │\n'
        '│      1 │                                                                                2 │\n'
        '│      2 │                                                                                2 │\n'
        '│      3 │                                                                                3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.3')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.4')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with values from and including current row plus `expr`\n'
        'until and including the last row in the window partition \n'
        'when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified with the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)─┐\n'
        '│      1 │                                                                                        5 │\n'
        '│      1 │                                                                                        5 │\n'
        '│      2 │                                                                                        3 │\n'
        '│      3 │                                                                                        0 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.5')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED PRECEDING` frame is specified \n'
        'with or without the `ORDER BY` clause.\n'
        '\n'
        'For example,\n'
        '\n'
        '**Without `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '**With `ORDER BY`**\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.6')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified \n'
        'without the `ORDER BY`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.7')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified \n'
        'with the `ORDER BY` clause if the value of both `expr` is not `0`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.8')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithOrderBy_ZeroSpecialCase = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithOrderBy.ZeroSpecialCase',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with value equal to [current row peers] in the window partition\n'
        'when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified \n'
        "with the `ORDER BY` clause if and only if both `expr`'s are `0`.\n"
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING ─┐\n'
        '│      1 │                                                                                2 │\n'
        '│      1 │                                                                                2 │\n'
        '│      2 │                                                                                2 │\n'
        '│      3 │                                                                                3 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.9')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithoutOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithoutOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified \n'
        'without the `ORDER BY` clause.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.10')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified \n'
        'with the `ORDER BY` clause but the `expr` for the [frame_end] is less than the `expr` for the [frame_start].\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.11')

RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL include all rows with value from and including current row plus `expr` for the [frame_start]\n'
        'until and including current row plus `expr` for the [frame_end] in the window partition\n'
        'when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified \n'
        'with the `ORDER BY` clause if and only if the `expr` for the [frame_end] is greater than or equal than the \n'
        '`expr` for the [frame_start].\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))\n"
        '```\n'
        '\n'
        '```bash\n'
        '┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)─┐\n'
        '│      1 │                                                                                5 │\n'
        '│      1 │                                                                                5 │\n'
        '│      2 │                                                                                3 │\n'
        '│      3 │                                                                                0 │\n'
        '└────────┴──────────────────────────────────────────────────────────────────────────────────┘\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.5.4.18.12')

RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Extent = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Extent',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [frame_extent] defined as\n'
        '\n'
        '```\n'
        'frame_extent:\n'
        '    {frame_start | frame_between}\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.5.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Start = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Start',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [frame_start] defined as\n'
        '\n'
        '```\n'
        'frame_start: {\n'
        '    CURRENT ROW\n'
        '  | UNBOUNDED PRECEDING\n'
        '  | UNBOUNDED FOLLOWING\n'
        '  | expr PRECEDING\n'
        '  | expr FOLLOWING\n'
        '}\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.6.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Between = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Between',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [frame_between] defined as\n'
        '\n'
        '```\n'
        'frame_between:\n'
        '    BETWEEN frame_start AND frame_end\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.7.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Frame_End = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.End',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support [frame_end] defined as\n'
        '\n'
        '```\n'
        'frame_end: {\n'
        '    CURRENT ROW\n'
        '  | UNBOUNDED PRECEDING\n'
        '  | UNBOUNDED FOLLOWING\n'
        '  | expr PRECEDING\n'
        '  | expr FOLLOWING\n'
        '}\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.8.1')

RQ_SRS_019_ClickHouse_WindowFunctions_CurrentRow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.CurrentRow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `CURRENT ROW` as `frame_start` or `frame_end` value.\n'
        '\n'
        '* For `ROWS` SHALL define the bound to be the current row\n'
        '* For `RANGE` SHALL define the bound to be the peers of the current row\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.9.1')

RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `UNBOUNDED PRECEDING` as `frame_start` or `frame_end` value\n'
        'and it SHALL define that the bound is the first partition row.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.10.1')

RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `UNBOUNDED FOLLOWING` as `frame_start` or `frame_end` value\n'
        'and it SHALL define that the bound is the last partition row.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.11.1')

RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `expr PRECEDING` as `frame_start` or `frame_end` value\n'
        '\n'
        '* For `ROWS` it SHALL define the bound to be the `expr` rows before the current row\n'
        '* For `RANGE` it SHALL define the bound to be the rows with values equal to the current row value minus the `expr`.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.12.1')

RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding_ExprValue = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding.ExprValue',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support only non-negative numeric literal as the value for the `expr` in the `expr PRECEDING` frame boundary.\n'
        '\n'
        'For example,\n'
        '\n'
        '```\n'
        '5 PRECEDING\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.12.2')

RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `expr FOLLOWING` as `frame_start` or `frame_end` value\n'
        '\n'
        '* For `ROWS` it SHALL define the bound to be the `expr` rows after the current row\n'
        '* For `RANGE` it SHALL define  the bound to be the rows with values equal to the current row value plus `expr`\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.13.1')

RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing_ExprValue = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing.ExprValue',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support only non-negative numeric literal as the value for the `expr` in the `expr FOLLOWING` frame boundary.\n'
        '\n'
        'For example,\n'
        '\n'
        '```\n'
        '5 FOLLOWING\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.5.13.2')

RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `WINDOW` clause to define one or more windows.\n'
        '\n'
        '```sql\n'
        'WINDOW window_name AS (window_spec)\n'
        '    [, window_name AS (window_spec)] ..\n'
        '```\n'
        '\n'
        'The `window_name` SHALL be the name of a window defined by a `WINDOW` clause.\n'
        '\n'
        'The [window_spec] SHALL specify the window.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT ... FROM table WINDOW w AS (partiton by id))\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.6.1')

RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MultipleWindows = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MultipleWindows',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `WINDOW` clause that defines multiple windows.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT ... FROM table WINDOW w1 AS (partition by id), w2 AS (partition by customer)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.6.2')

RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MissingWindowSpec_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MissingWindowSpec.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `WINDOW` clause definition is missing [window_spec].\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.6.3')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `OVER` clause to either use named window defined using `WINDOW` clause\n'
        'or adhoc window defined inplace.\n'
        '\n'
        '\n'
        '```\n'
        'OVER ()|(window_spec)|named_window \n'
        '```\n'
        '\n'
        ),
    link=None,
    level=3,
    num='3.7.1')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_EmptyOverClause = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.EmptyOverClause',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL treat the entire set of query rows as a single partition when `OVER` clause is empty.\n'
        'For example,\n'
        '\n'
        '```\n'
        'SELECT sum(x) OVER () FROM table\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.2.1')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support ad hoc window specification in the `OVER` clause.\n'
        '\n'
        '```\n'
        'OVER [window_spec]\n'
        '```\n'
        '\n'
        'See [window_spec] definition.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        '(count(*) OVER (partition by id order by time desc))\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.3.1')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow_MissingWindowSpec_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow.MissingWindowSpec.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `OVER` clause has missing [window_spec].\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.3.2')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support using a previously defined named window in the `OVER` clause.\n'
        '\n'
        '```\n'
        'OVER [window_name]\n'
        '```\n'
        '\n'
        'See [window_name] definition.\n'
        '\n'
        'For example,\n'
        '\n'
        '```sql\n'
        'SELECT count(*) OVER w FROM table WINDOW w AS (partition by id)\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.4.1')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_InvalidName_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.InvalidName.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `OVER` clause reference invalid window name.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.4.2')

RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_MultipleWindows_Error = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.MultipleWindows.Error',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL return an error if the `OVER` clause references more than one window name.\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.7.4.3')

RQ_SRS_019_ClickHouse_WindowFunctions_FirstValue = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.FirstValue',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `first_value` window function that\n'
        'SHALL be synonum for the `any(value)` function\n'
        'that SHALL return the value of `expr` from first row in the window frame.\n'
        '\n'
        '```\n'
        'first_value(expr) OVER ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.1.1')

RQ_SRS_019_ClickHouse_WindowFunctions_LastValue = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.LastValue',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `last_value` window function that\n'
        'SHALL be synonym for the `anyLast(value)` function\n'
        'that SHALL return the value of `expr` from the last row in the window frame.\n'
        '\n'
        '```\n'
        'last_value(expr) OVER ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.2.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Lag_Workaround = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Lag.Workaround',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support a workaround for the `lag(value, offset)` function as\n'
        '\n'
        '```\n'
        'any(value) OVER (.... ROWS BETWEEN <offset> PRECEDING AND <offset> PRECEDING)\n'
        '```\n'
        '\n'
        'The function SHALL returns the value from the row that lags (precedes) the current row\n'
        'by the `N` rows within its partition. Where `N` is the `value` passed to the `any` function.\n'
        '\n'
        'If there is no such row, the return value SHALL be default.\n'
        '\n'
        'For example, if `N` is 3, the return value is default for the first two rows.\n'
        'If N or default are missing, the defaults are 1 and NULL, respectively.\n'
        '\n'
        '`N` SHALL be a literal non-negative integer. If N is 0, the value SHALL be\n'
        'returned for the current row.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.3.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Lead_Workaround = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Lead.Workaround',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support a workaround for the `lead(value, offset)` function as\n'
        '\n'
        '```\n'
        'any(value) OVER (.... ROWS BETWEEN <offset> FOLLOWING AND <offset> FOLLOWING)\n'
        '```\n'
        '\n'
        'The function SHALL returns the value from the row that leads (follows) the current row by\n'
        'the `N` rows within its partition. Where `N` is the `value` passed to the `any` function.\n'
        '\n'
        'If there is no such row, the return value SHALL be default.\n'
        '\n'
        'For example, if `N` is 3, the return value is default for the last two rows.\n'
        'If `N` or default are missing, the defaults are 1 and NULL, respectively.\n'
        '\n'
        '`N` SHALL be a literal non-negative integer. If `N` is 0, the value SHALL be\n'
        'returned for the current row.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.4.1')

RQ_SRS_019_ClickHouse_WindowFunctions_LeadInFrame = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.LeadInFrame',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `leadInFrame(expr[, offset, [default]])` function.\n'
        '\n'
        'For example,\n'
        '```\n'
        'leadInFrame(column) OVER (...)\n'
        '```\n'
        '\n'
        'The function SHALL return the value from the row that leads (follows) the current row\n'
        'by the `offset` rows within the current frame. If there is no such row,\n'
        'the return value SHALL be the `default` value. If the `default` value is not specified \n'
        'then the default value for the corresponding column data type SHALL be returned.\n'
        '\n'
        'The `offset` SHALL be a literal non-negative integer. If the `offset` is set to `0`, then\n'
        'the value SHALL be returned for the current row. If the `offset` is not specified, the default\n'
        'value SHALL be `1`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.5.1')

RQ_SRS_019_ClickHouse_WindowFunctions_LagInFrame = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.LagInFrame',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support the `lagInFrame(expr[, offset, [default]])` function.\n'
        '\n'
        'For example,\n'
        '```\n'
        'lagInFrame(column) OVER (...)\n'
        '```\n'
        '\n'
        'The function SHALL return the value from the row that lags (preceds) the current row\n'
        'by the `offset` rows within the current frame. If there is no such row,\n'
        'the return value SHALL be the `default` value. If the `default` value is not specified \n'
        'then the default value for the corresponding column data type SHALL be returned.\n'
        '\n'
        'The `offset` SHALL be a literal non-negative integer. If the `offset` is set to `0`, then\n'
        'the value SHALL be returned for the current row. If the `offset` is not specified, the default\n'
        'value SHALL be `1`.\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.6.1')

RQ_SRS_019_ClickHouse_WindowFunctions_Rank = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.Rank',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `rank` window function that SHALL\n'
        'return the rank of the current row within its partition with gaps.\n'
        '\n'
        'Peers SHALL be considered ties and receive the same rank.\n'
        'The function SHALL not assign consecutive ranks to peer groups if groups of size greater than one exist\n'
        'and the result is noncontiguous rank numbers.\n'
        '\n'
        'If the function is used without `ORDER BY` to sort partition rows into the desired order\n'
        'then all rows SHALL be peers.\n'
        '\n'
        '```\n'
        'rank() OVER ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.7.1')

RQ_SRS_019_ClickHouse_WindowFunctions_DenseRank = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.DenseRank',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `dense_rank` function over a window that SHALL\n'
        'return the rank of the current row within its partition without gaps.\n'
        '\n'
        'Peers SHALL be considered ties and receive the same rank.\n'
        'The function SHALL assign consecutive ranks to peer groups and\n'
        'the result is that groups of size greater than one do not produce noncontiguous rank numbers.\n'
        '\n'
        'If the function is used without `ORDER BY` to sort partition rows into the desired order\n'
        'then all rows SHALL be peers.\n'
        '\n'
        '```\n'
        'dense_rank() OVER ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.8.1')

RQ_SRS_019_ClickHouse_WindowFunctions_RowNumber = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.RowNumber',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support `row_number` function over a window that SHALL\n'
        'returns the number of the current row within its partition.\n'
        '\n'
        'Rows numbers SHALL range from 1 to the number of partition rows.\n'
        '\n'
        'The `ORDER BY` affects the order in which rows are numbered.\n'
        'Without `ORDER BY`, row numbering MAY be nondeterministic.\n'
        '\n'
        '```\n'
        'row_number() OVER ...\n'
        '```\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.1.9.1')

RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support using aggregate functions over windows.\n'
        '\n'
        '* [count](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/count/)\n'
        '* [min](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/min/)\n'
        '* [max](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/max/)\n'
        '* [sum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/sum/)\n'
        '* [avg](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/avg/)\n'
        '* [any](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/)\n'
        '* [stddevPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stddevpop/)\n'
        '* [stddevSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stddevsamp/)\n'
        '* [varPop(x)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/varpop/)\n'
        '* [varSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/varsamp/)\n'
        '* [covarPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/covarpop/)\n'
        '* [covarSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/covarsamp/)\n'
        '* [anyHeavy](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/anyheavy/)\n'
        '* [anyLast](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/anylast/)\n'
        '* [argMin](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmin/)\n'
        '* [argMax](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmax/)\n'
        '* [avgWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/avgweighted/)\n'
        '* [corr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/corr/)\n'
        '* [topK](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/topk/)\n'
        '* [topKWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/topkweighted/)\n'
        '* [groupArray](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparray/)\n'
        '* [groupUniqArray](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupuniqarray/)\n'
        '* [groupArrayInsertAt](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparrayinsertat/)\n'
        '* [groupArrayMovingSum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraymovingsum/)\n'
        '* [groupArrayMovingAvg](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraymovingavg/)\n'
        '* [groupArraySample](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraysample/)\n'
        '* [groupBitAnd](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitand/)\n'
        '* [groupBitOr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitor/)\n'
        '* [groupBitXor](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitxor/)\n'
        '* [groupBitmap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmap/)\n'
        '* [groupBitmapAnd](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapand/)\n'
        '* [groupBitmapOr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapor/)\n'
        '* [groupBitmapXor](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapxor/)\n'
        '* [sumWithOverflow](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/sumwithoverflow/)\n'
        '* [deltaSum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/deltasum/)\n'
        '* [sumMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/summap/)\n'
        '* [minMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/minmap/)\n'
        '* [maxMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/maxmap/)\n'
        '* [initializeAggregation](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/initializeAggregation/)\n'
        '* [skewPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewpop/)\n'
        '* [skewSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewsamp/)\n'
        '* [kurtPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/kurtpop/)\n'
        '* [kurtSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/kurtsamp/)\n'
        '* [uniq](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniq/)\n'
        '* [uniqExact](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqexact/)\n'
        '* [uniqCombined](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqcombined/)\n'
        '* [uniqCombined64](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqcombined64/)\n'
        '* [uniqHLL12](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqhll12/)\n'
        '* [quantile](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantile/)\n'
        '* [quantiles](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiles/)\n'
        '* [quantileExact](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantileexact/)\n'
        '* [quantileExactWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantileexactweighted/)\n'
        '* [quantileTiming](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletiming/)\n'
        '* [quantileTimingWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletimingweighted/)\n'
        '* [quantileDeterministic](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiledeterministic/)\n'
        '* [quantileTDigest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletdigest/)\n'
        '* [quantileTDigestWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletdigestweighted/)\n'
        '* [simpleLinearRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/simplelinearregression/)\n'
        '* [stochasticLinearRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlinearregression/)\n'
        '* [stochasticLogisticRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlogisticregression/)\n'
        '* [categoricalInformationValue](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlogisticregression/)\n'
        '* [studentTTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/studentttest/)\n'
        '* [welchTTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/welchttest/)\n'
        '* [mannWhitneyUTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/mannwhitneyutest/)\n'
        '* [median](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/median/)\n'
        '* [rankCorr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/rankCorr/)\n'
        '\n'
        ),
    link=None,
    level=4,
    num='3.8.2.1')

RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions_Combinators = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Combinators',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support aggregate functions with combinator prefixes over windows.\n'
        '\n'
        '* [-If](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-if)\n'
        '* [-Array](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-array)\n'
        '* [-SimpleState](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-simplestate)\n'
        '* [-State](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-state)\n'
        '* [-Merge](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#aggregate_functions_combinators-merge)\n'
        '* [-MergeState](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#aggregate_functions_combinators-mergestate)\n'
        '* [-ForEach](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-foreach)\n'
        '* [-Distinct](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-distinct)\n'
        '* [-OrDefault](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-ordefault)\n'
        '* [-OrNull](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-ornull)\n'
        '* [-Resample](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-resample)\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.2.2.1')

RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions_Parametric = Requirement(
    name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Parametric',
    version='1.0',
    priority=None,
    group=None,
    type=None,
    uid=None,
    description=(
        '[ClickHouse] SHALL support parametric aggregate functions over windows.\n'
        '\n'
        '* [histogram](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#histogram)\n'
        '* [sequenceMatch(pattern)(timestamp, cond1, cond2, ...)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#function-sequencematch)\n'
        '* [sequenceCount(pattern)(time, cond1, cond2, ...)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#function-sequencecount)\n'
        '* [windowFunnel](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#windowfunnel)\n'
        '* [retention](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#retention)\n'
        '* [uniqUpTo(N)(x)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#uniquptonx)\n'
        '* [sumMapFiltered(keys_to_keep)(keys, values)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#summapfilteredkeys-to-keepkeys-values)\n'
        '\n'
        ),
    link=None,
    level=5,
    num='3.8.2.3.1')

SRS019_ClickHouse_Window_Functions = Specification(
    name='SRS019 ClickHouse Window Functions', 
    description=None,
    author=None,
    date=None, 
    status=None, 
    approved_by=None,
    approved_date=None,
    approved_version=None,
    version=None,
    group=None,
    type=None,
    link=None,
    uid=None,
    parent=None,
    children=None,
    headings=(
        Heading(name='Revision History', level=1, num='1'),
        Heading(name='Introduction', level=1, num='2'),
        Heading(name='Requirements', level=1, num='3'),
        Heading(name='General', level=2, num='3.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions', level=3, num='3.1.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.NonDistributedTables', level=3, num='3.1.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.DistributedTables', level=3, num='3.1.3'),
        Heading(name='Window Specification', level=2, num='3.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowSpec', level=3, num='3.2.1'),
        Heading(name='PARTITION Clause', level=2, num='3.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause', level=3, num='3.3.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MultipleExpr', level=3, num='3.3.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MissingExpr.Error', level=3, num='3.3.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.InvalidExpr.Error', level=3, num='3.3.4'),
        Heading(name='ORDER Clause', level=2, num='3.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause', level=3, num='3.4.1'),
        Heading(name='order_clause', level=4, num='3.4.1.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MultipleExprs', level=3, num='3.4.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MissingExpr.Error', level=3, num='3.4.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.InvalidExpr.Error', level=3, num='3.4.4'),
        Heading(name='FRAME Clause', level=2, num='3.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause', level=3, num='3.5.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause.DefaultFrame', level=3, num='3.5.2'),
        Heading(name='ROWS', level=3, num='3.5.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame', level=4, num='3.5.3.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.MissingFrameExtent.Error', level=4, num='3.5.3.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.InvalidFrameExtent.Error', level=4, num='3.5.3.3'),
        Heading(name='ROWS CURRENT ROW', level=4, num='3.5.3.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.CurrentRow', level=5, num='3.5.3.4.1'),
        Heading(name='ROWS UNBOUNDED PRECEDING', level=4, num='3.5.3.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedPreceding', level=5, num='3.5.3.5.1'),
        Heading(name='ROWS `expr` PRECEDING', level=4, num='3.5.3.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprPreceding', level=5, num='3.5.3.6.1'),
        Heading(name='ROWS UNBOUNDED FOLLOWING', level=4, num='3.5.3.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedFollowing.Error', level=5, num='3.5.3.7.1'),
        Heading(name='ROWS `expr` FOLLOWING', level=4, num='3.5.3.8'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprFollowing.Error', level=5, num='3.5.3.8.1'),
        Heading(name='ROWS BETWEEN CURRENT ROW', level=4, num='3.5.3.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.CurrentRow', level=5, num='3.5.3.9.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedPreceding.Error', level=5, num='3.5.3.9.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprPreceding.Error', level=5, num='3.5.3.9.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedFollowing', level=5, num='3.5.3.9.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprFollowing', level=5, num='3.5.3.9.5'),
        Heading(name='ROWS BETWEEN UNBOUNDED PRECEDING', level=4, num='3.5.3.10'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.CurrentRow', level=5, num='3.5.3.10.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedPreceding.Error', level=5, num='3.5.3.10.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprPreceding', level=5, num='3.5.3.10.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedFollowing', level=5, num='3.5.3.10.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprFollowing', level=5, num='3.5.3.10.5'),
        Heading(name='ROWS BETWEEN UNBOUNDED FOLLOWING', level=4, num='3.5.3.11'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedFollowing.Error', level=5, num='3.5.3.11.1'),
        Heading(name='ROWS BETWEEN `expr` FOLLOWING', level=4, num='3.5.3.12'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.Error', level=5, num='3.5.3.12.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing.Error', level=5, num='3.5.3.12.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.UnboundedFollowing', level=5, num='3.5.3.12.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing', level=5, num='3.5.3.12.4'),
        Heading(name='ROWS BETWEEN `expr` PRECEDING', level=4, num='3.5.3.13'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.CurrentRow', level=5, num='3.5.3.13.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedPreceding.Error', level=5, num='3.5.3.13.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedFollowing', level=5, num='3.5.3.13.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding.Error', level=5, num='3.5.3.13.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding', level=5, num='3.5.3.13.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprFollowing', level=5, num='3.5.3.13.6'),
        Heading(name='RANGE', level=3, num='3.5.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame', level=4, num='3.5.4.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.DateAndDateTime', level=4, num='3.5.4.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.IntAndUInt', level=4, num='3.5.4.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MultipleColumnsInOrderBy.Error', level=4, num='3.5.4.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MissingFrameExtent.Error', level=4, num='3.5.4.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.InvalidFrameExtent.Error', level=4, num='3.5.4.6'),
        Heading(name='`CURRENT ROW` Peers', level=4, num='3.5.4.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.CurrentRow.Peers', level=4, num='3.5.4.8'),
        Heading(name='RANGE CURRENT ROW', level=4, num='3.5.4.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithoutOrderBy', level=5, num='3.5.4.9.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithOrderBy', level=5, num='3.5.4.9.2'),
        Heading(name='RANGE UNBOUNDED FOLLOWING', level=4, num='3.5.4.10'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedFollowing.Error', level=5, num='3.5.4.10.1'),
        Heading(name='RANGE UNBOUNDED PRECEDING', level=4, num='3.5.4.11'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithoutOrderBy', level=5, num='3.5.4.11.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithOrderBy', level=5, num='3.5.4.11.2'),
        Heading(name='RANGE `expr` PRECEDING', level=4, num='3.5.4.12'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithoutOrderBy.Error', level=5, num='3.5.4.12.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.OrderByNonNumericalColumn.Error', level=5, num='3.5.4.12.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithOrderBy', level=5, num='3.5.4.12.3'),
        Heading(name='RANGE `expr` FOLLOWING', level=4, num='3.5.4.13'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.13.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithOrderBy.Error', level=5, num='3.5.4.13.2'),
        Heading(name='RANGE BETWEEN CURRENT ROW', level=4, num='3.5.4.14'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.CurrentRow', level=5, num='3.5.4.14.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedPreceding.Error', level=5, num='3.5.4.14.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedFollowing', level=5, num='3.5.4.14.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.14.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithOrderBy', level=5, num='3.5.4.14.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprPreceding.Error', level=5, num='3.5.4.14.6'),
        Heading(name='RANGE BETWEEN UNBOUNDED PRECEDING', level=4, num='3.5.4.15'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.CurrentRow', level=5, num='3.5.4.15.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedPreceding.Error', level=5, num='3.5.4.15.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedFollowing', level=5, num='3.5.4.15.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithoutOrderBy.Error', level=5, num='3.5.4.15.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithOrderBy', level=5, num='3.5.4.15.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.15.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithOrderBy', level=5, num='3.5.4.15.7'),
        Heading(name='RANGE BETWEEN UNBOUNDED FOLLOWING', level=4, num='3.5.4.16'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.CurrentRow.Error', level=5, num='3.5.4.16.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedFollowing.Error', level=5, num='3.5.4.16.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedPreceding.Error', level=5, num='3.5.4.16.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprPreceding.Error', level=5, num='3.5.4.16.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprFollowing.Error', level=5, num='3.5.4.16.5'),
        Heading(name='RANGE BETWEEN expr PRECEDING', level=4, num='3.5.4.17'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithOrderBy', level=5, num='3.5.4.17.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithoutOrderBy.Error', level=5, num='3.5.4.17.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedPreceding.Error', level=5, num='3.5.4.17.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.17.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithOrderBy', level=5, num='3.5.4.17.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.17.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithOrderBy', level=5, num='3.5.4.17.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithoutOrderBy.Error', level=5, num='3.5.4.17.8'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy.Error', level=5, num='3.5.4.17.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy', level=5, num='3.5.4.17.10'),
        Heading(name='RANGE BETWEEN expr FOLLOWING', level=4, num='3.5.4.18'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithoutOrderBy.Error', level=5, num='3.5.4.18.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithOrderBy.Error', level=5, num='3.5.4.18.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.ZeroSpecialCase', level=5, num='3.5.4.18.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.18.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithOrderBy', level=5, num='3.5.4.18.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedPreceding.Error', level=5, num='3.5.4.18.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithoutOrderBy.Error', level=5, num='3.5.4.18.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.Error', level=5, num='3.5.4.18.8'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithOrderBy.ZeroSpecialCase', level=5, num='3.5.4.18.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithoutOrderBy.Error', level=5, num='3.5.4.18.10'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy.Error', level=5, num='3.5.4.18.11'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy', level=5, num='3.5.4.18.12'),
        Heading(name='Frame Extent', level=3, num='3.5.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Extent', level=4, num='3.5.5.1'),
        Heading(name='Frame Start', level=3, num='3.5.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Start', level=4, num='3.5.6.1'),
        Heading(name='Frame Between', level=3, num='3.5.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Between', level=4, num='3.5.7.1'),
        Heading(name='Frame End', level=3, num='3.5.8'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Frame.End', level=4, num='3.5.8.1'),
        Heading(name='`CURRENT ROW`', level=3, num='3.5.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.CurrentRow', level=4, num='3.5.9.1'),
        Heading(name='`UNBOUNDED PRECEDING`', level=3, num='3.5.10'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedPreceding', level=4, num='3.5.10.1'),
        Heading(name='`UNBOUNDED FOLLOWING`', level=3, num='3.5.11'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedFollowing', level=4, num='3.5.11.1'),
        Heading(name='`expr PRECEDING`', level=3, num='3.5.12'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding', level=4, num='3.5.12.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding.ExprValue', level=4, num='3.5.12.2'),
        Heading(name='`expr FOLLOWING`', level=3, num='3.5.13'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing', level=4, num='3.5.13.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing.ExprValue', level=4, num='3.5.13.2'),
        Heading(name='WINDOW Clause', level=2, num='3.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause', level=3, num='3.6.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MultipleWindows', level=3, num='3.6.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MissingWindowSpec.Error', level=3, num='3.6.3'),
        Heading(name='`OVER` Clause', level=2, num='3.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause', level=3, num='3.7.1'),
        Heading(name='Empty Clause', level=3, num='3.7.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.EmptyOverClause', level=4, num='3.7.2.1'),
        Heading(name='Ad-Hoc Window', level=3, num='3.7.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow', level=4, num='3.7.3.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow.MissingWindowSpec.Error', level=4, num='3.7.3.2'),
        Heading(name='Named Window', level=3, num='3.7.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow', level=4, num='3.7.4.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.InvalidName.Error', level=4, num='3.7.4.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.MultipleWindows.Error', level=4, num='3.7.4.3'),
        Heading(name='Window Functions', level=2, num='3.8'),
        Heading(name='Nonaggregate Functions', level=3, num='3.8.1'),
        Heading(name='The `first_value(expr)` Function', level=4, num='3.8.1.1'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.FirstValue', level=5, num='3.8.1.1.1'),
        Heading(name='The `last_value(expr)` Function', level=4, num='3.8.1.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.LastValue', level=5, num='3.8.1.2.1'),
        Heading(name='The `lag(value, offset)` Function Workaround', level=4, num='3.8.1.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Lag.Workaround', level=5, num='3.8.1.3.1'),
        Heading(name='The `lead(value, offset)` Function Workaround', level=4, num='3.8.1.4'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Lead.Workaround', level=5, num='3.8.1.4.1'),
        Heading(name='The `leadInFrame(expr[, offset, [default]])`', level=4, num='3.8.1.5'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.LeadInFrame', level=5, num='3.8.1.5.1'),
        Heading(name='The `lagInFrame(expr[, offset, [default]])`', level=4, num='3.8.1.6'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.LagInFrame', level=5, num='3.8.1.6.1'),
        Heading(name='The `rank()` Function', level=4, num='3.8.1.7'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.Rank', level=5, num='3.8.1.7.1'),
        Heading(name='The `dense_rank()` Function', level=4, num='3.8.1.8'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.DenseRank', level=5, num='3.8.1.8.1'),
        Heading(name='The `row_number()` Function', level=4, num='3.8.1.9'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.RowNumber', level=5, num='3.8.1.9.1'),
        Heading(name='Aggregate Functions', level=3, num='3.8.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions', level=4, num='3.8.2.1'),
        Heading(name='Combinators', level=4, num='3.8.2.2'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Combinators', level=5, num='3.8.2.2.1'),
        Heading(name='Parametric', level=4, num='3.8.2.3'),
        Heading(name='RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Parametric', level=5, num='3.8.2.3.1'),
        Heading(name='References', level=1, num='4'),
        ),
    requirements=(
        RQ_SRS_019_ClickHouse_WindowFunctions,
        RQ_SRS_019_ClickHouse_WindowFunctions_NonDistributedTables,
        RQ_SRS_019_ClickHouse_WindowFunctions_DistributedTables,
        RQ_SRS_019_ClickHouse_WindowFunctions_WindowSpec,
        RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MultipleExpr,
        RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_MissingExpr_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_PartitionClause_InvalidExpr_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MultipleExprs,
        RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_MissingExpr_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_OrderClause_InvalidExpr_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause_DefaultFrame,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_MissingFrameExtent_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_InvalidFrameExtent_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_DateAndDateTime,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_IntAndUInt,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MultipleColumnsInOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MissingFrameExtent_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_InvalidFrameExtent_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_CurrentRow_Peers,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithoutOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithoutOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_OrderByNonNumericalColumn_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_CurrentRow_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprFollowing_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_ZeroSpecialCase,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithOrderBy_ZeroSpecialCase,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithoutOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy,
        RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Extent,
        RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Start,
        RQ_SRS_019_ClickHouse_WindowFunctions_Frame_Between,
        RQ_SRS_019_ClickHouse_WindowFunctions_Frame_End,
        RQ_SRS_019_ClickHouse_WindowFunctions_CurrentRow,
        RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_UnboundedFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding,
        RQ_SRS_019_ClickHouse_WindowFunctions_ExprPreceding_ExprValue,
        RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing,
        RQ_SRS_019_ClickHouse_WindowFunctions_ExprFollowing_ExprValue,
        RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MultipleWindows,
        RQ_SRS_019_ClickHouse_WindowFunctions_WindowClause_MissingWindowSpec_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_EmptyOverClause,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_AdHocWindow_MissingWindowSpec_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_InvalidName_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_OverClause_NamedWindow_MultipleWindows_Error,
        RQ_SRS_019_ClickHouse_WindowFunctions_FirstValue,
        RQ_SRS_019_ClickHouse_WindowFunctions_LastValue,
        RQ_SRS_019_ClickHouse_WindowFunctions_Lag_Workaround,
        RQ_SRS_019_ClickHouse_WindowFunctions_Lead_Workaround,
        RQ_SRS_019_ClickHouse_WindowFunctions_LeadInFrame,
        RQ_SRS_019_ClickHouse_WindowFunctions_LagInFrame,
        RQ_SRS_019_ClickHouse_WindowFunctions_Rank,
        RQ_SRS_019_ClickHouse_WindowFunctions_DenseRank,
        RQ_SRS_019_ClickHouse_WindowFunctions_RowNumber,
        RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions,
        RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions_Combinators,
        RQ_SRS_019_ClickHouse_WindowFunctions_AggregateFunctions_Parametric,
        ),
    content='''
# SRS019 ClickHouse Window Functions
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Requirements](#requirements)
  * 3.1 [General](#general)
    * 3.1.1 [RQ.SRS-019.ClickHouse.WindowFunctions](#rqsrs-019clickhousewindowfunctions)
    * 3.1.2 [RQ.SRS-019.ClickHouse.WindowFunctions.NonDistributedTables](#rqsrs-019clickhousewindowfunctionsnondistributedtables)
    * 3.1.3 [RQ.SRS-019.ClickHouse.WindowFunctions.DistributedTables](#rqsrs-019clickhousewindowfunctionsdistributedtables)
  * 3.2 [Window Specification](#window-specification)
    * 3.2.1 [RQ.SRS-019.ClickHouse.WindowFunctions.WindowSpec](#rqsrs-019clickhousewindowfunctionswindowspec)
  * 3.3 [PARTITION Clause](#partition-clause)
    * 3.3.1 [RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause](#rqsrs-019clickhousewindowfunctionspartitionclause)
    * 3.3.2 [RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MultipleExpr](#rqsrs-019clickhousewindowfunctionspartitionclausemultipleexpr)
    * 3.3.3 [RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MissingExpr.Error](#rqsrs-019clickhousewindowfunctionspartitionclausemissingexprerror)
    * 3.3.4 [RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.InvalidExpr.Error](#rqsrs-019clickhousewindowfunctionspartitionclauseinvalidexprerror)
  * 3.4 [ORDER Clause](#order-clause)
    * 3.4.1 [RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause](#rqsrs-019clickhousewindowfunctionsorderclause)
      * 3.4.1.1 [order_clause](#order_clause)
    * 3.4.2 [RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MultipleExprs](#rqsrs-019clickhousewindowfunctionsorderclausemultipleexprs)
    * 3.4.3 [RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MissingExpr.Error](#rqsrs-019clickhousewindowfunctionsorderclausemissingexprerror)
    * 3.4.4 [RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.InvalidExpr.Error](#rqsrs-019clickhousewindowfunctionsorderclauseinvalidexprerror)
  * 3.5 [FRAME Clause](#frame-clause)
    * 3.5.1 [RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause](#rqsrs-019clickhousewindowfunctionsframeclause)
    * 3.5.2 [RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause.DefaultFrame](#rqsrs-019clickhousewindowfunctionsframeclausedefaultframe)
    * 3.5.3 [ROWS](#rows)
      * 3.5.3.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame](#rqsrs-019clickhousewindowfunctionsrowsframe)
      * 3.5.3.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.MissingFrameExtent.Error](#rqsrs-019clickhousewindowfunctionsrowsframemissingframeextenterror)
      * 3.5.3.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.InvalidFrameExtent.Error](#rqsrs-019clickhousewindowfunctionsrowsframeinvalidframeextenterror)
      * 3.5.3.4 [ROWS CURRENT ROW](#rows-current-row)
        * 3.5.3.4.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.CurrentRow](#rqsrs-019clickhousewindowfunctionsrowsframestartcurrentrow)
      * 3.5.3.5 [ROWS UNBOUNDED PRECEDING](#rows-unbounded-preceding)
        * 3.5.3.5.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedPreceding](#rqsrs-019clickhousewindowfunctionsrowsframestartunboundedpreceding)
      * 3.5.3.6 [ROWS `expr` PRECEDING](#rows-expr-preceding)
        * 3.5.3.6.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprPreceding](#rqsrs-019clickhousewindowfunctionsrowsframestartexprpreceding)
      * 3.5.3.7 [ROWS UNBOUNDED FOLLOWING](#rows-unbounded-following)
        * 3.5.3.7.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedFollowing.Error](#rqsrs-019clickhousewindowfunctionsrowsframestartunboundedfollowingerror)
      * 3.5.3.8 [ROWS `expr` FOLLOWING](#rows-expr-following)
        * 3.5.3.8.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprFollowing.Error](#rqsrs-019clickhousewindowfunctionsrowsframestartexprfollowingerror)
      * 3.5.3.9 [ROWS BETWEEN CURRENT ROW](#rows-between-current-row)
        * 3.5.3.9.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.CurrentRow](#rqsrs-019clickhousewindowfunctionsrowsframebetweencurrentrowcurrentrow)
        * 3.5.3.9.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweencurrentrowunboundedprecedingerror)
        * 3.5.3.9.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprPreceding.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweencurrentrowexprprecedingerror)
        * 3.5.3.9.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweencurrentrowunboundedfollowing)
        * 3.5.3.9.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweencurrentrowexprfollowing)
      * 3.5.3.10 [ROWS BETWEEN UNBOUNDED PRECEDING](#rows-between-unbounded-preceding)
        * 3.5.3.10.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.CurrentRow](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedprecedingcurrentrow)
        * 3.5.3.10.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedprecedingunboundedprecedingerror)
        * 3.5.3.10.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprPreceding](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedprecedingexprpreceding)
        * 3.5.3.10.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedprecedingunboundedfollowing)
        * 3.5.3.10.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedprecedingexprfollowing)
      * 3.5.3.11 [ROWS BETWEEN UNBOUNDED FOLLOWING](#rows-between-unbounded-following)
        * 3.5.3.11.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedFollowing.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenunboundedfollowingerror)
      * 3.5.3.12 [ROWS BETWEEN `expr` FOLLOWING](#rows-between-expr-following)
        * 3.5.3.12.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprfollowingerror)
        * 3.5.3.12.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprfollowingexprfollowingerror)
        * 3.5.3.12.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprfollowingunboundedfollowing)
        * 3.5.3.12.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprfollowingexprfollowing)
      * 3.5.3.13 [ROWS BETWEEN `expr` PRECEDING](#rows-between-expr-preceding)
        * 3.5.3.13.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.CurrentRow](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingcurrentrow)
        * 3.5.3.13.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingunboundedprecedingerror)
        * 3.5.3.13.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingunboundedfollowing)
        * 3.5.3.13.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding.Error](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingexprprecedingerror)
        * 3.5.3.13.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingexprpreceding)
        * 3.5.3.13.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprFollowing](#rqsrs-019clickhousewindowfunctionsrowsframebetweenexprprecedingexprfollowing)
    * 3.5.4 [RANGE](#range)
      * 3.5.4.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame](#rqsrs-019clickhousewindowfunctionsrangeframe)
      * 3.5.4.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.DateAndDateTime](#rqsrs-019clickhousewindowfunctionsrangeframedatatypesdateanddatetime)
      * 3.5.4.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.IntAndUInt](#rqsrs-019clickhousewindowfunctionsrangeframedatatypesintanduint)
      * 3.5.4.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MultipleColumnsInOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframemultiplecolumnsinorderbyerror)
      * 3.5.4.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MissingFrameExtent.Error](#rqsrs-019clickhousewindowfunctionsrangeframemissingframeextenterror)
      * 3.5.4.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.InvalidFrameExtent.Error](#rqsrs-019clickhousewindowfunctionsrangeframeinvalidframeextenterror)
      * 3.5.4.7 [`CURRENT ROW` Peers](#current-row-peers)
      * 3.5.4.8 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.CurrentRow.Peers](#rqsrs-019clickhousewindowfunctionsrangeframecurrentrowpeers)
      * 3.5.4.9 [RANGE CURRENT ROW](#range-current-row)
        * 3.5.4.9.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithoutOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframestartcurrentrowwithoutorderby)
        * 3.5.4.9.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframestartcurrentrowwithorderby)
      * 3.5.4.10 [RANGE UNBOUNDED FOLLOWING](#range-unbounded-following)
        * 3.5.4.10.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedFollowing.Error](#rqsrs-019clickhousewindowfunctionsrangeframestartunboundedfollowingerror)
      * 3.5.4.11 [RANGE UNBOUNDED PRECEDING](#range-unbounded-preceding)
        * 3.5.4.11.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithoutOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframestartunboundedprecedingwithoutorderby)
        * 3.5.4.11.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframestartunboundedprecedingwithorderby)
      * 3.5.4.12 [RANGE `expr` PRECEDING](#range-expr-preceding)
        * 3.5.4.12.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframestartexprprecedingwithoutorderbyerror)
        * 3.5.4.12.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.OrderByNonNumericalColumn.Error](#rqsrs-019clickhousewindowfunctionsrangeframestartexprprecedingorderbynonnumericalcolumnerror)
        * 3.5.4.12.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframestartexprprecedingwithorderby)
      * 3.5.4.13 [RANGE `expr` FOLLOWING](#range-expr-following)
        * 3.5.4.13.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframestartexprfollowingwithoutorderbyerror)
        * 3.5.4.13.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframestartexprfollowingwithorderbyerror)
      * 3.5.4.14 [RANGE BETWEEN CURRENT ROW](#range-between-current-row)
        * 3.5.4.14.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.CurrentRow](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowcurrentrow)
        * 3.5.4.14.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowunboundedprecedingerror)
        * 3.5.4.14.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowunboundedfollowing)
        * 3.5.4.14.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowexprfollowingwithoutorderbyerror)
        * 3.5.4.14.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowexprfollowingwithorderby)
        * 3.5.4.14.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweencurrentrowexprprecedingerror)
      * 3.5.4.15 [RANGE BETWEEN UNBOUNDED PRECEDING](#range-between-unbounded-preceding)
        * 3.5.4.15.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.CurrentRow](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingcurrentrow)
        * 3.5.4.15.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingunboundedprecedingerror)
        * 3.5.4.15.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingunboundedfollowing)
        * 3.5.4.15.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingexprprecedingwithoutorderbyerror)
        * 3.5.4.15.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingexprprecedingwithorderby)
        * 3.5.4.15.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingexprfollowingwithoutorderbyerror)
        * 3.5.4.15.7 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedprecedingexprfollowingwithorderby)
      * 3.5.4.16 [RANGE BETWEEN UNBOUNDED FOLLOWING](#range-between-unbounded-following)
        * 3.5.4.16.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.CurrentRow.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedfollowingcurrentrowerror)
        * 3.5.4.16.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedFollowing.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedfollowingunboundedfollowingerror)
        * 3.5.4.16.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedfollowingunboundedprecedingerror)
        * 3.5.4.16.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedfollowingexprprecedingerror)
        * 3.5.4.16.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprFollowing.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenunboundedfollowingexprfollowingerror)
      * 3.5.4.17 [RANGE BETWEEN expr PRECEDING](#range-between-expr-preceding)
        * 3.5.4.17.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingcurrentrowwithorderby)
        * 3.5.4.17.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingcurrentrowwithoutorderbyerror)
        * 3.5.4.17.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingunboundedprecedingerror)
        * 3.5.4.17.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingunboundedfollowingwithoutorderbyerror)
        * 3.5.4.17.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingunboundedfollowingwithorderby)
        * 3.5.4.17.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingexprfollowingwithoutorderbyerror)
        * 3.5.4.17.7 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingexprfollowingwithorderby)
        * 3.5.4.17.8 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingexprprecedingwithoutorderbyerror)
        * 3.5.4.17.9 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingexprprecedingwithorderbyerror)
        * 3.5.4.17.10 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprprecedingexprprecedingwithorderby)
      * 3.5.4.18 [RANGE BETWEEN expr FOLLOWING](#range-between-expr-following)
        * 3.5.4.18.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingcurrentrowwithoutorderbyerror)
        * 3.5.4.18.2 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingcurrentrowwithorderbyerror)
        * 3.5.4.18.3 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.ZeroSpecialCase](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingcurrentrowzerospecialcase)
        * 3.5.4.18.4 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingunboundedfollowingwithoutorderbyerror)
        * 3.5.4.18.5 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingunboundedfollowingwithorderby)
        * 3.5.4.18.6 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingunboundedprecedingerror)
        * 3.5.4.18.7 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprprecedingwithoutorderbyerror)
        * 3.5.4.18.8 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprprecedingerror)
        * 3.5.4.18.9 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithOrderBy.ZeroSpecialCase](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprprecedingwithorderbyzerospecialcase)
        * 3.5.4.18.10 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithoutOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprfollowingwithoutorderbyerror)
        * 3.5.4.18.11 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy.Error](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprfollowingwithorderbyerror)
        * 3.5.4.18.12 [RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy](#rqsrs-019clickhousewindowfunctionsrangeframebetweenexprfollowingexprfollowingwithorderby)
    * 3.5.5 [Frame Extent](#frame-extent)
      * 3.5.5.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Extent](#rqsrs-019clickhousewindowfunctionsframeextent)
    * 3.5.6 [Frame Start](#frame-start)
      * 3.5.6.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Start](#rqsrs-019clickhousewindowfunctionsframestart)
    * 3.5.7 [Frame Between](#frame-between)
      * 3.5.7.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Between](#rqsrs-019clickhousewindowfunctionsframebetween)
    * 3.5.8 [Frame End](#frame-end)
      * 3.5.8.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Frame.End](#rqsrs-019clickhousewindowfunctionsframeend)
    * 3.5.9 [`CURRENT ROW`](#current-row)
      * 3.5.9.1 [RQ.SRS-019.ClickHouse.WindowFunctions.CurrentRow](#rqsrs-019clickhousewindowfunctionscurrentrow)
    * 3.5.10 [`UNBOUNDED PRECEDING`](#unbounded-preceding)
      * 3.5.10.1 [RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedPreceding](#rqsrs-019clickhousewindowfunctionsunboundedpreceding)
    * 3.5.11 [`UNBOUNDED FOLLOWING`](#unbounded-following)
      * 3.5.11.1 [RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedFollowing](#rqsrs-019clickhousewindowfunctionsunboundedfollowing)
    * 3.5.12 [`expr PRECEDING`](#expr-preceding)
      * 3.5.12.1 [RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding](#rqsrs-019clickhousewindowfunctionsexprpreceding)
      * 3.5.12.2 [RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding.ExprValue](#rqsrs-019clickhousewindowfunctionsexprprecedingexprvalue)
    * 3.5.13 [`expr FOLLOWING`](#expr-following)
      * 3.5.13.1 [RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing](#rqsrs-019clickhousewindowfunctionsexprfollowing)
      * 3.5.13.2 [RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing.ExprValue](#rqsrs-019clickhousewindowfunctionsexprfollowingexprvalue)
  * 3.6 [WINDOW Clause](#window-clause)
    * 3.6.1 [RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause](#rqsrs-019clickhousewindowfunctionswindowclause)
    * 3.6.2 [RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MultipleWindows](#rqsrs-019clickhousewindowfunctionswindowclausemultiplewindows)
    * 3.6.3 [RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MissingWindowSpec.Error](#rqsrs-019clickhousewindowfunctionswindowclausemissingwindowspecerror)
  * 3.7 [`OVER` Clause](#over-clause)
    * 3.7.1 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause](#rqsrs-019clickhousewindowfunctionsoverclause)
    * 3.7.2 [Empty Clause](#empty-clause)
      * 3.7.2.1 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.EmptyOverClause](#rqsrs-019clickhousewindowfunctionsoverclauseemptyoverclause)
    * 3.7.3 [Ad-Hoc Window](#ad-hoc-window)
      * 3.7.3.1 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow](#rqsrs-019clickhousewindowfunctionsoverclauseadhocwindow)
      * 3.7.3.2 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow.MissingWindowSpec.Error](#rqsrs-019clickhousewindowfunctionsoverclauseadhocwindowmissingwindowspecerror)
    * 3.7.4 [Named Window](#named-window)
      * 3.7.4.1 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow](#rqsrs-019clickhousewindowfunctionsoverclausenamedwindow)
      * 3.7.4.2 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.InvalidName.Error](#rqsrs-019clickhousewindowfunctionsoverclausenamedwindowinvalidnameerror)
      * 3.7.4.3 [RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.MultipleWindows.Error](#rqsrs-019clickhousewindowfunctionsoverclausenamedwindowmultiplewindowserror)
  * 3.8 [Window Functions](#window-functions)
    * 3.8.1 [Nonaggregate Functions](#nonaggregate-functions)
      * 3.8.1.1 [The `first_value(expr)` Function](#the-first_valueexpr-function)
        * 3.8.1.1.1 [RQ.SRS-019.ClickHouse.WindowFunctions.FirstValue](#rqsrs-019clickhousewindowfunctionsfirstvalue)
      * 3.8.1.2 [The `last_value(expr)` Function](#the-last_valueexpr-function)
        * 3.8.1.2.1 [RQ.SRS-019.ClickHouse.WindowFunctions.LastValue](#rqsrs-019clickhousewindowfunctionslastvalue)
      * 3.8.1.3 [The `lag(value, offset)` Function Workaround](#the-lagvalue-offset-function-workaround)
        * 3.8.1.3.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Lag.Workaround](#rqsrs-019clickhousewindowfunctionslagworkaround)
      * 3.8.1.4 [The `lead(value, offset)` Function Workaround](#the-leadvalue-offset-function-workaround)
        * 3.8.1.4.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Lead.Workaround](#rqsrs-019clickhousewindowfunctionsleadworkaround)
      * 3.8.1.5 [The `leadInFrame(expr[, offset, [default]])`](#the-leadinframeexpr-offset-default)
        * 3.8.1.5.1 [RQ.SRS-019.ClickHouse.WindowFunctions.LeadInFrame](#rqsrs-019clickhousewindowfunctionsleadinframe)
      * 3.8.1.6 [The `lagInFrame(expr[, offset, [default]])`](#the-laginframeexpr-offset-default)
        * 3.8.1.6.1 [RQ.SRS-019.ClickHouse.WindowFunctions.LagInFrame](#rqsrs-019clickhousewindowfunctionslaginframe)
      * 3.8.1.7 [The `rank()` Function](#the-rank-function)
        * 3.8.1.7.1 [RQ.SRS-019.ClickHouse.WindowFunctions.Rank](#rqsrs-019clickhousewindowfunctionsrank)
      * 3.8.1.8 [The `dense_rank()` Function](#the-dense_rank-function)
        * 3.8.1.8.1 [RQ.SRS-019.ClickHouse.WindowFunctions.DenseRank](#rqsrs-019clickhousewindowfunctionsdenserank)
      * 3.8.1.9 [The `row_number()` Function](#the-row_number-function)
        * 3.8.1.9.1 [RQ.SRS-019.ClickHouse.WindowFunctions.RowNumber](#rqsrs-019clickhousewindowfunctionsrownumber)
    * 3.8.2 [Aggregate Functions](#aggregate-functions)
      * 3.8.2.1 [RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions](#rqsrs-019clickhousewindowfunctionsaggregatefunctions)
      * 3.8.2.2 [Combinators](#combinators)
        * 3.8.2.2.1 [RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Combinators](#rqsrs-019clickhousewindowfunctionsaggregatefunctionscombinators)
      * 3.8.2.3 [Parametric](#parametric)
        * 3.8.2.3.1 [RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Parametric](#rqsrs-019clickhousewindowfunctionsaggregatefunctionsparametric)
* 4 [References](#references)


## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a [GitHub Repository].
All the updates are tracked using the [Revision History].

## Introduction

This software requirements specification covers requirements for supporting window functions in [ClickHouse].
Similar functionality exists in [MySQL] and [PostreSQL]. [PostreSQL] defines a window function as follows:

> A window function performs a calculation across a set of table rows that are somehow related to the current row.
> This is comparable to the type of calculation that can be done with an aggregate function.
> But unlike regular aggregate functions, use of a window function does not cause rows to
> become grouped into a single output row — the rows retain their separate identities.

## Requirements

### General

#### RQ.SRS-019.ClickHouse.WindowFunctions
version: 1.0

[ClickHouse] SHALL support [window functions] that produce a result for each row inside the window.

#### RQ.SRS-019.ClickHouse.WindowFunctions.NonDistributedTables
version: 1.0

[ClickHouse] SHALL support correct operation of [window functions] on non-distributed 
table engines such as `MergeTree`.


#### RQ.SRS-019.ClickHouse.WindowFunctions.DistributedTables
version: 1.0

[ClickHouse] SHALL support correct operation of [window functions] on
[Distributed](https://clickhouse.tech/docs/en/engines/table-engines/special/distributed/) table engine.

### Window Specification

#### RQ.SRS-019.ClickHouse.WindowFunctions.WindowSpec
version: 1.0

[ClickHouse] SHALL support defining a window using window specification clause.
The [window_spec] SHALL be defined as

```
window_spec:
   [partition_clause] [order_clause] [frame_clause]
```

that SHALL specify how to partition query rows into groups for processing by the window function.

### PARTITION Clause

#### RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause
version: 1.0

[ClickHouse] SHALL support [partition_clause] that indicates how to divide the query rows into groups.
The [partition_clause] SHALL be defined as

```
partition_clause:
    PARTITION BY expr [, expr] ...
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MultipleExpr
version: 1.0

[ClickHouse] SHALL support partitioning by more than one `expr` in the [partition_clause] definition.

For example,

```sql
SELECT x,s, sum(x) OVER (PARTITION BY x,s) FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))
```

```bash
┌─x─┬─s─┬─sum(x) OVER (PARTITION BY x, s)─┐
│ 1 │ a │                               1 │
│ 1 │ b │                               1 │
│ 2 │ b │                               2 │
└───┴───┴─────────────────────────────────┘
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.MissingExpr.Error
version: 1.0

[ClickHouse] SHALL return an error if `expr` is missing in the [partition_clause] definition.

```sql
SELECT sum(number) OVER (PARTITION BY) FROM numbers(1,3)
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.PartitionClause.InvalidExpr.Error
version: 1.0

[ClickHouse] SHALL return an error if `expr` is invalid in the [partition_clause] definition.

### ORDER Clause

#### RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause
version: 1.0

[ClickHouse] SHALL support [order_clause] that indicates how to sort rows in each window.

##### order_clause

The `order_clause` SHALL be defined as

```
order_clause:
    ORDER BY expr [ASC|DESC] [, expr [ASC|DESC]] ...
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MultipleExprs
version: 1.0

[ClickHouse] SHALL return support using more than one `expr` in the [order_clause] definition.

For example, 

```sql
SELECT x,s, sum(x) OVER (ORDER BY x DESC, s DESC) FROM values('x Int8, s String', (1,'a'),(1,'b'),(2,'b'))
```

```bash
┌─x─┬─s─┬─sum(x) OVER (ORDER BY x DESC, s DESC)─┐
│ 2 │ b │                                     2 │
│ 1 │ b │                                     3 │
│ 1 │ a │                                     4 │
└───┴───┴───────────────────────────────────────┘
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.MissingExpr.Error
version: 1.0

[ClickHouse] SHALL return an error if `expr` is missing in the [order_clause] definition.

#### RQ.SRS-019.ClickHouse.WindowFunctions.OrderClause.InvalidExpr.Error
version: 1.0

[ClickHouse] SHALL return an error if `expr` is invalid in the [order_clause] definition.

### FRAME Clause

#### RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause
version: 1.0

[ClickHouse] SHALL support [frame_clause] that SHALL specify a subset of the current window.

The `frame_clause` SHALL be defined as

```
frame_clause:
    {ROWS | RANGE } frame_extent
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.FrameClause.DefaultFrame
version: 1.0

[ClickHouse] SHALL support the default `frame_clause` to be `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. 

If the `ORDER BY` clause is specified then this SHALL set the frame to be all rows from 
the partition start up to and including current row and its peers. 

If the `ORDER BY` clause is not specified then this SHALL set the frame to include all rows
in the partition because all the rows are considered to be the peers of the current row.

#### ROWS

##### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame
version: 1.0

[ClickHouse] SHALL support `ROWS` frame to define beginning and ending row positions.
Offsets SHALL be differences in row numbers from the current row number.

```sql
ROWS frame_extent
```

See [frame_extent] definition.

##### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.MissingFrameExtent.Error
version: 1.0

[ClickHouse] SHALL return an error if the `ROWS` frame clause is defined without [frame_extent].

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number ROWS) FROM numbers(1,3)
```

##### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.InvalidFrameExtent.Error
version: 1.0

[ClickHouse] SHALL return an error if the `ROWS` frame clause has invalid [frame_extent].

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number ROWS '1') FROM numbers(1,3)
```

##### ROWS CURRENT ROW

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.CurrentRow
version: 1.0

[ClickHouse] SHALL include only the current row in the window partition
when `ROWS CURRENT ROW` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS CURRENT ROW) FROM numbers(1,2)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                           1 │
│      2 │                                                           2 │
└────────┴─────────────────────────────────────────────────────────────┘
```

##### ROWS UNBOUNDED PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedPreceding
version: 1.0

[ClickHouse] SHALL include all rows before and including the current row in the window partition
when `ROWS UNBOUNDED PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS UNBOUNDED PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                   1 │
│      2 │                                                                   3 │
│      3 │                                                                   6 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

##### ROWS `expr` PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprPreceding
version: 1.0

[ClickHouse] SHALL include `expr` rows before and including the current row in the window partition 
when `ROWS expr PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS 1 PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)─┐
│      1 │                                                           1 │
│      2 │                                                           3 │
│      3 │                                                           5 │
└────────┴─────────────────────────────────────────────────────────────┘
```

##### ROWS UNBOUNDED FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.UnboundedFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS UNBOUNDED FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

##### ROWS `expr` FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Start.ExprFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS expr FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS 1 FOLLOWING) FROM numbers(1,3)
```

##### ROWS BETWEEN CURRENT ROW

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.CurrentRow
version: 1.0

[ClickHouse] SHALL include only the current row in the window partition
when `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,2)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                           1 │
│      2 │                                                           2 │
└────────┴─────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING` frame is specified.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS BETWEEN CURRENT ROW AND expr PRECEDING` frame is specified.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include the current row and all the following rows in the window partition
when `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                   6 │
│      2 │                                                                   5 │
│      3 │                                                                   3 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.CurrentRow.ExprFollowing
version: 1.0

[ClickHouse] SHALL include the current row and the `expr` rows that are following the current row in the window partition
when `ROWS BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)─┐
│      1 │                                                           3 │
│      2 │                                                           5 │
│      3 │                                                           3 │
└────────┴─────────────────────────────────────────────────────────────┘
```

##### ROWS BETWEEN UNBOUNDED PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.CurrentRow
version: 1.0

[ClickHouse] SHALL include all the rows before and including the current row in the window partition
when `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                   1 │
│      2 │                                                                   3 │
│      3 │                                                                   6 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprPreceding
version: 1.0

[ClickHouse] SHALL include all the rows until and including the current row minus `expr` rows preceding it
when `ROWS BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)─┐
│      1 │                                                                   0 │
│      2 │                                                                   1 │
│      3 │                                                                   3 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include all rows in the window partition 
when `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                           6 │
│      2 │                                                                           6 │
│      3 │                                                                           6 │
└────────┴─────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedPreceding.ExprFollowing
version: 1.0

[ClickHouse] SHALL include all the rows until and including the current row plus `expr` rows following it
when `ROWS BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)─┐
│      1 │                                                                   3 │
│      2 │                                                                   6 │
│      3 │                                                                   6 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

##### ROWS BETWEEN UNBOUNDED FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.UnboundedFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `UNBOUNDED FOLLOWING` is specified as the start of the frame, including

* `ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW`
* `ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING`
* `ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING`
* `ROWS BETWEEN UNBOUNDED FOLLOWING AND expr PRECEDING`
* `ROWS BETWEEN UNBOUNDED FOLLOWING AND expr FOLLOWING`

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM numbers(1,3)
```

##### ROWS BETWEEN `expr` FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `expr FOLLOWING` is specified as the start of the frame
and it points to a row that is after the start of the frame inside the window partition such
as the following cases

* `ROWS BETWEEN expr FOLLOWING AND CURRENT ROW`
* `ROWS BETWEEN expr FOLLOWING AND UNBOUNDED PRECEDING`
* `ROWS BETWEEN expr FOLLOWING AND expr PRECEDING`

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `ROWS BETWEEN expr FOLLOWING AND expr FOLLOWING`
is specified and the end of the frame specified by the `expr FOLLOWING` is a row that is before the row 
specified by the frame start.

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include all the rows from and including current row plus `expr` rows following it 
until and including the last row in the window partition
when `ROWS BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                   5 │
│      2 │                                                                   3 │
│      3 │                                                                   0 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprFollowing.ExprFollowing
version: 1.0

[ClickHouse] SHALL include the rows from and including current row plus `expr` following it 
until and including the row specified by the frame end when the frame end 
is the current row plus `expr` following it is right at or after the start of the frame
when `ROWS BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)─┐
│      1 │                                                           5 │
│      2 │                                                           3 │
│      3 │                                                           0 │
└────────┴─────────────────────────────────────────────────────────────┘
```

##### ROWS BETWEEN `expr` PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.CurrentRow
version: 1.0

[ClickHouse] SHALL include the rows from and including current row minus `expr` rows
preceding it until and including the current row in the window frame
when `ROWS BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)─┐
│      1 │                                                           1 │
│      2 │                                                           3 │
│      3 │                                                           5 │
└────────┴─────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error
when `ROWS BETWEEN expr PRECEDING AND UNBOUNDED PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include the rows from and including current row minus `expr` rows
preceding it until and including the last row in the window partition
when `ROWS BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                   6 │
│      2 │                                                                   6 │
│      3 │                                                                   5 │
└────────┴─────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when the frame end specified by the `expr PRECEDING`
evaluates to a row that is before the row specified by the frame start in the window partition
when `ROWS BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprPreceding
version: 1.0

[ClickHouse] SHALL include the rows from and including current row minus `expr` rows preceding it
until and including the current row minus `expr` rows preceding it if the end
of the frame is after the frame start in the window partition 
when `ROWS BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING)─┐
│      1 │                                                           1 │
│      2 │                                                           3 │
│      3 │                                                           5 │
└────────┴─────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowsFrame.Between.ExprPreceding.ExprFollowing
version: 1.0

[ClickHouse] SHALL include the rows from and including current row minus `expr` rows preceding it
until and including the current row plus `expr` rows following it in the window partition
when `ROWS BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified.

For example,

```sql
SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)─┐
│      1 │                                                           3 │
│      2 │                                                           6 │
│      3 │                                                           5 │
└────────┴─────────────────────────────────────────────────────────────┘
```

#### RANGE

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame
version: 1.0

[ClickHouse] SHALL support `RANGE` frame to define rows within a value range.
Offsets SHALL be differences in row values from the current row value.

```sql
RANGE frame_extent
```

See [frame_extent] definition.

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.DateAndDateTime
version: 1.0

[ClickHouse] SHALL support `RANGE` frame over columns with `Date` and `DateTime`
data types.

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.DataTypes.IntAndUInt
version: 1.0

[ClickHouse] SHALL support `RANGE` frame over columns with numerical data types
such `IntX` and `UIntX`.

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MultipleColumnsInOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error if the `RANGE` frame definition is used with `ORDER BY`
that uses multiple columns.

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.MissingFrameExtent.Error
version: 1.0

[ClickHouse] SHALL return an error if the `RANGE` frame definition is missing [frame_extent].

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.InvalidFrameExtent.Error
version: 1.0

[ClickHouse] SHALL return an error if the `RANGE` frame definition has invalid [frame_extent].

##### `CURRENT ROW` Peers

##### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.CurrentRow.Peers
version: 1.0

[ClickHouse] for the `RANGE` frame SHALL define the `peers` of the `CURRENT ROW` to be all
the rows that are inside the same order bucket.

##### RANGE CURRENT ROW

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithoutOrderBy
version: 1.0

[ClickHouse] SHALL include all rows in the window partition
when `RANGE CURRENT ROW` frame is specified without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE CURRENT ROW) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                            6 │
│      2 │                                                            6 │
│      3 │                                                            6 │
└────────┴──────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.CurrentRow.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows that are [current row peers] in the window partition
when `RANGE CURRENT ROW` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                                                2 │
│      1 │                                                                                2 │
│      2 │                                                                                2 │
│      3 │                                                                                3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

##### RANGE UNBOUNDED FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE UNBOUNDED FOLLOWING` frame is specified with or without order by
as `UNBOUNDED FOLLOWING` SHALL not be supported as [frame_start].

For example,

```sql
SELECT number,sum(number) OVER (RANGE UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

##### RANGE UNBOUNDED PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithoutOrderBy
version: 1.0

[ClickHouse] SHALL include all rows in the window partition
when `RANGE UNBOUNDED PRECEDING` frame is specified without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE UNBOUNDED PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                    6 │
│      2 │                                                                    6 │
│      3 │                                                                    6 │
└────────┴──────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.UnboundedPreceding.WithOrderBy
version: 1.0

[ClickHouse] SHALL include rows with values from and including the first row 
until and including all [current row peers] in the window partition
when `RANGE UNBOUNDED PRECEDING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE UNBOUNDED PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                                        1 │
│      2 │                                                                                        3 │
│      3 │                                                                                        6 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

##### RANGE `expr` PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE expr PRECEDING` frame is specified without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE 1 PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.OrderByNonNumericalColumn.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE expr PRECEDING` is used with `ORDER BY` clause
over a non-numerical column.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprPreceding.WithOrderBy
version: 1.0

[ClickHouse] SHALL include rows with values from and including current row value minus `expr`
until and including the value for the current row 
when `RANGE expr PRECEDING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE 1 PRECEDING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                                1 │
│      2 │                                                                                3 │
│      3 │                                                                                5 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

##### RANGE `expr` FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE expr FOLLOWING` frame is specified without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE 1 FOLLOWING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Start.ExprFollowing.WithOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE expr FOLLOWING` frame is specified wit the `ORDER BY` clause 
as the value for the frame start cannot be larger than the value for the frame end.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE 1 FOLLOWING) FROM numbers(1,3)
```

##### RANGE BETWEEN CURRENT ROW

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.CurrentRow
version: 1.0

[ClickHouse] SHALL include all [current row peers] in the window partition 
when `RANGE BETWEEN CURRENT ROW AND CURRENT ROW` frame is specified with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**  

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                            6 │
│      2 │                                                            6 │
│      3 │                                                            6 │
└────────┴──────────────────────────────────────────────────────────────┘
```

**With `ORDER BY`**  

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND CURRENT ROW)─┐
│      1 │                                                                                1 │
│      2 │                                                                                2 │
│      3 │                                                                                3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING` frame is specified
with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include all rows with values from and including [current row peers] until and including
the last row in the window partition when `RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` frame is specified
with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                    6 │
│      2 │                                                                    6 │
│      3 │                                                                    6 │
└────────┴──────────────────────────────────────────────────────────────────────┘
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                                        6 │
│      2 │                                                                                        5 │
│      3 │                                                                                        3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including [current row peers] until and including
current row value plus `expr` when `RANGE BETWEEN CURRENT ROW AND expr FOLLOWING` frame is specified
with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)─┐
│      1 │                                                                                4 │
│      1 │                                                                                4 │
│      2 │                                                                                5 │
│      3 │                                                                                3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.CurrentRow.ExprPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN CURRENT ROW AND expr PRECEDING` frame is specified
with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)
```

##### RANGE BETWEEN UNBOUNDED PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.CurrentRow
version: 1.0

[ClickHouse] SHALL include all rows with values from and including the first row until and including
[current row peers] in the window partition when `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame is specified
with and without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                    7 │
│      1 │                                                                    7 │
│      2 │                                                                    7 │
│      3 │                                                                    7 │
└────────┴──────────────────────────────────────────────────────────────────────┘
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                                        2 │
│      1 │                                                                                        2 │
│      2 │                                                                                        4 │
│      3 │                                                                                        7 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return and error when `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING` frame is specified
with and without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL include all rows in the window partition when `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame is specified
with and without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                            7 │
│      1 │                                                                            7 │
│      2 │                                                                            7 │
│      3 │                                                                            7 │
└────────┴──────────────────────────────────────────────────────────────────────────────┘
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                                                7 │
│      1 │                                                                                                7 │
│      2 │                                                                                                7 │
│      3 │                                                                                                7 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprPreceding.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including the first row until and including
the value of the current row minus `expr` in the window partition
when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)─┐
│      1 │                                                                                        0 │
│      1 │                                                                                        0 │
│      2 │                                                                                        2 │
│      3 │                                                                                        4 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedPreceding.ExprFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including the first row until and including
the value of the current row plus `expr` in the window partition
when `RANGE BETWEEN UNBOUNDED PRECEDING AND expr FOLLOWING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)─┐
│      1 │                                                                                        4 │
│      1 │                                                                                        4 │
│      2 │                                                                                        7 │
│      3 │                                                                                        7 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

##### RANGE BETWEEN UNBOUNDED FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.CurrentRow.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW` frame is specified 
with or without the `ORDER BY` clause.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified 
with or without the `ORDER BY` clause.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING` frame is specified 
with or without the `ORDER BY` clause.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND expr PRECEDING` frame is specified 
with or without the `ORDER BY` clause.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.UnboundedFollowing.ExprFollowing.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN UNBOUNDED FOLLOWING AND expr FOLLOWING` frame is specified 
with or without the `ORDER BY` clause.

##### RANGE BETWEEN expr PRECEDING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including current row minus `expr` 
until and including [current row peers] in the window partition
when `RANGE BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)─┐
│      1 │                                                                                2 │
│      1 │                                                                                2 │
│      2 │                                                                                4 │
│      3 │                                                                                5 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.CurrentRow.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND CURRENT ROW` frame is specified
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED PRECEDING` frame is specified 
with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame is specified 
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.UnboundedFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including current row minus `expr` 
until and including the last row in the window partition when `RANGE BETWEEN expr PRECEDING AND UNBOUNDED FOLLOWING` frame
is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                                        7 │
│      1 │                                                                                        7 │
│      2 │                                                                                        7 │
│      3 │                                                                                        5 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified 
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including current row minus preceding `expr` 
until and including current row plus following `expr` in the window partition 
when `RANGE BETWEEN expr PRECEDING AND expr FOLLOWING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)─┐
│      1 │                                                                                4 │
│      1 │                                                                                4 │
│      2 │                                                                                7 │
│      3 │                                                                                5 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified 
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM numbers(1,3)
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when the value of the [frame_end] specified by the 
current row minus preceding `expr` is greater than the value of the [frame_start] in the window partition
when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprPreceding.ExprPreceding.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including current row minus preceding `expr` for the [frame_start]
until and including current row minus following `expr` for the [frame_end] in the window partition 
when `RANGE BETWEEN expr PRECEDING AND expr PRECEDING` frame is specified with the `ORDER BY` clause
if an only if the [frame_end] value is equal or greater than [frame_start] value.

For example,

**Greater Than**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING)─┐
│      1 │                                                                                2 │
│      1 │                                                                                2 │
│      2 │                                                                                4 │
│      3 │                                                                                5 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

or **Equal**

```sql
 SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 PRECEDING AND 1 PRECEDING)─┐
│      1 │                                                                                0 │
│      1 │                                                                                0 │
│      2 │                                                                                2 │
│      3 │                                                                                2 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

##### RANGE BETWEEN expr FOLLOWING

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified 
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.WithOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified 
with the `ORDER BY` clause and `expr` is greater than `0`.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.CurrentRow.ZeroSpecialCase
version: 1.0

[ClickHouse] SHALL include all [current row peers] in the window partition
when `RANGE BETWEEN expr FOLLOWING AND CURRENT ROW` frame is specified 
with the `ORDER BY` clause if and only if the `expr` equals to `0`.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW)─┐
│      1 │                                                            7 │
│      1 │                                                            7 │
│      2 │                                                            7 │
│      3 │                                                            7 │
└────────┴──────────────────────────────────────────────────────────────┘
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW)─┐
│      1 │                                                                                2 │
│      1 │                                                                                2 │
│      2 │                                                                                2 │
│      3 │                                                                                3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified 
without the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with values from and including current row plus `expr`
until and including the last row in the window partition 
when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED FOLLOWING` frame is specified with the `ORDER BY` clause.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)─┐
│      1 │                                                                                        5 │
│      1 │                                                                                        5 │
│      2 │                                                                                        3 │
│      3 │                                                                                        0 │
└────────┴──────────────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.UnboundedPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND UNBOUNDED PRECEDING` frame is specified 
with or without the `ORDER BY` clause.

For example,

**Without `ORDER BY`**

```sql
SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

**With `ORDER BY`**

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified 
without the `ORDER BY`.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified 
with the `ORDER BY` clause if the value of both `expr` is not `0`.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprPreceding.WithOrderBy.ZeroSpecialCase
version: 1.0

[ClickHouse] SHALL include all rows with value equal to [current row peers] in the window partition
when `RANGE BETWEEN expr FOLLOWING AND expr PRECEDING` frame is specified 
with the `ORDER BY` clause if and only if both `expr`'s are `0`.

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING ─┐
│      1 │                                                                                2 │
│      1 │                                                                                2 │
│      2 │                                                                                2 │
│      3 │                                                                                3 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithoutOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified 
without the `ORDER BY` clause.

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy.Error
version: 1.0

[ClickHouse] SHALL return an error when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified 
with the `ORDER BY` clause but the `expr` for the [frame_end] is less than the `expr` for the [frame_start].

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

###### RQ.SRS-019.ClickHouse.WindowFunctions.RangeFrame.Between.ExprFollowing.ExprFollowing.WithOrderBy
version: 1.0

[ClickHouse] SHALL include all rows with value from and including current row plus `expr` for the [frame_start]
until and including current row plus `expr` for the [frame_end] in the window partition
when `RANGE BETWEEN expr FOLLOWING AND expr FOLLOWING` frame is specified 
with the `ORDER BY` clause if and only if the `expr` for the [frame_end] is greater than or equal than the 
`expr` for the [frame_start].

For example,

```sql
SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))
```

```bash
┌─number─┬─sum(number) OVER (ORDER BY number ASC RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING)─┐
│      1 │                                                                                5 │
│      1 │                                                                                5 │
│      2 │                                                                                3 │
│      3 │                                                                                0 │
└────────┴──────────────────────────────────────────────────────────────────────────────────┘
```

#### Frame Extent

##### RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Extent
version: 1.0

[ClickHouse] SHALL support [frame_extent] defined as

```
frame_extent:
    {frame_start | frame_between}
```

#### Frame Start

##### RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Start
version: 1.0

[ClickHouse] SHALL support [frame_start] defined as

```
frame_start: {
    CURRENT ROW
  | UNBOUNDED PRECEDING
  | UNBOUNDED FOLLOWING
  | expr PRECEDING
  | expr FOLLOWING
}
```

#### Frame Between

##### RQ.SRS-019.ClickHouse.WindowFunctions.Frame.Between
version: 1.0

[ClickHouse] SHALL support [frame_between] defined as

```
frame_between:
    BETWEEN frame_start AND frame_end
```

#### Frame End

##### RQ.SRS-019.ClickHouse.WindowFunctions.Frame.End
version: 1.0

[ClickHouse] SHALL support [frame_end] defined as

```
frame_end: {
    CURRENT ROW
  | UNBOUNDED PRECEDING
  | UNBOUNDED FOLLOWING
  | expr PRECEDING
  | expr FOLLOWING
}
```

#### `CURRENT ROW`

##### RQ.SRS-019.ClickHouse.WindowFunctions.CurrentRow
version: 1.0

[ClickHouse] SHALL support `CURRENT ROW` as `frame_start` or `frame_end` value.

* For `ROWS` SHALL define the bound to be the current row
* For `RANGE` SHALL define the bound to be the peers of the current row

#### `UNBOUNDED PRECEDING`

##### RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedPreceding
version: 1.0

[ClickHouse] SHALL support `UNBOUNDED PRECEDING` as `frame_start` or `frame_end` value
and it SHALL define that the bound is the first partition row.

#### `UNBOUNDED FOLLOWING`

##### RQ.SRS-019.ClickHouse.WindowFunctions.UnboundedFollowing
version: 1.0

[ClickHouse] SHALL support `UNBOUNDED FOLLOWING` as `frame_start` or `frame_end` value
and it SHALL define that the bound is the last partition row.

#### `expr PRECEDING`

##### RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding
version: 1.0

[ClickHouse] SHALL support `expr PRECEDING` as `frame_start` or `frame_end` value

* For `ROWS` it SHALL define the bound to be the `expr` rows before the current row
* For `RANGE` it SHALL define the bound to be the rows with values equal to the current row value minus the `expr`.

##### RQ.SRS-019.ClickHouse.WindowFunctions.ExprPreceding.ExprValue
version: 1.0

[ClickHouse] SHALL support only non-negative numeric literal as the value for the `expr` in the `expr PRECEDING` frame boundary.

For example,

```
5 PRECEDING
```

#### `expr FOLLOWING`

##### RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing
version: 1.0

[ClickHouse] SHALL support `expr FOLLOWING` as `frame_start` or `frame_end` value

* For `ROWS` it SHALL define the bound to be the `expr` rows after the current row
* For `RANGE` it SHALL define  the bound to be the rows with values equal to the current row value plus `expr`

##### RQ.SRS-019.ClickHouse.WindowFunctions.ExprFollowing.ExprValue
version: 1.0

[ClickHouse] SHALL support only non-negative numeric literal as the value for the `expr` in the `expr FOLLOWING` frame boundary.

For example,

```
5 FOLLOWING
```

### WINDOW Clause

#### RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause
version: 1.0

[ClickHouse] SHALL support `WINDOW` clause to define one or more windows.

```sql
WINDOW window_name AS (window_spec)
    [, window_name AS (window_spec)] ..
```

The `window_name` SHALL be the name of a window defined by a `WINDOW` clause.

The [window_spec] SHALL specify the window.

For example,

```sql
SELECT ... FROM table WINDOW w AS (partiton by id))
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MultipleWindows
version: 1.0

[ClickHouse] SHALL support `WINDOW` clause that defines multiple windows.

For example,

```sql
SELECT ... FROM table WINDOW w1 AS (partition by id), w2 AS (partition by customer)
```

#### RQ.SRS-019.ClickHouse.WindowFunctions.WindowClause.MissingWindowSpec.Error
version: 1.0

[ClickHouse] SHALL return an error if the `WINDOW` clause definition is missing [window_spec].

### `OVER` Clause

#### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause
version: 1.0

[ClickHouse] SHALL support `OVER` clause to either use named window defined using `WINDOW` clause
or adhoc window defined inplace.


```
OVER ()|(window_spec)|named_window 
```

#### Empty Clause

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.EmptyOverClause
version: 1.0

[ClickHouse] SHALL treat the entire set of query rows as a single partition when `OVER` clause is empty.
For example,

```
SELECT sum(x) OVER () FROM table
```

#### Ad-Hoc Window

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow
version: 1.0

[ClickHouse] SHALL support ad hoc window specification in the `OVER` clause.

```
OVER [window_spec]
```

See [window_spec] definition.

For example,

```sql
(count(*) OVER (partition by id order by time desc))
```

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.AdHocWindow.MissingWindowSpec.Error
version: 1.0

[ClickHouse] SHALL return an error if the `OVER` clause has missing [window_spec].

#### Named Window

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow
version: 1.0

[ClickHouse] SHALL support using a previously defined named window in the `OVER` clause.

```
OVER [window_name]
```

See [window_name] definition.

For example,

```sql
SELECT count(*) OVER w FROM table WINDOW w AS (partition by id)
```

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.InvalidName.Error
version: 1.0

[ClickHouse] SHALL return an error if the `OVER` clause reference invalid window name.

##### RQ.SRS-019.ClickHouse.WindowFunctions.OverClause.NamedWindow.MultipleWindows.Error
version: 1.0

[ClickHouse] SHALL return an error if the `OVER` clause references more than one window name.

### Window Functions

#### Nonaggregate Functions

##### The `first_value(expr)` Function

###### RQ.SRS-019.ClickHouse.WindowFunctions.FirstValue
version: 1.0

[ClickHouse] SHALL support `first_value` window function that
SHALL be synonum for the `any(value)` function
that SHALL return the value of `expr` from first row in the window frame.

```
first_value(expr) OVER ...
```

##### The `last_value(expr)` Function

###### RQ.SRS-019.ClickHouse.WindowFunctions.LastValue
version: 1.0

[ClickHouse] SHALL support `last_value` window function that
SHALL be synonym for the `anyLast(value)` function
that SHALL return the value of `expr` from the last row in the window frame.

```
last_value(expr) OVER ...
```

##### The `lag(value, offset)` Function Workaround

###### RQ.SRS-019.ClickHouse.WindowFunctions.Lag.Workaround
version: 1.0

[ClickHouse] SHALL support a workaround for the `lag(value, offset)` function as

```
any(value) OVER (.... ROWS BETWEEN <offset> PRECEDING AND <offset> PRECEDING)
```

The function SHALL returns the value from the row that lags (precedes) the current row
by the `N` rows within its partition. Where `N` is the `value` passed to the `any` function.

If there is no such row, the return value SHALL be default.

For example, if `N` is 3, the return value is default for the first two rows.
If N or default are missing, the defaults are 1 and NULL, respectively.

`N` SHALL be a literal non-negative integer. If N is 0, the value SHALL be
returned for the current row.

##### The `lead(value, offset)` Function Workaround

###### RQ.SRS-019.ClickHouse.WindowFunctions.Lead.Workaround
version: 1.0

[ClickHouse] SHALL support a workaround for the `lead(value, offset)` function as

```
any(value) OVER (.... ROWS BETWEEN <offset> FOLLOWING AND <offset> FOLLOWING)
```

The function SHALL returns the value from the row that leads (follows) the current row by
the `N` rows within its partition. Where `N` is the `value` passed to the `any` function.

If there is no such row, the return value SHALL be default.

For example, if `N` is 3, the return value is default for the last two rows.
If `N` or default are missing, the defaults are 1 and NULL, respectively.

`N` SHALL be a literal non-negative integer. If `N` is 0, the value SHALL be
returned for the current row.

##### The `leadInFrame(expr[, offset, [default]])`

###### RQ.SRS-019.ClickHouse.WindowFunctions.LeadInFrame
version: 1.0

[ClickHouse] SHALL support the `leadInFrame(expr[, offset, [default]])` function.

For example,
```
leadInFrame(column) OVER (...)
```

The function SHALL return the value from the row that leads (follows) the current row
by the `offset` rows within the current frame. If there is no such row,
the return value SHALL be the `default` value. If the `default` value is not specified 
then the default value for the corresponding column data type SHALL be returned.

The `offset` SHALL be a literal non-negative integer. If the `offset` is set to `0`, then
the value SHALL be returned for the current row. If the `offset` is not specified, the default
value SHALL be `1`.

##### The `lagInFrame(expr[, offset, [default]])`

###### RQ.SRS-019.ClickHouse.WindowFunctions.LagInFrame
version: 1.0

[ClickHouse] SHALL support the `lagInFrame(expr[, offset, [default]])` function.

For example,
```
lagInFrame(column) OVER (...)
```

The function SHALL return the value from the row that lags (preceds) the current row
by the `offset` rows within the current frame. If there is no such row,
the return value SHALL be the `default` value. If the `default` value is not specified 
then the default value for the corresponding column data type SHALL be returned.

The `offset` SHALL be a literal non-negative integer. If the `offset` is set to `0`, then
the value SHALL be returned for the current row. If the `offset` is not specified, the default
value SHALL be `1`.

##### The `rank()` Function

###### RQ.SRS-019.ClickHouse.WindowFunctions.Rank
version: 1.0

[ClickHouse] SHALL support `rank` window function that SHALL
return the rank of the current row within its partition with gaps.

Peers SHALL be considered ties and receive the same rank.
The function SHALL not assign consecutive ranks to peer groups if groups of size greater than one exist
and the result is noncontiguous rank numbers.

If the function is used without `ORDER BY` to sort partition rows into the desired order
then all rows SHALL be peers.

```
rank() OVER ...
```

##### The `dense_rank()` Function

###### RQ.SRS-019.ClickHouse.WindowFunctions.DenseRank
version: 1.0

[ClickHouse] SHALL support `dense_rank` function over a window that SHALL
return the rank of the current row within its partition without gaps.

Peers SHALL be considered ties and receive the same rank.
The function SHALL assign consecutive ranks to peer groups and
the result is that groups of size greater than one do not produce noncontiguous rank numbers.

If the function is used without `ORDER BY` to sort partition rows into the desired order
then all rows SHALL be peers.

```
dense_rank() OVER ...
```

##### The `row_number()` Function

###### RQ.SRS-019.ClickHouse.WindowFunctions.RowNumber
version: 1.0

[ClickHouse] SHALL support `row_number` function over a window that SHALL
returns the number of the current row within its partition.

Rows numbers SHALL range from 1 to the number of partition rows.

The `ORDER BY` affects the order in which rows are numbered.
Without `ORDER BY`, row numbering MAY be nondeterministic.

```
row_number() OVER ...
```

#### Aggregate Functions

##### RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions
version: 1.0

[ClickHouse] SHALL support using aggregate functions over windows.

* [count](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/count/)
* [min](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/min/)
* [max](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/max/)
* [sum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/sum/)
* [avg](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/avg/)
* [any](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/)
* [stddevPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stddevpop/)
* [stddevSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stddevsamp/)
* [varPop(x)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/varpop/)
* [varSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/varsamp/)
* [covarPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/covarpop/)
* [covarSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/covarsamp/)
* [anyHeavy](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/anyheavy/)
* [anyLast](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/anylast/)
* [argMin](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmin/)
* [argMax](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/argmax/)
* [avgWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/avgweighted/)
* [corr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/corr/)
* [topK](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/topk/)
* [topKWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/topkweighted/)
* [groupArray](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparray/)
* [groupUniqArray](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupuniqarray/)
* [groupArrayInsertAt](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparrayinsertat/)
* [groupArrayMovingSum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraymovingsum/)
* [groupArrayMovingAvg](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraymovingavg/)
* [groupArraySample](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/grouparraysample/)
* [groupBitAnd](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitand/)
* [groupBitOr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitor/)
* [groupBitXor](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitxor/)
* [groupBitmap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmap/)
* [groupBitmapAnd](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapand/)
* [groupBitmapOr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapor/)
* [groupBitmapXor](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/groupbitmapxor/)
* [sumWithOverflow](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/sumwithoverflow/)
* [deltaSum](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/deltasum/)
* [sumMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/summap/)
* [minMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/minmap/)
* [maxMap](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/maxmap/)
* [initializeAggregation](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/initializeAggregation/)
* [skewPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewpop/)
* [skewSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/skewsamp/)
* [kurtPop](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/kurtpop/)
* [kurtSamp](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/kurtsamp/)
* [uniq](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniq/)
* [uniqExact](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqexact/)
* [uniqCombined](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqcombined/)
* [uniqCombined64](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqcombined64/)
* [uniqHLL12](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/uniqhll12/)
* [quantile](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantile/)
* [quantiles](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiles/)
* [quantileExact](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantileexact/)
* [quantileExactWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantileexactweighted/)
* [quantileTiming](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletiming/)
* [quantileTimingWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletimingweighted/)
* [quantileDeterministic](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiledeterministic/)
* [quantileTDigest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletdigest/)
* [quantileTDigestWeighted](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/quantiletdigestweighted/)
* [simpleLinearRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/simplelinearregression/)
* [stochasticLinearRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlinearregression/)
* [stochasticLogisticRegression](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlogisticregression/)
* [categoricalInformationValue](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/stochasticlogisticregression/)
* [studentTTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/studentttest/)
* [welchTTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/welchttest/)
* [mannWhitneyUTest](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/mannwhitneyutest/)
* [median](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/median/)
* [rankCorr](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/rankCorr/)

##### Combinators

###### RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Combinators
version: 1.0

[ClickHouse] SHALL support aggregate functions with combinator prefixes over windows.

* [-If](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-if)
* [-Array](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-array)
* [-SimpleState](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-simplestate)
* [-State](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-state)
* [-Merge](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#aggregate_functions_combinators-merge)
* [-MergeState](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#aggregate_functions_combinators-mergestate)
* [-ForEach](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-foreach)
* [-Distinct](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-distinct)
* [-OrDefault](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-ordefault)
* [-OrNull](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-ornull)
* [-Resample](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/combinators/#agg-functions-combinator-resample)

##### Parametric

###### RQ.SRS-019.ClickHouse.WindowFunctions.AggregateFunctions.Parametric
version: 1.0

[ClickHouse] SHALL support parametric aggregate functions over windows.

* [histogram](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#histogram)
* [sequenceMatch(pattern)(timestamp, cond1, cond2, ...)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#function-sequencematch)
* [sequenceCount(pattern)(time, cond1, cond2, ...)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#function-sequencecount)
* [windowFunnel](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#windowfunnel)
* [retention](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#retention)
* [uniqUpTo(N)(x)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#uniquptonx)
* [sumMapFiltered(keys_to_keep)(keys, values)](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/parametric-functions/#summapfilteredkeys-to-keepkeys-values)

## References

* [ClickHouse]
* [GitHub Repository]
* [Revision History]
* [Git]

[current row peers]: #current-row-peers
[frame_extent]: #frame-extent
[frame_between]: #frame-between
[frame_start]: #frame-start
[frame_end]: #frame-end
[windows_name]: #window-clause
[window_spec]: #window-specification
[partition_clause]: #partition-clause
[order_clause]: #order-clause
[frame_clause]: #frame-clause
[window functions]: https://clickhouse.tech/docs/en/sql-reference/window-functions/
[ClickHouse]: https://clickhouse.tech
[GitHub Repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/window_functions/requirements/requirements.md
[Revision History]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/window_functions/requirements/requirements.md
[Git]: https://git-scm.com/
[GitHub]: https://github.com
[PostreSQL]: https://www.postgresql.org/docs/9.2/tutorial-window.html
[MySQL]: https://dev.mysql.com/doc/refman/8.0/en/window-functions.html
''')
