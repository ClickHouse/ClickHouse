from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_MissingFrameExtent_Error("1.0")
)
def missing_frame_extent(self):
    """Check that when rows frame has missing frame extent then an error is returned."""
    exitcode, message = syntax_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number ROWS) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_InvalidFrameExtent_Error("1.0")
)
def invalid_frame_extent(self):
    """Check that when rows frame has invalid frame extent then an error is returned."""
    exitcode, message = frame_offset_nonnegative_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number ROWS -1) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_CurrentRow("1.0"))
def start_current_row(self):
    """Check rows current row frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+-------
       1    |  5000  | 5000
       2    |  3900  | 3900
       3    |  4800  | 4800
       4    |  4800  | 4800
       5    |  3500  | 3500
       7    |  4200  | 4200
       8    |  6000  | 6000
       9    |  4500  | 4500
      10    |  5200  | 5200
      11    |  5200  | 5200
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS CURRENT ROW) AS sum FROM empsalary ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedPreceding("1.0")
)
def start_unbounded_preceding(self):
    """Check rows unbounded preceding frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+-------
        1   |  5000  | 5000
        2   |  3900  | 8900
        3   |  4800  | 13700
        4   |  4800  | 18500
        5   |  3500  | 22000
        7   |  4200  | 26200
        8   |  6000  | 32200
        9   |  4500  | 36700
        10  |  5200  | 41900
        11  |  5200  | 47100
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS UNBOUNDED PRECEDING) AS sum FROM empsalary ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprPreceding("1.0")
)
def start_expr_preceding(self):
    """Check rows expr preceding frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       1    |  5000  | 5000
       2    |  3900  | 8900
       3    |  4800  | 8700
       4    |  4800  | 9600
       5    |  3500  | 8300
       7    |  4200  | 7700
       8    |  6000  | 10200
       9    |  4500  | 10500
       10   |  5200  | 9700
       11   |  5200  | 10400
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS 1 PRECEDING) AS sum FROM empsalary ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedFollowing_Error(
        "1.0"
    )
)
def start_unbounded_following_error(self):
    """Check rows unbounded following frame returns an error."""
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT empno, salary, sum(salary) OVER (ROWS UNBOUNDED FOLLOWING) AS sum FROM empsalary ORDER BY empno",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprFollowing_Error("1.0")
)
def start_expr_following_error(self):
    """Check rows expr following frame returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT empno, salary, sum(salary) OVER (ROWS 1 FOLLOWING) AS sum FROM empsalary ORDER BY empno",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_CurrentRow("1.0")
)
def between_current_row_and_current_row(self):
    """Check rows between current row and current row frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
        1   |  5000  | 5000
        2   |  3900  | 3900
        3   |  4800  | 4800
        4   |  4800  | 4800
        5   |  3500  | 3500
        7   |  4200  | 4200
        8   |  6000  | 6000
        9   |  4500  | 4500
        10  |  5200  | 5200
        11  |  5200  | 5200
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprPreceding_Error(
        "1.0"
    )
)
def between_current_row_and_expr_preceding_error(self):
    """Check rows between current row and expr preceding returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_current_row_and_unbounded_preceding_error(self):
    """Check rows between current row and unbounded preceding returns an error."""
    exitcode, message = frame_end_unbounded_preceding_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedFollowing(
        "1.0"
    )
)
def between_current_row_and_unbounded_following(self):
    """Check rows between current row and unbounded following."""
    expected = convert_output(
        """
         sum | unique1 | four
        -----+---------+------
         45	 |    0    |   0
         45	 |    1    |   1
         44	 |    2    |   2
         42	 |    3    |   3
         39	 |    4    |   0
         35	 |    5    |   1
         30	 |    6    |   2
         24	 |    7    |   3
         17	 |    8    |   0
         9   |    9    |   1
        """
    )

    execute_query(
        "SELECT sum(unique1) over (order by unique1 rows between current row and unbounded following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprFollowing(
        "1.0"
    )
)
def between_current_row_and_expr_following(self):
    """Check rows between current row and expr following."""
    expected = convert_output(
        """
     i | b | bool_and | bool_or
    ---+---+----------+---------
     1 | 1 | 1        | 1
     2 | 1 | 0        | 1
     3 | 0 | 0        | 0
     4 | 0 | 0        | 1
     5 | 1 | 1        | 1
    """
    )

    execute_query(
        """
        SELECT i, b, groupBitAnd(b) OVER w AS bool_and, groupBitOr(b) OVER w AS bool_or
          FROM VALUES('i Int8, b UInt8', (1,1), (2,1), (3,0), (4,0), (5,1))
          WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
        """,
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_CurrentRow(
        "1.0"
    )
)
def between_unbounded_preceding_and_current_row(self):
    """Check rows between unbounded preceding and current row."""
    expected = convert_output(
        """
         four | two | sum | last_value
        ------+-----+-----+------------
            0 |   0 |   0 |          0
            0 |   0 |   0 |          0
            0 |   1 |   1 |          1
            0 |   1 |   2 |          1
            0 |   2 |   4 |          2
            1 |   0 |   0 |          0
            1 |   0 |   0 |          0
            1 |   1 |   1 |          1
            1 |   1 |   2 |          1
            1 |   2 |   4 |          2
            2 |   0 |   0 |          0
            2 |   0 |   0 |          0
            2 |   1 |   1 |          1
            2 |   1 |   2 |          1
            2 |   2 |   4 |          2
            3 |   0 |   0 |          0
            3 |   0 |   0 |          0
            3 |   1 |   1 |          1
            3 |   1 |   2 |          1
            3 |   2 |   4 |          2
        """
    )

    execute_query(
        "SELECT four, toInt8(ten/4) as two,"
        "sum(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) rows between unbounded preceding and current row) AS sum,"
        "last_value(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) rows between unbounded preceding and current row) AS last_value "
        "FROM (select distinct ten, four from tenk1)",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_unbounded_preceding_and_unbounded_preceding_error(self):
    """Check rows between unbounded preceding and unbounded preceding returns an error."""
    exitcode, message = frame_end_unbounded_preceding_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprPreceding(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_preceding(self):
    """Check rows between unbounded preceding and expr preceding frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       1    |  5000  | 0
       2    |  3900  | 5000
       3    |  4800  | 8900
       4    |  4800  | 13700
       5    |  3500  | 18500
       7    |  4200  | 22000
       8    |  6000  | 26200
       9    |  4500  | 32200
       10   |  5200  | 36700
       11   |  5200  | 41900
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedFollowing(
        "1.0"
    )
)
def between_unbounded_preceding_and_unbounded_following(self):
    """Check rows between unbounded preceding and unbounded following frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       1    |  5000  | 47100
       2    |  3900  | 47100
       3    |  4800  | 47100
       4    |  4800  | 47100
       5    |  3500  | 47100
       7    |  4200  | 47100
       8    |  6000  | 47100
       9    |  4500  | 47100
       10   |  5200  | 47100
       11   |  5200  | 47100
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprFollowing(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_following(self):
    """Check rows between unbounded preceding and expr following."""
    expected = convert_output(
        """
         sum | unique1 | four
        -----+---------+------
          1  |    0    |  0
          3  |    1    |  1
          6  |    2    |  2
          10 |    3    |  3
          15 |    4    |  0
          21 |    5    |  1
          28 |    6    |  2
          36 |    7    |  3
          45 |    8    |  0
          45 |    9    |  1
        """
    )

    execute_query(
        "SELECT sum(unique1) over (order by unique1 rows between unbounded preceding and 1 following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected,
    )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedFollowing_Error(
        "1.0"
    )
)
@Examples(
    "range",
    [
        ("UNBOUNDED FOLLOWING AND CURRENT ROW",),
        ("UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING",),
        ("UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING",),
        ("UNBOUNDED FOLLOWING AND 1 PRECEDING",),
        ("UNBOUNDED FOLLOWING AND 1 FOLLOWING",),
    ],
)
def between_unbounded_following_error(self, range):
    """Check rows between unbounded following and any end frame returns an error."""
    exitcode, message = frame_start_error()

    self.context.node.query(
        f"SELECT number,sum(number) OVER (ROWS BETWEEN {range}) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_Error("1.0")
)
@Examples(
    "range exitcode message",
    [
        ("1 FOLLOWING AND CURRENT ROW", *window_frame_error()),
        ("1 FOLLOWING AND UNBOUNDED PRECEDING", *frame_end_unbounded_preceding_error()),
        ("1 FOLLOWING AND 1 PRECEDING", *frame_start_error()),
    ],
)
def between_expr_following_error(self, range, exitcode, message):
    """Check cases when rows between expr following returns an error."""
    self.context.node.query(
        f"SELECT number,sum(number) OVER (ROWS BETWEEN {range}) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing_Error(
        "1.0"
    )
)
def between_expr_following_and_expr_following_error(self):
    """Check rows between expr following and expr following returns an error when frame end index is less
    than frame start.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_UnboundedFollowing(
        "1.0"
    )
)
def between_expr_following_and_unbounded_following(self):
    """Check rows between exp following and unbounded following frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
        1   |  5000  | 28600
        2   |  3900  | 25100
        3   |  4800  | 20900
        4   |  4800  | 14900
        5   |  3500  | 10400
        7   |  4200  | 5200
        8   |  6000  | 0
        9   |  4500  | 0
        10  |  5200  | 0
        11  |  5200  | 0
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 4 FOLLOWING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing(
        "1.0"
    )
)
def between_expr_following_and_expr_following(self):
    """Check rows between exp following and expr following frame when end of the frame is greater than
    the start of the frame.
    """
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
        1   |  5000  | 17000
        2   |  3900  | 17300
        3   |  4800  | 18500
        4   |  4800  | 18200
        5   |  3500  | 19900
        7   |  4200  | 20900
        8   |  6000  | 14900
        9   |  4500  | 10400
        10  |  5200  | 5200
        11  |  5200  | 0
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_CurrentRow(
        "1.0"
    )
)
def between_expr_preceding_and_current_row(self):
    """Check rows between exp preceding and current row frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
         8  |   6000 |  6000
        10  |   5200 | 11200
        11  |   5200 | 10400
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_expr_preceding_and_unbounded_preceding_error(self):
    """Check rows between expr preceding and unbounded preceding returns an error."""
    exitcode, message = frame_end_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedFollowing(
        "1.0"
    )
)
def between_expr_preceding_and_unbounded_following(self):
    """Check rows between exp preceding and unbounded following frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
        8   |   6000 | 16400
       10   |   5200 | 16400
       11   |   5200 | 10400
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding_Error(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding_error(self):
    """Check rows between expr preceding and expr preceding returns an error when frame end is
    before frame start.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding(self):
    """Check rows between expr preceding and expr preceding frame when frame end is after or at frame start."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       1    | 5000   | 5000
       2    | 3900   | 8900
       3    | 4800   | 8700
       4    | 4800   | 9600
       5    | 3500   | 8300
       7    | 4200   | 7700
       8    | 6000   | 10200
       9    | 4500   | 10500
       10   | 5200   | 9700
       11   | 5200   | 10400
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing(
        "1.0"
    )
)
def between_expr_preceding_and_expr_following(self):
    """Check rows between expr preceding and expr following frame."""
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       8    |   6000 | 11200
      10    |   5200 | 16400
      11    |   5200 | 10400
    """
    )

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ORDER BY empno ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing(
        "1.0"
    )
)
def between_expr_following_and_expr_following_ref(self):
    """Check reference result for rows between expr following and expr following range."""
    expected = convert_output(
        """
     sum | unique1 | four
    -----+---------+------
      6  |    0    |  0
      9  |    1    |  1
      12 |    2    |  2
      15 |    3    |  3
      18 |    4    |  0
      21 |    5    |  1
      24 |    6    |  2
      17 |    7    |  3
      9  |    8    |  0
      0  |    9    |  1
    """
    )

    execute_query(
        "SELECT sum(unique1) over (order by unique1 rows between 1 following and 3 following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding_ref(self):
    """Check reference result for rows between expr preceding and expr preceding frame."""
    expected = convert_output(
        """
     sum | unique1 | four
    -----+---------+------
      0  |   0     |  0
      0  |   1     |  1
      1  |   2     |  2
      3  |   3     |  3
      5  |   4     |  0
      7  |   5     |  1
      9  |   6     |  2
      11 |   7     |  3
      13 |   8     |  0
      15 |   9     |  1
    """
    )

    execute_query(
        "SELECT sum(unique1) over (order by unique1 rows between 2 preceding and 1 preceding) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing(
        "1.0"
    )
)
def between_expr_preceding_and_expr_following_ref(self):
    """Check reference result for rows between expr preceding and expr following frame."""
    expected = convert_output(
        """
     sum | unique1 | four
    -----+---------+------
      3  |   0     |  0
      6  |   1     |  1
      10 |   2     |  2
      15 |   3     |  3
      20 |   4     |  0
      25 |   5     |  1
      30 |   6     |  2
      35 |   7     |  3
      30 |   8     |  0
      24 |   9     |  1
    """
    )

    execute_query(
        "SELECT sum(unique1) over (order by unique1 rows between 2 preceding and 2 following) AS sum, "
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected,
    )


@TestFeature
@Name("rows frame")
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame("1.0"))
def feature(self):
    """Check defining rows frame."""
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
