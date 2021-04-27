from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_MissingFrameExtent_Error("1.0")
)
def missing_frame_extent(self):
    """Check that when rows frame has missing frame extent then an error is returned.
    """
    exitcode, message = syntax_error()

    self.context.node.query("SELECT number,sum(number) OVER (ORDER BY number ROWS) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_InvalidFrameExtent_Error("1.0")
)
def invalid_frame_extent(self):
    """Check that when rows frame has invalid frame extent then an error is returned.
    """
    exitcode, message = frame_offset_nonnegative_error()

    self.context.node.query("SELECT number,sum(number) OVER (ORDER BY number ROWS -1) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_CurrentRow("1.0")
)
def start_current_row(self):
    """Check rows current row frame.
    """
    expected = convert_output("""
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
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS CURRENT ROW) AS sum FROM empsalary ORDER BY empno",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedPreceding("1.0")
)
def start_unbounded_preceding(self):
    """Check rows unbounded preceding frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------
          1 |   5000 |  5000
          2 |   3900 | 14900
          3 |   4800 | 24900
          4 |   4800 | 29700
          5 |   3500 | 38400
          7 |   4200 | 42600
          8 |   6000 | 11000
          9 |   4500 | 47100
         10 |   5200 | 20100
         11 |   5200 | 34900
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS UNBOUNDED PRECEDING) AS sum FROM empsalary ORDER BY empno",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprPreceding("1.0")
)
def start_expr_preceding(self):
    """Check rows expr preceding frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+--------  
          1 |   5000 |  5000
          2 |   3900 |  9900
          3 |   4800 | 10000
          4 |   4800 |  9600
          5 |   3500 |  8700
          7 |   4200 |  7700
          8 |   6000 | 11000
          9 |   4500 |  8700
         10 |   5200 |  9100
         11 |   5200 | 10000
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS 1 PRECEDING) AS sum FROM empsalary ORDER BY empno",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_UnboundedFollowing_Error("1.0")
)
def start_unbounded_following_error(self):
    """Check rows unbounded following frame returns an error.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT empno, salary, sum(salary) OVER (ROWS UNBOUNDED FOLLOWING) AS sum FROM empsalary ORDER BY empno",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Start_ExprFollowing_Error("1.0")
)
def start_expr_following_error(self):
    """Check rows expr following frame returns an error.
    """
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT empno, salary, sum(salary) OVER (ROWS 1 FOLLOWING) AS sum FROM empsalary ORDER BY empno",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_CurrentRow("1.0")
)
def between_current_row_and_current_row(self):
    """Check rows between current row and current row frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
          1 |   5000 | 5000 
          8 |   6000 | 6000 
          2 |   3900 | 3900 
         10 |   5200 | 5200 
          3 |   4800 | 4800 
          4 |   4800 | 4800 
         11 |   5200 | 5200 
          5 |   3500 | 3500 
          7 |   4200 | 4200 
          9 |   4500 | 4500 
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprPreceding_Error("1.0")
)
def between_current_row_and_expr_preceding_error(self):
    """Check rows between current row and expr preceding returns an error.
    """
    exitcode, message = window_frame_error()

    self.context.node.query("SELECT number,sum(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedPreceding_Error("1.0")
)
def between_current_row_and_unbounded_preceding_error(self):
    """Check rows between current row and unbounded preceding returns an error.
    """
    exitcode, message = frame_end_unbounded_preceding_error()

    self.context.node.query("SELECT number,sum(number) OVER (ORDER BY number ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_UnboundedFollowing("1.0")
)
def between_current_row_and_unbounded_following(self):
    """Check rows between current row and unbounded following.
    """
    expected = convert_output("""
         sum | unique1 | four 
        -----+---------+------
          45 |       4 |    0
          41 |       2 |    2
          39 |       1 |    1
          38 |       6 |    2
          32 |       9 |    1
          23 |       8 |    0
          15 |       5 |    1
          10 |       3 |    3
           7 |       7 |    3
           0 |       0 |    0
        """)

    execute_query(
        "SELECT sum(unique1) over (rows between current row and unbounded following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_CurrentRow_ExprFollowing("1.0")
)
def between_current_row_and_expr_following(self):
    """Check rows between current row and expr following.
    """
    expected = convert_output("""
     i | b | bool_and | bool_or 
    ---+---+----------+---------
     1 | 1 | 1        | 1
     2 | 1 | 0        | 1
     3 | 0 | 0        | 0
     4 | 0 | 0        | 1
     5 | 1 | 1        | 1
    """)

    execute_query("""
        SELECT i, b, groupBitAnd(b) OVER w AS bool_and, groupBitOr(b) OVER w AS bool_or
          FROM VALUES('i Int8, b UInt8', (1,1), (2,1), (3,0), (4,0), (5,1))
          WINDOW w AS (ORDER BY i ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)
        """,
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_CurrentRow("1.0")
)
def between_unbounded_preceding_and_current_row(self):
    """Check rows between unbounded preceding and current row.
    """
    expected = convert_output("""
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
        """)

    execute_query(
        "SELECT four, toInt8(ten/4) as two,"
        "sum(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) rows between unbounded preceding and current row) AS sum,"
        "last_value(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) rows between unbounded preceding and current row) AS last_value "
        "FROM (select distinct ten, four from tenk1)",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedPreceding_Error("1.0")
)
def between_unbounded_preceding_and_unbounded_preceding_error(self):
    """Check rows between unbounded preceding and unbounded preceding returns an error.
    """
    exitcode, message = frame_end_unbounded_preceding_error()

    self.context.node.query("SELECT number,sum(number) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprPreceding("1.0")
)
def between_unbounded_preceding_and_expr_preceding(self):
    """Check rows between unbounded preceding and expr preceding frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
         1  |  5000  |      0 
         8  |  6000  |   5000 
         2  |  3900  |  11000 
        10  |  5200  |  14900 
         3  |  4800  |  20100 
         4  |  4800  |  24900 
        11  |  5200  |  29700 
         5  |  3500  |  34900 
         7  |  4200  |  38400 
         9  |  4500  |  42600 
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_UnboundedFollowing("1.0")
)
def between_unbounded_preceding_and_unbounded_following(self):
    """Check rows between unbounded preceding and unbounded following frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
        1   |   5000 | 47100 
        8   |   6000 | 47100 
        2   |   3900 | 47100 
       10   |   5200 | 47100 
        3   |   4800 | 47100 
        4   |   4800 | 47100 
       11   |   5200 | 47100 
        5   |   3500 | 47100 
        7   |   4200 | 47100 
        9   |   4500 | 47100 
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedPreceding_ExprFollowing("1.0")
)
def between_unbounded_preceding_and_expr_following(self):
    """Check rows between unbounded preceding and expr following.
    """
    expected = convert_output("""
         sum | unique1 | four 
        -----+---------+------
           6 |       4 |    0
           7 |       2 |    2
          13 |       1 |    1
          22 |       6 |    2
          30 |       9 |    1
          35 |       8 |    0
          38 |       5 |    1
          45 |       3 |    3
          45 |       7 |    3
          45 |       0 |    0
        """)

    execute_query(
        "SELECT sum(unique1) over (rows between unbounded preceding and 1 following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_UnboundedFollowing_Error("1.0")
)
@Examples("range", [
    ("UNBOUNDED FOLLOWING AND CURRENT ROW",),
    ("UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING",),
    ("UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING",),
    ("UNBOUNDED FOLLOWING AND 1 PRECEDING",),
    ("UNBOUNDED FOLLOWING AND 1 FOLLOWING",),
])
def between_unbounded_following_error(self, range):
    """Check rows between unbounded following and any end frame returns an error.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(f"SELECT number,sum(number) OVER (ROWS BETWEEN {range}) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_Error("1.0")
)
@Examples("range exitcode message", [
    ("1 FOLLOWING AND CURRENT ROW", *window_frame_error()),
    ("1 FOLLOWING AND UNBOUNDED PRECEDING", *frame_end_unbounded_preceding_error()),
    ("1 FOLLOWING AND 1 PRECEDING", *frame_start_error())
])
def between_expr_following_error(self, range, exitcode, message):
    """Check cases when rows between expr following returns an error.
    """
    self.context.node.query(f"SELECT number,sum(number) OVER (ROWS BETWEEN {range}) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing_Error("1.0")
)
def between_expr_following_and_expr_following_error(self):
    """Check rows between expr following and expr following returns an error when frame end index is less
    than frame start.
    """
    exitcode, message = frame_start_error()

    self.context.node.query("SELECT number,sum(number) OVER (ROWS BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_UnboundedFollowing("1.0")
)
def between_exp_following_and_unbounded_following(self):
    """Check rows between exp following and unbounded following frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
          1 |   5000 | 27000
          8 |   6000 | 22200
          2 |   3900 | 17400
         10 |   5200 | 12200
          3 |   4800 |  8700
          4 |   4800 |  4500
         11 |   5200 |     0
          5 |   3500 |     0
          7 |   4200 |     0
          9 |   4500 |     0
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 4 FOLLOWING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing("1.0")
)
def between_exp_following_and_expr_following(self):
    """Check rows between exp following and expr following frame when end of the frame is greater than
    the start of the frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
          1 |   5000 | 19900
          8 |   6000 | 18700
          2 |   3900 | 20000
         10 |   5200 | 18300
          3 |   4800 | 17700
          4 |   4800 | 17400
         11 |   5200 | 12200
          5 |   3500 |  8700
          7 |   4200 |  4500
          9 |   4500 |     0
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 1 FOLLOWING AND 4 FOLLOWING) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_CurrentRow("1.0")
)
def between_exp_preceding_and_current_row(self):
    """Check rows between exp preceding and current row frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
         8  |   6000 |  6000 
        10  |   5200 | 11200 
        11  |   5200 | 10400 
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedPreceding_Error("1.0")
)
def between_expr_preceding_and_unbounded_preceding_error(self):
    """Check rows between expr preceding and unbounded preceding returns an error.
    """
    exitcode, message = frame_end_error()

    self.context.node.query("SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_UnboundedFollowing("1.0")
)
def between_exp_preceding_and_unbounded_following(self):
    """Check rows between exp preceding and unbounded following frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
        8   |   6000 | 16400
       10   |   5200 | 16400
       11   |   5200 | 10400
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding_Error("1.0")
)
def between_expr_preceding_and_expr_preceding_error(self):
    """Check rows between expr preceding and expr preceding returns an error when frame end is
    before frame start.
    """
    exitcode, message = frame_start_error()

    self.context.node.query("SELECT number,sum(number) OVER (ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM numbers(1,3)",
        exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding("1.0")
)
def between_expr_preceding_and_expr_preceding_following(self):
    """Check rows between expr preceding and expr preceding frame when frame end is after or at frame start.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
         1  |   5000 |  5000
         8  |   6000 | 11000
         2  |   3900 |  9900
        10  |   5200 |  9100
         3  |   4800 | 10000
         4  |   4800 |  9600
        11  |   5200 | 10000
         5  |   3500 |  8700
         7  |   4200 |  7700
         9  |   4500 |  8700
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 1 PRECEDING AND 0 PRECEDING) AS sum FROM empsalary",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing("1.0")
)
def between_expr_preceding_and_expr_following(self):
    """Check rows between expr preceding and expr following frame.
    """
    expected = convert_output("""
      empno | salary | sum
    --------+--------+-------- 
       8    |   6000 | 11200 
      10    |   5200 | 16400 
      11    |   5200 | 10400     
    """)

    execute_query(
        "SELECT empno, salary, sum(salary) OVER (ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum FROM empsalary WHERE salary > 5000",
        expected=expected
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprFollowing_ExprFollowing("1.0")
)
def between_expr_following_and_expr_following_ref(self):
    """Check reference result for rows between expr following and expr following range.
    """
    expected = convert_output("""
     sum | unique1 | four 
    -----+---------+------
       9 |       4 |    0
      16 |       2 |    2
      23 |       1 |    1
      22 |       6 |    2
      16 |       9 |    1
      15 |       8 |    0
      10 |       5 |    1
       7 |       3 |    3
       0 |       7 |    3
       0 |       0 |    0
    """)

    execute_query(
        "SELECT sum(unique1) over (rows between 1 following and 3 following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprPreceding("1.0")
)
def between_expr_preceding_and_expr_preceding_ref(self):
    """Check reference result for rows between expr preceding and expr preceding frame.
    """
    expected = convert_output("""
     sum | unique1 | four 
    -----+---------+------
       0 |       4 |    0
       4 |       2 |    2
       6 |       1 |    1
       3 |       6 |    2
       7 |       9 |    1
      15 |       8 |    0
      17 |       5 |    1
      13 |       3 |    3
       8 |       7 |    3
      10 |       0 |    0
    """)

    execute_query(
        "SELECT sum(unique1) over (rows between 2 preceding and 1 preceding) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame_Between_ExprPreceding_ExprFollowing("1.0")
)
def between_expr_preceding_and_expr_following_ref(self):
    """Check reference result for rows between expr preceding and expr following frame.
    """
    expected = convert_output("""
     sum | unique1 | four 
    -----+---------+------
       7 |       4 |    0
      13 |       2 |    2
      22 |       1 |    1
      26 |       6 |    2
      29 |       9 |    1
      31 |       8 |    0
      32 |       5 |    1
      23 |       3 |    3
      15 |       7 |    3
      10 |       0 |    0
    """)

    execute_query(
        "SELECT sum(unique1) over (rows between 2 preceding and 2 following) AS sum, "
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestFeature
@Name("rows frame")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RowsFrame("1.0")
)
def feature(self):
    """Check defining rows frame.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
