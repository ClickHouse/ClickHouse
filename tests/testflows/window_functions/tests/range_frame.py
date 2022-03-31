from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_MissingFrameExtent_Error("1.0")
)
def missing_frame_extent(self):
    """Check that when range frame has missing frame extent then an error is returned."""
    exitcode, message = syntax_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_InvalidFrameExtent_Error("1.0")
)
def invalid_frame_extent(self):
    """Check that when range frame has invalid frame extent then an error is returned."""
    exitcode, message = syntax_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE '1') FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_CurrentRow_Peers("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithoutOrderBy(
        "1.0"
    ),
)
def start_current_row_without_order_by(self):
    """Check range current row frame without order by and
    that the peers of the current row are rows that have values in the same order bucket.
    In this case without order by clause all rows are the peers of the current row.
    """
    expected = convert_output(
        """
      empno | salary | sum
    --------+--------+--------
       1    | 5000   | 47100
       2    | 3900   | 47100
       3    | 4800   | 47100
       4    | 4800   | 47100
       5    | 3500   | 47100
       7    | 4200   | 47100
       8    | 6000   | 47100
       9    | 4500   | 47100
       10   | 5200   | 47100
       11   | 5200   | 47100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, salary, sum(salary) OVER (RANGE CURRENT ROW) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_CurrentRow_Peers("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_CurrentRow_WithOrderBy(
        "1.0"
    ),
)
def start_current_row_with_order_by(self):
    """Check range current row frame with order by and that the peers of the current row
    are rows that have values in the same order bucket.
    """
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
       1    | sales     | 5000   | 14600
       2    | personnel | 3900   | 7400
       3    | sales     | 4800   | 14600
       4    | sales     | 4800   | 14600
       5    | personnel | 3500   | 7400
       7    | develop   | 4200   | 25100
       8    | develop   | 6000   | 25100
       9    | develop   | 4500   | 25100
       10   | develop   | 5200   | 25100
       11   | develop   | 5200   | 25100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY depname RANGE CURRENT ROW) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedFollowing_Error(
        "1.0"
    )
)
def start_unbounded_following_error(self):
    """Check range current row frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE UNBOUNDED FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE UNBOUNDED FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithoutOrderBy(
        "1.0"
    )
)
def start_unbounded_preceding_without_order_by(self):
    """Check range unbounded preceding frame without order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
       7    | develop   |  4200  | 25100
       8    | develop   |  6000  | 25100
       9    | develop   |  4500  | 25100
       10   | develop   |  5200  | 25100
       11   | develop   |  5200  | 25100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (RANGE UNBOUNDED PRECEDING) AS sum FROM empsalary WHERE depname = 'develop') ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_UnboundedPreceding_WithOrderBy(
        "1.0"
    )
)
def start_unbounded_preceding_with_order_by(self):
    """Check range unbounded preceding frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
       1    | sales     | 5000   | 47100
       2    | personnel | 3900   | 32500
       3    | sales     | 4800   | 47100
       4    | sales     | 4800   | 47100
       5    | personnel | 3500   | 32500
       7    | develop   | 4200   | 25100
       8    | develop   | 6000   | 25100
       9    | develop   | 4500   | 25100
       10   | develop   | 5200   | 25100
       11   | develop   | 5200   | 25100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY depname RANGE UNBOUNDED PRECEDING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def start_expr_following_without_order_by_error(self):
    """Check range expr following frame without order by returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT empno, depname, salary, sum(salary) OVER (RANGE 1 FOLLOWING) AS sum FROM empsalary",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprFollowing_WithOrderBy_Error(
        "1.0"
    )
)
def start_expr_following_with_order_by_error(self):
    """Check range expr following frame with order by returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE 1 FOLLOWING) AS sum FROM empsalary",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithOrderBy(
        "1.0"
    )
)
def start_expr_preceding_with_order_by(self):
    """Check range expr preceding frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
        1   | sales     | 5000   | 5000
        2   | personnel | 3900   | 3900
        3   | sales     | 4800   | 9600
        4   | sales     | 4800   | 9600
        5   | personnel | 3500   | 3500
        7   | develop   | 4200   | 4200
        8   | develop   | 6000   | 6000
        9   | develop   | 4500   | 4500
        10  | develop   | 5200   | 10400
        11  | develop   | 5200   | 10400
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE 1 PRECEDING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_OrderByNonNumericalColumn_Error(
        "1.0"
    )
)
def start_expr_preceding_order_by_non_numerical_column_error(self):
    """Check range expr preceding frame with order by non-numerical column returns an error."""
    exitcode, message = frame_range_offset_error()

    self.context.node.query(
        "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY depname RANGE 1 PRECEDING) AS sum FROM empsalary",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Start_ExprPreceding_WithoutOrderBy_Error(
        "1.0"
    )
)
def start_expr_preceding_without_order_by_error(self):
    """Check range expr preceding frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT empno, depname, salary, sum(salary) OVER (RANGE 1 PRECEDING) AS sum FROM empsalary",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_CurrentRow(
        "1.0"
    )
)
def between_current_row_and_current_row(self):
    """Check range between current row and current row frame with or without order by."""
    with Example("without order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
           7    | develop   | 4200   | 25100
           8    | develop   | 6000   | 25100
           9    | develop   | 4500   | 25100
           10   | develop   | 5200   | 25100
           11   | develop   | 5200   | 25100
        """
        )

        execute_query(
            "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS sum FROM empsalary WHERE depname = 'develop') ORDER BY empno",
            expected=expected,
        )

    with Example("with order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+------
           7	| develop   |  4200  | 4200
           8	| develop   |  6000  | 6000
           9	| develop   |  4500  | 4500
           10	| develop   |  5200  | 5200
           11	| develop   |  5200  | 5200
        """
        )

        execute_query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY empno RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS sum FROM empsalary WHERE depname = 'develop'",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_current_row_and_unbounded_preceding_error(self):
    """Check range between current row and unbounded preceding frame with or without order by returns an error."""
    exitcode, message = frame_end_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_UnboundedFollowing(
        "1.0"
    )
)
def between_current_row_and_unbounded_following(self):
    """Check range between current row and unbounded following frame with or without order by."""
    with Example("without order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
            7   |  develop  | 4200   | 25100
            8   |  develop  | 6000   | 25100
            9   |  develop  | 4500   | 25100
            10  |  develop  | 5200   | 25100
            11  |  develop  | 5200   | 25100
        """
        )

        execute_query(
            "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS sum FROM empsalary WHERE depname = 'develop') ORDER BY empno",
            expected=expected,
        )

    with Example("with order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
           7    | develop   | 4200   | 25100
           8    | develop   | 6000   | 20900
           9    | develop   | 4500   | 14900
           10   | develop   | 5200   | 10400
           11   | develop   | 5200   | 5200
        """
        )

        execute_query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY empno RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS sum FROM empsalary WHERE depname = 'develop'",
            expected=expected,
        )

    with Example("with order by from tenk1"):
        expected = convert_output(
            """
         sum | unique1 | four
        -----+---------+------
          45 |    0    |  0
          33 |    1    |  1
          18 |    2    |  2
          10 |    3    |  3
          45 |    4    |  0
          33 |    5    |  1
          18 |    6    |  2
          10 |    7    |  3
          45 |    8    |  0
          33 |    9    |  1
        """
        )

        execute_query(
            "SELECT * FROM (SELECT sum(unique1) over (order by four range between current row and unbounded following) AS sum,"
            "unique1, four "
            "FROM tenk1 WHERE unique1 < 10) ORDER BY unique1",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_current_row_and_expr_following_without_order_by_error(self):
    """Check range between current row and expr following frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM numbers(1,3)",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprFollowing_WithOrderBy(
        "1.0"
    )
)
def between_current_row_and_expr_following_with_order_by(self):
    """Check range between current row and expr following frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
       1    | sales     | 5000   | 8900
       2    | personnel | 3900   | 8700
       3    | sales     | 4800   | 9600
       4    | sales     | 4800   | 8300
       5    | personnel | 3500   | 3500
       7    | develop   | 4200   | 10200
       8    | develop   | 6000   | 10500
       9    | develop   | 4500   | 9700
       10   | develop   | 5200   | 10400
       11   | develop   | 5200   | 5200
    """
    )

    execute_query(
        "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY empno RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_CurrentRow_ExprPreceding_Error(
        "1.0"
    )
)
def between_current_row_and_expr_preceding_error(self):
    """Check range between current row and expr preceding frame with or without order by returns an error."""
    exitcode, message = window_frame_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN CURRENT ROW AND 1 PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_CurrentRow(
        "1.0"
    )
)
def between_unbounded_preceding_and_current_row(self):
    """Check range between unbounded preceding and current row frame with and without order by."""
    with Example("with order by"):
        expected = convert_output(
            """
             four | ten | sum | last_value
            ------+-----+-----+------------
                0 |   0 |   0 |          0
                0 |   2 |   2 |          2
                0 |   4 |   6 |          4
                0 |   6 |  12 |          6
                0 |   8 |  20 |          8
                1 |   1 |   1 |          1
                1 |   3 |   4 |          3
                1 |   5 |   9 |          5
                1 |   7 |  16 |          7
                1 |   9 |  25 |          9
                2 |   0 |   0 |          0
                2 |   2 |   2 |          2
                2 |   4 |   6 |          4
                2 |   6 |  12 |          6
                2 |   8 |  20 |          8
                3 |   1 |   1 |          1
                3 |   3 |   4 |          3
                3 |   5 |   9 |          5
                3 |   7 |  16 |          7
                3 |   9 |  25 |          9
            """
        )

        execute_query(
            "SELECT four, ten,"
            "sum(ten) over (partition by four order by ten range between unbounded preceding and current row) AS sum,"
            "last_value(ten) over (partition by four order by ten range between unbounded preceding and current row) AS last_value "
            "FROM (select distinct ten, four from tenk1)",
            expected=expected,
        )

    with Example("without order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
           7    |  develop  | 4200   | 25100
           8    |  develop  | 6000   | 25100
           9    |  develop  | 4500   | 25100
           10   |  develop  | 5200   | 25100
           11   |  develop  | 5200   | 25100
        """
        )

        execute_query(
            "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum FROM empsalary WHERE depname = 'develop') ORDER BY empno",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_unbounded_preceding_and_unbounded_preceding_error(self):
    """Check range between unbounded preceding and unbounded preceding frame with or without order by returns an error."""
    exitcode, message = frame_end_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_UnboundedFollowing(
        "1.0"
    )
)
def between_unbounded_preceding_and_unbounded_following(self):
    """Check range between unbounded preceding and unbounded following range with and without order by."""
    with Example("with order by"):
        expected = convert_output(
            """
         four | ten | sum | last_value
        ------+-----+-----+------------
            0 |   0 |  20 |          8
            0 |   2 |  20 |          8
            0 |   4 |  20 |          8
            0 |   6 |  20 |          8
            0 |   8 |  20 |          8
            1 |   1 |  25 |          9
            1 |   3 |  25 |          9
            1 |   5 |  25 |          9
            1 |   7 |  25 |          9
            1 |   9 |  25 |          9
            2 |   0 |  20 |          8
            2 |   2 |  20 |          8
            2 |   4 |  20 |          8
            2 |   6 |  20 |          8
            2 |   8 |  20 |          8
            3 |   1 |  25 |          9
            3 |   3 |  25 |          9
            3 |   5 |  25 |          9
            3 |   7 |  25 |          9
            3 |   9 |  25 |          9
        """
        )

        execute_query(
            "SELECT four, ten, "
            "sum(ten) over (partition by four order by ten range between unbounded preceding and unbounded following) AS sum, "
            "last_value(ten) over (partition by four order by ten range between unbounded preceding and unbounded following) AS last_value "
            "FROM (select distinct ten, four from tenk1)",
            expected=expected,
        )

    with Example("without order by"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
           1    | sales     |  5000  | 47100
           2    | personnel |  3900  | 47100
           3    | sales     |  4800  | 47100
           4    | sales     |  4800  | 47100
           5    | personnel |  3500  | 47100
           7    | develop   |  4200  | 47100
           8    | develop   |  6000  | 47100
           9    | develop   |  4500  | 47100
           10   | develop   |  5200  | 47100
           11   | develop   |  5200  | 47100
        """
        )

        execute_query(
            "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary) ORDER BY empno",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_following_without_order_by_error(self):
    """Check range between unbounded preceding and expr following frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_preceding_without_order_by_error(self):
    """Check range between unbounded preceding and expr preceding frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprFollowing_WithOrderBy(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_following_with_order_by(self):
    """Check range between unbounded preceding and expr following frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
      1     | sales     | 5000   | 41100
      2     | personnel | 3900   | 11600
      3     | sales     | 4800   | 41100
      4     | sales     | 4800   | 41100
      5     | personnel | 3500   | 7400
      7     | develop   | 4200   | 16100
      8     | develop   | 6000   | 47100
      9     | develop   | 4500   | 30700
      10    | develop   | 5200   | 41100
      11    | develop   | 5200   | 41100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND 500 FOLLOWING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedPreceding_ExprPreceding_WithOrderBy(
        "1.0"
    )
)
def between_unbounded_preceding_and_expr_preceding_with_order_by(self):
    """Check range between unbounded preceding and expr preceding frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
      1     | sales     | 5000   | 16100
      2     | personnel | 3900   | 0
      3     | sales     | 4800   | 11600
      4     | sales     | 4800   | 11600
      5     | personnel | 3500   | 0
      7     | develop   | 4200   | 3500
      8     | develop   | 6000   | 41100
      9     | develop   | 4500   | 7400
      10    | develop   | 5200   | 16100
      11    | develop   | 5200   | 16100
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED PRECEDING AND 500 PRECEDING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_CurrentRow_Error(
        "1.0"
    )
)
def between_unbounded_following_and_current_row_error(self):
    """Check range between unbounded following and current row frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedFollowing_Error(
        "1.0"
    )
)
def between_unbounded_following_and_unbounded_following_error(self):
    """Check range between unbounded following and unbounded following frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_unbounded_following_and_unbounded_preceding_error(self):
    """Check range between unbounded following and unbounded preceding frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprPreceding_Error(
        "1.0"
    )
)
def between_unbounded_following_and_expr_preceding_error(self):
    """Check range between unbounded following and expr preceding frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED FOLLOWING AND 1 PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED FOLLOWING AND 1 PRECEDING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_UnboundedFollowing_ExprFollowing_Error(
        "1.0"
    )
)
def between_unbounded_following_and_expr_following_error(self):
    """Check range between unbounded following and expr following frame with or without order by returns an error."""
    exitcode, message = frame_start_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (RANGE BETWEEN UNBOUNDED FOLLOWING AND 1 FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN UNBOUNDED FOLLOWING AND 1 FOLLOWING) AS sum FROM empsalary",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_preceding_and_current_row_without_order_by_error(self):
    """Check range between expr preceding and current row frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_preceding_and_unbounded_following_without_order_by_error(self):
    """Check range between expr preceding and unbounded following frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_preceding_and_expr_following_without_order_by_error(self):
    """Check range between expr preceding and expr following frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding_without_order_by_error(self):
    """Check range between expr preceding and expr preceding frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_expr_preceding_and_unbounded_preceding_error(self):
    """Check range between expr preceding and unbounded preceding frame with or without order by returns an error."""
    exitcode, message = frame_end_unbounded_preceding_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (ORDER BY salary RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_CurrentRow_WithOrderBy(
        "1.0"
    )
)
def between_expr_preceding_and_current_row_with_order_by(self):
    """Check range between expr preceding and current row frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
        1   | sales     |  5000  | 5000
        2   | personnel |  3900  | 8900
        3   | sales     |  4800  | 13700
        4   | sales     |  4800  | 18500
        5   | personnel |  3500  | 22000
        7   | develop   |  4200  | 26200
        8   | develop   |  6000  | 32200
        9   | develop   |  4500  | 36700
        10  | develop   |  5200  | 41900
        11  | develop   |  5200  | 47100
    """
    )

    execute_query(
        "SELECT empno, depname, salary, sum(salary) OVER (ORDER BY empno RANGE BETWEEN 500 PRECEDING AND CURRENT ROW) AS sum FROM empsalary",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_UnboundedFollowing_WithOrderBy(
        "1.0"
    )
)
def between_expr_preceding_and_unbounded_following_with_order_by(self):
    """Check range between expr preceding and unbounded following frame with order by."""
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
       1    | sales     | 5000   | 35500
       2    | personnel | 3900   | 47100
       3    | sales     | 4800   | 35500
       4    | sales     | 4800   | 35500
       5    | personnel | 3500   | 47100
       7    | develop   | 4200   | 43600
       8    | develop   | 6000   | 6000
       9    | develop   | 4500   | 39700
       10   | develop   | 5200   | 31000
       11   | develop   | 5200   | 31000
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN 500 PRECEDING AND UNBOUNDED FOLLOWING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprFollowing_WithOrderBy(
        "1.0"
    )
)
def between_expr_preceding_and_expr_following_with_order_by(self):
    """Check range between expr preceding and expr following frame with order by."""
    with Example("empsalary"):
        expected = convert_output(
            """
          empno | depname   | salary | sum
        --------+-----------+--------+---------
           1    | sales     | 5000   | 29500
           2    | personnel | 3900   | 11600
           3    | sales     | 4800   | 29500
           4    | sales     | 4800   | 29500
           5    | personnel | 3500   | 7400
           7    | develop   | 4200   | 12600
           8    | develop   | 6000   | 6000
           9    | develop   | 4500   | 23300
           10   | develop   | 5200   | 25000
           11   | develop   | 5200   | 25000
        """
        )

        execute_query(
            "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN 500 PRECEDING AND 500 FOLLOWING) AS sum FROM empsalary) ORDER BY empno",
            expected=expected,
        )

    with Example("tenk1"):
        expected = convert_output(
            """
             sum | unique1 | four
            -----+---------+------
               4 |       0 |    0
              12 |       4 |    0
              12 |       8 |    0
               6 |       1 |    1
              15 |       5 |    1
              14 |       9 |    1
               8 |       2 |    2
               8 |       6 |    2
              10 |       3 |    3
              10 |       7 |    3
            """
        )

        execute_query(
            "SELECT sum(unique1) over (partition by four order by unique1 range between 5 preceding and 6 following) AS sum, "
            "unique1, four "
            "FROM tenk1 WHERE unique1 < 10",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding_with_order_by(self):
    """Check range between expr preceding and expr preceding range with order by."""
    with Example("order by asc"):
        expected = convert_output(
            """
         sum | unique1 | four
        -----+---------+------
          0  |   0     |  0
          0  |   4     |  0
          0  |   8     |  0
          12 |   1     |  1
          12 |   5     |  1
          12 |   9     |  1
          27 |   2     |  2
          27 |   6     |  2
          23 |   3     |  3
          23 |   7     |  3
        """
        )

        execute_query(
            "SELECT * FROM (SELECT sum(unique1) over (order by four range between 2 preceding and 1 preceding) AS sum, "
            "unique1, four "
            "FROM tenk1 WHERE unique1 < 10) ORDER BY four, unique1",
            expected=expected,
        )

    with Example("order by desc"):
        expected = convert_output(
            """
         sum | unique1 | four
        -----+---------+------
          23 |   0     |  0
          23 |   4     |  0
          23 |   8     |  0
          18 |   1     |  1
          18 |   5     |  1
          18 |   9     |  1
          10 |   2     |  2
          10 |   6     |  2
          0  |   3     |  3
          0  |   7     |  3
        """
        )

        execute_query(
            "SELECT * FROM (SELECT sum(unique1) over (order by four desc range between 2 preceding and 1 preceding) AS sum, "
            "unique1, four "
            "FROM tenk1 WHERE unique1 < 10) ORDER BY four, unique1",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprPreceding_ExprPreceding_WithOrderBy_Error(
        "1.0"
    )
)
def between_expr_preceding_and_expr_preceding_with_order_by_error(self):
    """Check range between expr preceding and expr preceding range with order by returns error
    when end frame is before of start frame.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 PRECEDING AND 2 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_current_row_without_order_by_error(self):
    """Check range between expr following and current row frame without order by returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_unbounded_following_without_order_by_error(self):
    """Check range between expr following and unbounded following frame without order by returns an error."""
    exitcode, message = frame_requires_order_by_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_expr_following_without_order_by_error(self):
    """Check range between expr following and expr following frame without order by returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND 1 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithoutOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_expr_preceding_without_order_by_error(self):
    """Check range between expr following and expr preceding frame without order by returns an error."""
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedPreceding_Error(
        "1.0"
    )
)
def between_expr_following_and_unbounded_preceding_error(self):
    """Check range between expr following and unbounded preceding frame with or without order by returns an error."""
    exitcode, message = frame_end_unbounded_preceding_error()

    with Example("without order by"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )

    with Example("with order by"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (ORDER BY salary RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_WithOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_current_row_with_order_by_error(self):
    """Check range between expr following and current row frame with order by returns an error
    when expr if greater than 0.
    """
    exitcode, message = window_frame_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND CURRENT ROW) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_Error(
        "1.0"
    )
)
def between_expr_following_and_expr_preceding_error(self):
    """Check range between expr following and expr preceding frame with order by returns an error
    when either expr is not 0.
    """
    exitcode, message = frame_start_error()

    with Example("1 following 0 preceding"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (RANGE BETWEEN 1 FOLLOWING AND 0 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )

    with Example("1 following 0 preceding"):
        self.context.node.query(
            "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND 1 PRECEDING) FROM values('number Int8', (1),(1),(2),(3))",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy_Error(
        "1.0"
    )
)
def between_expr_following_and_expr_following_with_order_by_error(self):
    """Check range between expr following and expr following frame with order by returns an error
    when the expr for the frame end is less than the expr for the framevstart.
    """
    exitcode, message = frame_start_error()

    self.context.node.query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND 0 FOLLOWING) FROM values('number Int8', (1),(1),(2),(3))",
        exitcode=exitcode,
        message=message,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_CurrentRow_ZeroSpecialCase(
        "1.0"
    )
)
def between_expr_following_and_current_row_zero_special_case(self):
    """Check range between expr following and current row frame for special case when exp is 0.
    It is expected to work.
    """
    with When("I use it with order by"):
        expected = convert_output(
            """
          number |  sum
        ---------+------
               1 |   2
               1 |   2
               2 |   2
               3 |   3
        """
        )

        execute_query(
            "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) AS sum FROM values('number Int8', (1),(1),(2),(3))",
            expected=expected,
        )

    with And("I use it without order by"):
        expected = convert_output(
            """
          number |  sum
        ---------+------
               1 |   7
               1 |   7
               2 |   7
               3 |   7
        """
        )

        execute_query(
            "SELECT number,sum(number) OVER (RANGE BETWEEN 0 FOLLOWING AND CURRENT ROW) AS sum FROM values('number Int8', (1),(1),(2),(3))",
            expected=expected,
        )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_UnboundedFollowing_WithOrderBy(
        "1.0"
    )
)
def between_expr_following_and_unbounded_following_with_order_by(self):
    """Check range between expr following and unbounded following range with order by."""
    expected = convert_output(
        """
          number |  sum
        ---------+------
               1 |   5
               1 |   5
               2 |   3
               3 |   0
        """
    )

    execute_query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS sum FROM values('number Int8', (1),(1),(2),(3))",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprPreceding_WithOrderBy_ZeroSpecialCase(
        "1.0"
    )
)
def between_expr_following_and_expr_preceding_with_order_by_zero_special_case(self):
    """Check range between expr following and expr preceding frame for special case when exp is 0.
    It is expected to work.
    """
    expected = convert_output(
        """
      number |  sum
    ---------+------
           1 |   2
           1 |   2
           2 |   2
           3 |   3
    """
    )

    execute_query(
        "SELECT number,sum(number) OVER (ORDER BY number RANGE BETWEEN 0 FOLLOWING AND 0 PRECEDING) AS sum FROM values('number Int8', (1),(1),(2),(3))",
        expected=expected,
    )


@TestScenario
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_Between_ExprFollowing_ExprFollowing_WithOrderBy(
        "1.0"
    )
)
def between_expr_following_and_expr_following_with_order_by(self):
    """Check range between expr following and expr following frame with order by when frame start
    is before frame end.
    """
    expected = convert_output(
        """
      empno | depname   | salary | sum
    --------+-----------+--------+---------
      1     | sales     | 5000   | 6000
      2     | personnel | 3900   | 14100
      3     | sales     | 4800   | 0
      4     | sales     | 4800   | 0
      5     | personnel | 3500   | 8700
      7     | develop   | 4200   | 25000
      8     | develop   | 6000   | 0
      9     | develop   | 4500   | 15400
      10    | develop   | 5200   | 6000
      11    | develop   | 5200   | 6000
    """
    )

    execute_query(
        "SELECT * FROM (SELECT empno, depname, salary, sum(salary) OVER (ORDER BY salary RANGE BETWEEN 500 FOLLOWING AND 1000 FOLLOWING) AS sum FROM empsalary) ORDER BY empno",
        expected=expected,
    )


@TestScenario
def between_unbounded_preceding_and_current_row_with_expressions_in_order_by_and_aggregate(
    self,
):
    """Check range between unbounded prceding and current row with
    expression used in the order by clause and aggregate functions.
    """
    expected = convert_output(
        """
     four | two | sum | last_value
    ------+-----+-----+------------
        0 |   0 |   0 |          0
        0 |   0 |   0 |          0
        0 |   1 |   2 |          1
        0 |   1 |   2 |          1
        0 |   2 |   4 |          2
        1 |   0 |   0 |          0
        1 |   0 |   0 |          0
        1 |   1 |   2 |          1
        1 |   1 |   2 |          1
        1 |   2 |   4 |          2
        2 |   0 |   0 |          0
        2 |   0 |   0 |          0
        2 |   1 |   2 |          1
        2 |   1 |   2 |          1
        2 |   2 |   4 |          2
        3 |   0 |   0 |          0
        3 |   0 |   0 |          0
        3 |   1 |   2 |          1
        3 |   1 |   2 |          1
        3 |   2 |   4 |          2
    """
    )

    execute_query(
        "SELECT four, toInt8(ten/4) as two, "
        "sum(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) range between unbounded preceding and current row) AS sum, "
        "last_value(toInt8(ten/4)) over (partition by four order by toInt8(ten/4) range between unbounded preceding and current row) AS last_value "
        "FROM (select distinct ten, four from tenk1)",
        expected=expected,
    )


@TestScenario
def between_current_row_and_unbounded_following_modifying_named_window(self):
    """Check range between current row and unbounded following when
    modifying named window.
    """
    expected = convert_output(
        """
     sum | unique1 | four
    -----+---------+------
      45 |       0 |    0
      45 |       8 |    0
      45 |       4 |    0
      33 |       5 |    1
      33 |       9 |    1
      33 |       1 |    1
      18 |       6 |    2
      18 |       2 |    2
      10 |       3 |    3
      10 |       7 |    3
    """
    )

    execute_query(
        "SELECT * FROM (SELECT sum(unique1) over (w range between current row and unbounded following) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10 WINDOW w AS (order by four)) ORDER BY unique1",
        expected=expected,
    )


@TestScenario
def between_current_row_and_unbounded_following_in_named_window(self):
    """Check range between current row and unbounded following in named window."""
    expected = convert_output(
        """
     first_value | last_value | unique1 | four
    -------------+------------+---------+------
          0      |      9     |    0    |  0
          1      |      9     |    1    |  1
          2      |      9     |    2    |  2
          3      |      9     |    3    |  3
          4      |      9     |    4    |  0
          5      |      9     |    5    |  1
          6      |      9     |    6    |  2
          7      |      9     |    7    |  3
          8      |      9     |    8    |  0
          9      |      9     |    9    |  1
    """
    )

    execute_query(
        "SELECT first_value(unique1) over w AS first_value, "
        "last_value(unique1) over w AS last_value, unique1, four "
        "FROM tenk1 WHERE unique1 < 10 "
        "WINDOW w AS (order by unique1 range between current row and unbounded following)",
        expected=expected,
    )


@TestScenario
def between_expr_preceding_and_expr_following_with_partition_by_two_columns(self):
    """Check range between n preceding and n following frame with partition
    by two int value columns.
    """
    expected = convert_output(
        """
     f1 | sum
    ----+-----
      1 |   0
      2 |   0
    """
    )

    execute_query(
        """
        select f1, sum(f1) over (partition by f1, f2 order by f2
                                 range between 1 following and 2 following) AS sum
        from t1 where f1 = f2
        """,
        expected=expected,
    )


@TestScenario
def between_expr_preceding_and_expr_following_with_partition_by_same_column_twice(self):
    """Check range between n preceding and n folowing with partition
    by the same column twice.
    """
    expected = convert_output(
        """
     f1 | sum
    ----+-----
      1 |   0
      2 |   0
    """
    )

    execute_query(
        """
        select * from (select f1, sum(f1) over (partition by f1, f1 order by f2
                                 range between 2 preceding and 1 preceding) AS sum
        from t1 where f1 = f2) order by f1, sum
        """,
        expected=expected,
    )


@TestScenario
def between_expr_preceding_and_expr_following_with_partition_and_order_by(self):
    """Check range between expr preceding and expr following frame used
    with partition by and order by clauses.
    """
    expected = convert_output(
        """
     f1 | sum
    ----+-----
      1 |   1
      2 |   2
    """
    )

    execute_query(
        """
        select f1, sum(f1) over (partition by f1 order by f2
                                 range between 1 preceding and 1 following) AS sum
        from t1 where f1 = f2
        """,
        expected=expected,
    )


@TestScenario
def order_by_decimal(self):
    """Check using range with order by decimal column."""
    expected = convert_output(
        """
     id | f_numeric | first_value | last_value
    ----+-----------+-------------+------------
      0 |     -1000 |           0 |          0
      1 |        -3 |           1 |          1
      2 |        -1 |           2 |          3
      3 |         0 |           2 |          4
      4 |       1.1 |           4 |          6
      5 |      1.12 |           4 |          6
      6 |         2 |           4 |          6
      7 |       100 |           7 |          7
      8 |      1000 |           8 |          8
      9 |       0   |           9 |          9
    """
    )

    execute_query(
        """
        select id, f_numeric, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from numerics
        window w as (order by f_numeric range between
                     1 preceding and 1 following)
        """,
        expected=expected,
    )


@TestScenario
def order_by_float(self):
    """Check using range with order by float column."""
    expected = convert_output(
        """
     id | f_float4  | first_value | last_value
    ----+-----------+-------------+------------
      0 |      -inf |           0 |          0
      1 |        -3 |           1 |          1
      2 |        -1 |           2 |          3
      3 |         0 |           2 |          3
      4 |       1.1 |           4 |          6
      5 |      1.12 |           4 |          6
      6 |         2 |           4 |          6
      7 |       100 |           7 |          7
      8 |       inf |           8 |          8
      9 |       nan |           8 |          8
    """
    )

    execute_query(
        """
        select id, f_float4, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from numerics
        window w as (order by f_float4 range between
                     1 preceding and 1 following)
        """,
        expected=expected,
    )


@TestScenario
def with_nulls(self):
    """Check using range frame over window with nulls."""
    expected = convert_output(
        """
     x | y  | first_value | last_value
    ---+----+-------------+------------
   \\N | 42 |          42 |         43
   \\N | 43 |          42 |         43
     1 |  1 |           1 |          3
     2 |  2 |           1 |          4
     3 |  3 |           1 |          5
     4 |  4 |           2 |          5
     5 |  5 |           3 |          5
    """
    )

    execute_query(
        """
        select x, y,
               first_value(y) over w AS first_value,
               last_value(y) over w AS last_value
        from
          (select number as x, x as y from numbers(1,5)
           union all select null, 42
           union all select null, 43)
        window w as
          (order by x asc nulls first range between 2 preceding and 2 following)
        """,
        expected=expected,
    )


@TestFeature
@Name("range frame")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame("1.0"),
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_IntAndUInt("1.0"),
)
def feature(self):
    """Check defining range frame."""
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
