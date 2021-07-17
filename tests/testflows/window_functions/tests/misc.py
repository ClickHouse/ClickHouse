from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
def subquery_expr_preceding(self):
    """Check using subquery expr in preceding.
    """
    expected = convert_output("""
     sum | unique1
    -----+---------
       0 |   0
       1 |   1
       3 |   2
       5 |   3
       7 |   4
       9 |   5
      11 |   6
      13 |   7
      15 |   8
      17 |   9
    """)

    execute_query(
        "SELECT sum(unique1) over "
        "(order by unique1 rows (SELECT unique1 FROM tenk1 ORDER BY unique1 LIMIT 1) + 1 PRECEDING) AS sum, "
        "unique1 "
        "FROM tenk1 WHERE unique1 < 10",
        expected=expected
    )

@TestScenario
def window_functions_in_select_expression(self):
    """Check using multiple window functions in an expression.
    """
    expected = convert_output("""
     cntsum
    --------
     22
     22
     87
     24
     24
     82
     92
     51
     92
     136
    """)

    execute_query(
        "SELECT (count(*) OVER (PARTITION BY four ORDER BY ten) + "
        "sum(hundred) OVER (PARTITION BY four ORDER BY ten)) AS cntsum "
        "FROM tenk1 WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
def window_functions_in_subquery(self):
    """Check using window functions in a subquery.
    """
    expected = convert_output("""
     total | fourcount | twosum
    -------+-----------+--------
    """)

    execute_query(
        "SELECT * FROM ("
        "  SELECT count(*) OVER (PARTITION BY four ORDER BY ten) + "
        "  sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS total, "
        "  count(*) OVER (PARTITION BY four ORDER BY ten) AS fourcount, "
        "  sum(hundred) OVER (PARTITION BY two ORDER BY ten) AS twosum "
        "  FROM tenk1 "
        ") WHERE total <> fourcount + twosum",
        expected=expected
    )

@TestScenario
def group_by_and_one_window(self):
    """Check running window function with group by and one window.
    """
    expected = convert_output("""
     four | ten | sum  |          avg
    ------+-----+------+------------------------
        0 |   0 |    0 |     0
        0 |   2 |    0 |     2
        0 |   4 |    0 |     4
        0 |   6 |    0 |     6
        0 |   8 |    0 |     8
        1 |   1 | 2500 |     1
        1 |   3 | 2500 |     3
        1 |   5 | 2500 |     5
        1 |   7 | 2500 |     7
        1 |   9 | 2500 |     9
        2 |   0 | 5000 |     0
        2 |   2 | 5000 |     2
        2 |   4 | 5000 |     4
        2 |   6 | 5000 |     6
        2 |   8 | 5000 |     8
        3 |   1 | 7500 |     1
        3 |   3 | 7500 |     3
        3 |   5 | 7500 |     5
        3 |   7 | 7500 |     7
        3 |   9 | 7500 |     9
    """)

    execute_query(
        "SELECT four, ten, SUM(SUM(four)) OVER (PARTITION BY four) AS sum, AVG(ten) AS avg FROM tenk1 GROUP BY four, ten ORDER BY four, ten",
        expected=expected,
    )

@TestScenario
def group_by_and_multiple_windows(self):
    """Check running window function with group by and multiple windows.
    """
    expected = convert_output("""
      sum1 | row_number |  sum2
    -------+------------+-------
     25100 |          1 | 47100
      7400 |          2 | 22000
     14600 |          3 | 14600
    """)

    execute_query(
        "SELECT sum(salary) AS sum1, row_number() OVER (ORDER BY depname) AS row_number, "
        "sum(sum(salary)) OVER (ORDER BY depname DESC) AS sum2 "
        "FROM empsalary GROUP BY depname",
        expected=expected,
    )

@TestScenario
def query_with_order_by_and_one_window(self):
    """Check using a window function in the query that has `ORDER BY` clause.
    """
    expected = convert_output("""
     depname   |    empno | salary |  rank
     ----------+----------+--------+---------
    sales      |    3     | 4800   |    1
    personnel  |    5     | 3500   |    1
    develop    |    7     | 4200   |    1
    personnel  |    2     | 3900   |    2
    sales      |    4     | 4800   |    2
    develop    |    9     | 4500   |    2
    sales      |    1     | 5000   |    3
    develop    |    10    | 5200   |    3
    develop    |    11    | 5200   |    4
    develop    |    8     | 6000   |    5
    """)
    execute_query(
        "SELECT depname, empno, salary, rank() OVER w AS rank FROM empsalary WINDOW w AS (PARTITION BY depname ORDER BY salary, empno) ORDER BY rank() OVER w, empno",
        expected=expected
    )

@TestScenario
def with_union_all(self):
    """Check using window over rows obtained with `UNION ALL`.
    """
    expected = convert_output("""
     count
    -------
    """)

    execute_query(
        "SELECT count(*) OVER (PARTITION BY four) AS count FROM (SELECT * FROM tenk1 UNION ALL SELECT * FROM tenk1) LIMIT 0",
        expected=expected
    )

@TestScenario
def empty_table(self):
    """Check using an empty table with a window function.
    """
    expected = convert_output("""
     count
    -------
    """)

    execute_query(
        "SELECT count(*) OVER (PARTITION BY four) AS count FROM (SELECT * FROM tenk1 WHERE 0)",
        expected=expected
    )

@TestScenario
def from_subquery(self):
    """Check using a window function over data from subquery.
    """
    expected = convert_output("""
     count | four
    -------+------
         4 |    1
         4 |    1
         4 |    1
         4 |    1
         2 |    3
         2 |    3
    """)

    execute_query(
        "SELECT count(*) OVER (PARTITION BY four) AS count, four FROM (SELECT * FROM tenk1 WHERE two = 1) WHERE unique2 < 10",
        expected=expected
    )

@TestScenario
def groups_frame(self):
    """Check using `GROUPS` frame.
    """
    exitcode, message = groups_frame_error()

    expected = convert_output("""
     sum | unique1 | four
    -----+---------+------
      12 |       0 |    0
      12 |       8 |    0
      12 |       4 |    0
      27 |       5 |    1
      27 |       9 |    1
      27 |       1 |    1
      35 |       6 |    2
      35 |       2 |    2
      45 |       3 |    3
      45 |       7 |    3
    """)

    execute_query("""
        SELECT sum(unique1) over (order by four groups between unbounded preceding and current row),
            unique1, four
        FROM tenk1 WHERE unique1 < 10
        """,
        exitcode=exitcode, message=message
    )

@TestScenario
def count_with_empty_over_clause_without_start(self):
    """Check that we can use `count()` window function without passing
    `*` argument when using empty over clause.
    """
    exitcode = 0
    message = "1"

    sql = ("SELECT count() OVER () FROM tenk1 LIMIT 1")

    with When("I execute query", description=sql):
        r = current().context.node.query(sql, exitcode=exitcode, message=message)


@TestScenario
def subquery_multiple_window_functions(self):
    """Check using multiple window functions is a subquery.
    """
    expected = convert_output("""
    depname |   depsalary |  depminsalary
    --------+-------------+--------------
    sales   |     5000    |  5000
    sales   |     9800    |  4800
    sales   |    14600    |  4800
    """)

    execute_query("""
        SELECT * FROM
          (SELECT depname,
                  sum(salary) OVER (PARTITION BY depname order by empno) AS depsalary,
                  min(salary) OVER (PARTITION BY depname, empno order by enroll_date) AS depminsalary
           FROM empsalary)
        WHERE depname = 'sales'
        """,
        expected=expected
    )

@TestScenario
def windows_with_same_partitioning_but_different_ordering(self):
    """Check using using two windows that use the same partitioning
    but different ordering.
    """
    expected = convert_output("""
    first | last
    ------+-----
    7     | 7
    7     | 9
    7     | 10
    7     | 11
    7     | 8
    5     | 5
    5     | 2
    3     | 3
    3     | 4
    3     | 1
    """)

    execute_query("""
        SELECT
          any(empno) OVER (PARTITION BY depname ORDER BY salary, enroll_date) AS first,
          anyLast(empno) OVER (PARTITION BY depname ORDER BY salary,enroll_date,empno) AS last
        FROM empsalary
        """,
        expected=expected
    )

@TestScenario
def subquery_with_multiple_windows_filtering(self):
    """Check filtering rows from a subquery that uses multiple window functions.
    """
    expected = convert_output("""
    depname   | empno |  salary  |  enroll_date |   first_emp |  last_emp
    ----------+-------+----------+--------------+-------------+----------
    develop   |  8    |    6000  | 2006-10-01   |      1      |  5
    develop   |  7    |    4200  | 2008-01-01   |      4      |  1
    personnel |  2    |    3900  | 2006-12-23   |      1      |  2
    personnel |  5    |    3500  | 2007-12-10   |      2      |  1
    sales     |  1    |    5000  | 2006-10-01   |      1      |  3
    sales     |  4    |    4800  | 2007-08-08   |      3      |  1
    """)

    execute_query("""
        SELECT * FROM
          (SELECT depname,
                  empno,
                  salary,
                  enroll_date,
                  row_number() OVER (PARTITION BY depname ORDER BY enroll_date, empno) AS first_emp,
                  row_number() OVER (PARTITION BY depname ORDER BY enroll_date DESC, empno) AS last_emp
           FROM empsalary) emp
        WHERE first_emp = 1 OR last_emp = 1
        """,
        expected=expected
    )

@TestScenario
def exclude_clause(self):
    """Check if exclude clause is supported.
    """
    exitcode, message = syntax_error()

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
        "SELECT sum(unique1) over (rows between 2 preceding and 2 following exclude no others) AS sum,"
        "unique1, four "
        "FROM tenk1 WHERE unique1 < 10",
        exitcode=exitcode, message=message
    )

@TestScenario
def in_view(self):
    """Check using a window function in a view.
    """
    with Given("I create a view"):
        sql = """
        CREATE VIEW v_window AS
        SELECT number, sum(number) over (order by number rows between 1 preceding and 1 following) as sum_rows
        FROM numbers(1, 10)
        """
        create_table(name="v_window", statement=sql)

    expected = convert_output("""
     number  | sum_rows
    ---------+----------
      1      |   3
      2      |   6
      3      |   9
      4      |  12
      5      |  15
      6      |  18
      7      |  21
      8      |  24
      9      |  27
     10      |  19
    """)

    execute_query(
        "SELECT * FROM v_window",
        expected=expected
    )

@TestFeature
@Name("misc")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_FrameClause("1.0")
)
def feature(self):
    """Check misc cases for frame clause.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
