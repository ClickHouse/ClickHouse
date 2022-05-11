from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *

@TestScenario
def order_by_asc_range_between_days_preceding_and_days_following(self):
    """Check range between days preceding and days following
    with ascending order by.
    """
    expected = convert_output("""
      sum  | salary | enroll_date
    -------+--------+-------------
     34900 |  5000  | 2006-10-01
     38400 |  3900  | 2006-12-23
     47100 |  4800  | 2007-08-01
     47100 |  4800  | 2007-08-08
     36100 |  3500  | 2007-12-10
     32200 |  4200  | 2008-01-01
     34900 |  6000  | 2006-10-01
     32200 |  4500  | 2008-01-01
     47100 |  5200  | 2007-08-01
     47100 |  5200  | 2007-08-15
    """)

    execute_query(
        "select sum(salary) over (order by enroll_date range between 365 preceding and 365 following) AS sum, "
        "salary, enroll_date from empsalary order by empno",
        expected=expected
    )

@TestScenario
def order_by_desc_range_between_days_preceding_and_days_following(self):
    """Check range between days preceding and days following
    with descending order by."""
    expected = convert_output("""
      sum  | salary | enroll_date
    -------+--------+-------------
     34900 |  5000  | 2006-10-01
     38400 |  3900  | 2006-12-23
     47100 |  4800  | 2007-08-01
     47100 |  4800  | 2007-08-08
     36100 |  3500  | 2007-12-10
     32200 |  4200  | 2008-01-01
     34900 |  6000  | 2006-10-01
     32200 |  4500  | 2008-01-01
     47100 |  5200  | 2007-08-01
     47100 |  5200  | 2007-08-15
    """)

    execute_query(
        "select sum(salary) over (order by enroll_date desc range between 365 preceding and 365 following) AS sum, "
        "salary, enroll_date from empsalary order by empno",
        expected=expected
    )

@TestScenario
def order_by_desc_range_between_days_following_and_days_following(self):
    """Check range between days following and days following with
    descending order by.
    """
    expected = convert_output("""
      sum  | salary | enroll_date
    -------+--------+-------------
       0   |  5000  | 2006-10-01
       0   |  3900  | 2006-12-23
       0   |  4800  | 2007-08-01
       0   |  4800  | 2007-08-08
       0   |  3500  | 2007-12-10
       0   |  4200  | 2008-01-01
       0   |  6000  | 2006-10-01
       0   |  4500  | 2008-01-01
       0   |  5200  | 2007-08-01
       0   |  5200  | 2007-08-15
    """)

    execute_query(
        "select sum(salary) over (order by enroll_date desc range between 365 following and 365 following) AS sum, "
        "salary, enroll_date from empsalary order by empno",
        expected=expected
    )

@TestScenario
def order_by_desc_range_between_days_preceding_and_days_preceding(self):
    """Check range between days preceding and days preceding with
    descending order by.
    """
    expected = convert_output("""
      sum  | salary | enroll_date
    -------+--------+-------------
       0   |  5000  | 2006-10-01
       0   |  3900  | 2006-12-23
       0   |  4800  | 2007-08-01
       0   |  4800  | 2007-08-08
       0   |  3500  | 2007-12-10
       0   |  4200  | 2008-01-01
       0   |  6000  | 2006-10-01
       0   |  4500  | 2008-01-01
       0   |  5200  | 2007-08-01
       0   |  5200  | 2007-08-15
    """)

    execute_query(
        "select sum(salary) over (order by enroll_date desc range between 365 preceding and 365 preceding) AS sum, "
        "salary, enroll_date from empsalary order by empno",
        expected=expected
    )

@TestScenario
def datetime_with_timezone_order_by_asc_range_between_n_preceding_and_n_following(self):
    """Check range between preceding and following with
    DateTime column that has timezone using ascending order by.
    """
    expected = convert_output("""
     id |        f_timestamptz         | first_value | last_value
    ----+------------------------------+-------------+------------
      1 | 2000-10-19 10:23:54          |           1 |          3
      2 | 2001-10-19 10:23:54          |           1 |          4
      3 | 2001-10-19 10:23:54          |           1 |          4
      4 | 2002-10-19 10:23:54          |           2 |          5
      5 | 2003-10-19 10:23:54          |           4 |          6
      6 | 2004-10-19 10:23:54          |           5 |          7
      7 | 2005-10-19 10:23:54          |           6 |          8
      8 | 2006-10-19 10:23:54          |           7 |          9
      9 | 2007-10-19 10:23:54          |           8 |         10
     10 | 2008-10-19 10:23:54          |           9 |         10
    """)

    execute_query(
        """
        select id, f_timestamptz, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from datetimes
        window w as (order by f_timestamptz range between
                     31622400 preceding and 31622400 following) order by id
        """,
        expected=expected
    )

@TestScenario
def datetime_with_timezone_order_by_desc_range_between_n_preceding_and_n_following(self):
    """Check range between preceding and following with
    DateTime column that has timezone using descending order by.
    """
    expected = convert_output("""
     id |        f_timestamptz         | first_value | last_value
    ----+------------------------------+-------------+------------
     10 | 2008-10-19 10:23:54          |          10 |          9
      9 | 2007-10-19 10:23:54          |          10 |          8
      8 | 2006-10-19 10:23:54          |           9 |          7
      7 | 2005-10-19 10:23:54          |           8 |          6
      6 | 2004-10-19 10:23:54          |           7 |          5
      5 | 2003-10-19 10:23:54          |           6 |          4
      4 | 2002-10-19 10:23:54          |           5 |          3
      3 | 2001-10-19 10:23:54          |           4 |          1
      2 | 2001-10-19 10:23:54          |           4 |          1
      1 | 2000-10-19 10:23:54          |           2 |          1
    """)

    execute_query(
        """
        select id, f_timestamptz, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from datetimes
        window w as (order by f_timestamptz desc range between
                     31622400 preceding and 31622400 following) order by id desc
        """,
        expected=expected
    )

@TestScenario
def datetime_order_by_asc_range_between_n_preceding_and_n_following(self):
    """Check range between preceding and following with
    DateTime column and ascending order by.
    """
    expected = convert_output("""
     id |        f_timestamp         | first_value | last_value
    ----+------------------------------+-------------+------------
      1 | 2000-10-19 10:23:54          |           1 |          3
      2 | 2001-10-19 10:23:54          |           1 |          4
      3 | 2001-10-19 10:23:54          |           1 |          4
      4 | 2002-10-19 10:23:54          |           2 |          5
      5 | 2003-10-19 10:23:54          |           4 |          6
      6 | 2004-10-19 10:23:54          |           5 |          7
      7 | 2005-10-19 10:23:54          |           6 |          8
      8 | 2006-10-19 10:23:54          |           7 |          9
      9 | 2007-10-19 10:23:54          |           8 |         10
     10 | 2008-10-19 10:23:54          |           9 |         10
    """)

    execute_query(
        """
        select id, f_timestamp, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from datetimes
        window w as (order by f_timestamp range between
                     31622400 preceding and 31622400 following) ORDER BY id
        """,
        expected=expected
    )

@TestScenario
def datetime_order_by_desc_range_between_n_preceding_and_n_following(self):
    """Check range between preceding and following with
    DateTime column and descending order by.
    """
    expected = convert_output("""
     id |        f_timestamp           | first_value | last_value
    ----+------------------------------+-------------+------------
     10 | 2008-10-19 10:23:54          |          10 |          9
      9 | 2007-10-19 10:23:54          |          10 |          8
      8 | 2006-10-19 10:23:54          |           9 |          7
      7 | 2005-10-19 10:23:54          |           8 |          6
      6 | 2004-10-19 10:23:54          |           7 |          5
      5 | 2003-10-19 10:23:54          |           6 |          4
      4 | 2002-10-19 10:23:54          |           5 |          3
      2 | 2001-10-19 10:23:54          |           4 |          1
      3 | 2001-10-19 10:23:54          |           4 |          1
      1 | 2000-10-19 10:23:54          |           2 |          1
    """)

    execute_query(
        """
        select id, f_timestamp, first_value(id) over w AS first_value, last_value(id) over w AS last_value
        from datetimes
        window w as (order by f_timestamp desc range between
                     31622400 preceding and 31622400 following)
        """,
        expected=expected
    )

@TestFeature
@Name("range datetime")
@Requirements(
    RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame_DataTypes_DateAndDateTime("1.0")
)
def feature(self):
    """Check `Date` and `DateTime` data time with range frames.
    """
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
