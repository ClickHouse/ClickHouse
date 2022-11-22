from testflows.core import *

from window_functions.requirements import *
from window_functions.tests.common import *


@TestScenario
def positive_overflow_with_Int16(self):
    """Check positive overflow with Int16."""
    expected = convert_output(
        """
       x   | last_value 
    -------+------------
     32764 |          0
     32765 |          0
     32766 |          0
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by toInt16(x) range between current row and 2147450884 following) AS last_value
        from numbers(32764, 3)
        """,
        expected=expected,
    )


@TestScenario
def negative_overflow_with_Int16(self):
    """Check negative overflow with Int16."""
    expected = convert_output(
        """
       x    | last_value 
    --------+------------
     -32764 |          0
     -32765 |          0
     -32766 |          0
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by toInt16(x) desc range between current row and 2147450885 following) as last_value
        from (SELECT -number - 32763 AS number FROM numbers(1, 3))
        """,
        expected=expected,
    )


@TestScenario
def positive_overflow_for_Int32(self):
    """Check positive overflow for Int32."""
    expected = convert_output(
        """
         x      | last_value 
    ------------+------------
     2147483644 | 2147483646
     2147483645 | 2147483646
     2147483646 | 2147483646
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by x range between current row and 4 following) as last_value
        from numbers(2147483644, 3) 
        """,
        expected=expected,
    )


@TestScenario
def negative_overflow_for_Int32(self):
    """Check negative overflow for Int32."""
    expected = convert_output(
        """
          x      | last_value  
    -------------+-------------
     -2147483644 | -2147483646
     -2147483645 | -2147483646
     -2147483646 | -2147483646
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by x desc range between current row and 5 following) as last_value
        from (select -number-2147483643 AS number FROM numbers(1,3))
        """,
        expected=expected,
    )


@TestScenario
def positive_overflow_for_Int64(self):
    """Check positive overflow for Int64."""
    expected = convert_output(
        """
              x          |     last_value      
    ---------------------+---------------------
     9223372036854775804 | 9223372036854775806
     9223372036854775805 | 9223372036854775806
     9223372036854775806 | 9223372036854775806
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by x range between current row and 4 following) as last_value
        from numbers(9223372036854775804, 3) 
        """,
        expected=expected,
    )


@TestScenario
def negative_overflow_for_Int64(self):
    """Check negative overflow for Int64."""
    expected = convert_output(
        """
              x           |      last_value      
    ----------------------+----------------------
     -9223372036854775804 | -9223372036854775806
     -9223372036854775805 | -9223372036854775806
     -9223372036854775806 | -9223372036854775806
    """
    )

    execute_query(
        """
        select number as x, last_value(x) over (order by x desc range between current row and 5 following) as last_value
        from (select -number-9223372036854775803 AS number from numbers(1,3)) 
        """,
        expected=expected,
    )


@TestFeature
@Name("range overflow")
@Requirements(RQ_SRS_019_ClickHouse_WindowFunctions_RangeFrame("1.0"))
def feature(self):
    """Check using range frame with overflows."""
    for scenario in loads(current_module(), Scenario):
        Scenario(run=scenario, flags=TE)
