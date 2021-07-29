from testflows.core import *

from datetime64_extended_range.requirements.requirements import *
from datetime64_extended_range.common import *
from datetime64_extended_range.tests.common import *

import pytz
import datetime
import itertools

@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start("1.0"),
)
def normal_range_start(self):
    """Check DateTime64 can accept a dates around the start of the normal range that begins at 1970-01-01 00:00:00.000.
    """
    with When("I do incrementing time sweep", description="check different time points in the first 24 hours at given date"):
        walk_datetime_in_incrementing_steps(date="1970-01-01", precision=3, hrs_range=(0, 24))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End("1.0")
)
def normal_range_end(self):
    """Check DateTime64 can accept a dates around the end of the normal range that ends at 2105-12-31 23:59:59.99999.
    """
    with When("I do decrementing time sweep",
            description="check different time points in the last 24 hours at given date"):
        walk_datetime_in_decrementing_steps(date="2105-12-31", precision=3, hrs_range=(23, 0))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_Start("1.0")
)
def extended_range_start(self):
    """Check DateTime64 supports dates around the beginning of the extended range that begins at 1698-01-01 00:00:00.000000.
    """
    with When("I do incrementing time sweep",
            description="check different time points in the first 24 hours at given date"):
        walk_datetime_in_incrementing_steps(date="1925-01-01", precision=5, hrs_range=(0, 24))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_End("1.0")
)
def extended_range_end(self, precision=3):
    """Check DateTime64 supports dates around the beginning of the extended range that ends at 2377-12-31T23:59:59.999999.
    """
    with When("I do decrementing time sweep",
            description="check different time points in the last 24 hours at given date"):
        walk_datetime_in_decrementing_steps(date="2238-12-31", precision=5, hrs_range=(23, 0))


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_Start_BeforeEpochForTimeZone("1.0")
)
def timezone_local_below_normal_range(self):
    """Check how UTC normal range time value treated
    when current timezone time value is out of normal range.
    """
    with When("I do incrementing time sweep",
            description="check different time points when UTC datetime fits normal range but below it for local datetime"):
        walk_datetime_in_incrementing_steps(date="1969-12-31", hrs_range=(17, 24), timezone='America/Phoenix')


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_NormalRange_End_AfterEpochForTimeZone("1.0")
)
def timezone_local_above_normal_range(self):
    """Check how UTC normal range time value treated
    when current timezone time value is out of normal range.
    """
    with When("I do decrementing time sweep",
              description="check different time points when UTC datetime fits normal range but above it for local datetime"):
        walk_datetime_in_decrementing_steps(date="2106-01-01", hrs_range=(6, 0), timezone='Asia/Novosibirsk')


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_Comparison("1.0")
)
def comparison_check(self):
    """Check how comparison works with DateTime64 extended range.
    """
    stress = self.context.stress
    comparators = (">", "<", "==", "<=", ">=", "!=")
    timezones = timezones_range(stress=stress)
    datetimes = []

    for year in years_range(stress=stress):
        datetimes += list(select_dates_in_year(year=year, stress=stress))

    for dt_1, dt_2 in itertools.product(datetimes, datetimes):
        for tz1, tz2 in itertools.product(timezones, timezones):
            dt1_str = dt_1.strftime("%Y-%m-%d %H:%M:%S")
            dt2_str = dt_2.strftime("%Y-%m-%d %H:%M:%S")
            dt1 = pytz.timezone(tz1).localize(dt_1)
            dt2 = pytz.timezone(tz2).localize(dt_2)

            with When(f"{dt1_str} {tz1}, {dt2_str} {tz2}"):
                for c in comparators:
                    expr = f"dt1 {c} dt2"
                    expected = str(int(eval(expr)))
                    with Then(f"I check {dt1_str} {c} {dt2_str}"):
                        query = f"SELECT toDateTime64('{dt1_str}', 0, '{tz1}') {c} toDateTime64('{dt2_str}', 0, '{tz2}')"
                        exec_query(request=query, expected=expected)


@TestScenario
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_TimeZones("1.0")
)
def timezones_support(self):
    """Check how timezones work with DateTime64 extended range.
    """
    stress = self.context.stress
    timezones = timezones_range(stress=stress)

    for year in years_range(stress):
        with Given("I select datetimes in a year"):
            datetimes = select_dates_in_year(year=year, stress=stress)

        for dt in datetimes:
            for tz in timezones:
                with Step(f"{dt} {tz}"):
                    with By("computing expected output using python"):
                        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                    with And("forming a toTimeZone ClickHouse query"):
                        query = f"SELECT toDateTime64('{dt_str}', 0, '{tz}')"
                    with Then(f"I execute query", flags=TE):
                        exec_query(request=query, expected=f"{dt_str}")


@TestFeature
def generic(self, node="clickhouse1"):
    """Check the basic operations with DateTime64
    """
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario, Suite):
        Scenario(run=scenario)
