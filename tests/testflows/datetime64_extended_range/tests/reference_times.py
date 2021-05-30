import pytz
from datetime import datetime

from testflows.core import *
from datetime64_extended_range.common import *
from datetime64_extended_range.tests.common import select_check_datetime
from datetime64_extended_range.requirements.requirements import *
from datetime64_extended_range.tests.common import *


@TestSuite
@Requirements(
    RQ_SRS_010_DateTime64_ExtendedRange_SpecificTimestamps("1.0")
)
def reference_times(self, node="clickhouse1"):
    """Check how ClickHouse converts a set of particular timestamps
    to DateTime64 for all timezones and compare the result to pytz.
    """
    self.context.node = self.context.cluster.node(node)

    timestamps = [9961200, 73476000, 325666800, 354675600, 370400400, 386125200, 388566010, 401850000, 417574811,
        496803600, 528253200, 624423614, 636516015, 671011200, 717555600, 752047218, 859683600, 922582800,
        1018173600, 1035705600, 1143334800, 1162105223, 1174784400, 1194156000, 1206838823, 1224982823,
        1236495624, 1319936400, 1319936424, 1425798025, 1459040400, 1509872400, 2090451627, 2140668000]

    query = ""

    for tz in pytz.all_timezones:
        timezone = pytz.timezone(tz)
        query += f"select '{tz}', arrayJoin(arrayFilter(x -> x.2 <> x.3, arrayMap(x -> tuple(x.1, x.2, toString(toDateTime64(x.1, 0, '{tz}'))), ["
        need_comma = 0
        for timestamp in timestamps:
            for reference_timestamp in [timestamp - 1, timestamp, timestamp + 1]:
                query += f"{',' if need_comma else ''}tuple({reference_timestamp},'{datetime.datetime.fromtimestamp(reference_timestamp, timezone).strftime('%Y-%m-%d %H:%M:%S')}')"
                need_comma = 1
        query += "] ) ) );"

    exec_query(request=query, expected="")
