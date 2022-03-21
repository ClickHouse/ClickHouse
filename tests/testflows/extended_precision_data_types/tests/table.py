from testflows.core import *
from testflows.asserts import error
from contextlib import contextmanager

from extended_precision_data_types.requirements import *
from extended_precision_data_types.common import *

@TestFeature
@Name("table")
@Requirements(
    RQ_SRS_020_ClickHouse_Extended_Precision_Create_Table("1.0"),
)
def feature(self, node="clickhouse1", mysql_node="mysql1", stress=None, parallel=None):
    """Check that clickhouse is able to create a table with extended precision data types.
    """
    node = self.context.cluster.node(node)

    table_name = f"table_{getuid()}"

    with allow_experimental_bigint(node):

        try:
            with When("I create a table with Int128, UInt128, Int256, UInt256, Decimal256"):
                node.query(f"CREATE TABLE {table_name}(a Int128, b UInt128, c Int256, d UInt256, e Decimal256(0)) ENGINE = Memory")

            with And("I insert values into the table"):
                node.query(f"INSERT INTO {table_name} VALUES (toInt128(1), toUInt128(1), toInt256(1), toUInt256(1), toDecimal256(1,0))")

            with Then("I select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert output == '1\t1\t1\t1\t1', error()

        finally:
            with Finally("I drop the table"):
                node.query(f"DROP TABLE IF EXISTS {table_name}")
