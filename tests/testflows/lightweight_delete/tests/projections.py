from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
def simple_projection(self, number_of_rows=10000000, node=None):
    """Check delete on a table that has simple projection
    which optimizes query with a simple where clause.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    projection_name_1 = f"projection_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name, partition="")

    with When("I insert data into the table"):
        node.query(
            f"insert into {table_name} select number%100, number%100 from numbers({number_of_rows})"
        )

    with When(f"I create projection"):
        r = node.query(
            f"ALTER TABLE {table_name} ADD PROJECTION {projection_name_1} "
            f"(SELECT * order by x)",
            settings=[("allow_experimental_projection_optimization", 1)],
        )

        r = node.query(
            f"ALTER TABLE {table_name} MATERIALIZE PROJECTION {projection_name_1}",
            settings=[
                ("allow_experimental_projection_optimization", 1),
                ("mutations_sync", 2),
            ],
        )

    with Then("I check that projection works"):
        r = node.command(
            f"clickhouse client --format JSONEachRowWithProgress -n -q "
            f'"set allow_experimental_projection_optimization=1; select count(*) '
            f'from {table_name} where x >= 50"',
            timeout=1,
        )

        with By("checking query result"):
            temp_string = r.output[r.output.find('{"row":{"count()":"') + 19 :]
            temp_string = temp_string[0 : temp_string.find('"')]
            assert int(temp_string) == number_of_rows // 2, error()

        with And("read rows count"):
            temp_string = r.output[r.output.find('read_rows":"') + 12 :]
            temp_string = temp_string[0 : temp_string.find('"')]
            assert int(temp_string) < int(number_of_rows / 1.9), error()

    with When(f"I perform delete operation that deletes half of the rows"):
        r = delete(table_name=table_name, condition="WHERE id % 2 = 0")

    with Then("I check that projection works after deletion"):
        r = node.command(
            f"clickhouse client --format JSONEachRowWithProgress -n -q "
            f'"set allow_experimental_projection_optimization=1; select count(*) '
            f'from {table_name} where x >= 50"',
            timeout=1,
        )

        with By("checking query result"):
            temp_string = r.output[r.output.find('{"row":{"count()":"') + 19 :]
            temp_string = temp_string[0 : temp_string.find('"')]
            assert int(temp_string) == number_of_rows // 4, error()

        with And("read rows count"):
            temp_string = r.output[r.output.find('read_rows":"') + 12 :]
            temp_string = temp_string[0 : temp_string.find('"')]
            assert int(temp_string) < int(number_of_rows / 3.9), error()


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_Projections("1.0"))
@Name("projections")
def feature(self, node="clickhouse1"):
    """Check that tables that have one or more projections work with delete."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
