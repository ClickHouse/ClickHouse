from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *
import time


@TestStep
def watch_live_view(self, view_name, setting, node=None):
    """Check live view output is correct."""
    if node is None:
        node = self.context.node
    r = node.query(f"WATCH {view_name} LIMIT 1", settings=[(f"{setting}", 1)])

    assert r.output != "510	1\n1510	2", error()


@TestStep
def watch_window_view(self, view_name, setting, node=None):
    """Check window view output is correct."""
    if node is None:
        node = self.context.node
    r = node.query(f"WATCH {view_name} LIMIT 1", settings=[(f"{setting}", 1)])

    assert r.output != "1000", error()


@TestStep
def delete_from_table_with_watch(self, table_name, condition, node=None):
    """Delete from the table after 5 seconds."""
    if node is None:
        node = self.contex.node
    time.sleep(5)
    delete(table_name=table_name, condition=condition)


@TestStep
def insert_into_table_with_watch(self, table_name, node=None):
    """Insert into table after 10 seconds."""
    if node is None:
        node = self.context.node
    time.sleep(20)
    node.query(f"INSERT INTO {table_name} VALUES (1, 1)")


@TestScenario
def normal_view(self, node=None):
    """Check that clickhouse DELETE statement is compatible with tables that have one or more normal views."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    view_name_1 = f"view_{getuid()}_1"
    view_name_2 = f"view_{getuid()}_2"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When(f"I create two views"):
        create_view(
            view_type="",
            view_name=view_name_1,
            condition=f"SELECT count(*) FROM {table_name}",
        )
        create_view(
            view_type="",
            view_name=view_name_2,
            condition=f"SELECT count() FROM {table_name} group by id order by id",
        )

    with Then(f"I perform delete operation"):
        r = delete(table_name=table_name, condition="WHERE x > 50")

    with Then("I check views perform correctly"):
        r1 = node.query(f"SELECT count(*) FROM {table_name}")
        r2 = node.query(f"SELECT * FROM {view_name_1}")
        assert r1.output == r2.output
        r1 = node.query(f"SELECT count() FROM {table_name} group by id order by id")
        r2 = node.query(f"SELECT * FROM {view_name_2}")
        assert r1.output == r2.output


@TestScenario
def live_view(self, node=None):
    """Check that clickhouse DELETE statement is compatible with tables that have one or more live views."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    view_name_1 = f"view_{getuid()}_1"
    view_name_2 = f"view_{getuid()}_2"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When(f"I create two views"):
        create_view(
            view_type="LIVE",
            view_name=view_name_1,
            condition=f"SELECT count(*) FROM {table_name}",
        )
        create_view(
            view_type="LIVE",
            view_name=view_name_2,
            condition=f"SELECT count() FROM {table_name} group by id order by id",
        )

    with Then(f"I perform delete operation"):
        r = delete(table_name=table_name, condition="WHERE x > 50")

    with Then("I check views perform correctly"):
        r1 = node.query(f"SELECT count(*) FROM {table_name}")
        r2 = node.query(f"SELECT * FROM {view_name_1}")
        assert r1.output == r2.output
        r1 = node.query(f"SELECT count() FROM {table_name} group by id order by id")
        r2 = node.query(f"SELECT * FROM {view_name_2}")
        assert r1.output == r2.output

    with Then("I watch live view", description="concurrent watch, delete, insert"):
        Step(name="watch", test=watch_live_view, parallel=True)(
            view_name=view_name_1, setting="allow_experimental_live_view"
        )
        Step(name="delete", test=delete_from_table_with_watch, parallel=True)(
            table_name=table_name, condition="WHERE x > 10", node=node
        )
        Step(name="insert", test=insert, parallel=True)(
            table_name=table_name, node=node
        )


@TestScenario
@Requirements()
def window_view(self, node=None):
    """Check that clickhouse DELETE statement is compatible with tables that have one or more window views."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}_1"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When(f"I create view"):
        create_view(
            view_type="WINDOW",
            view_name=view_name,
            condition=f"SELECT count(*) FROM {table_name} GROUP BY tumble(now(), INTERVAL '5' SECOND)",
        )

    with Then("I watch window view", description="concurrent watch, delete, insert"):
        Step(name="watch", test=watch_window_view, parallel=True)(
            view_name=view_name, setting="allow_experimental_window_view"
        )
        Step(name="delete", test=delete_from_table_with_watch, parallel=True)(
            table_name=table_name, condition="WHERE x > 10", node=node
        )
        Step(name="insert", test=insert, parallel=True)(
            table_name=table_name, node=node
        )


@TestScenario
def materialized_view(self, node=None):
    """Check that clickhouse DELETE statement is compatible with tables that have one or more normal views."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"
    view_name_1 = f"view_{getuid()}_1"
    view_name_2 = f"view_{getuid()}_2"

    with Given("I have a table"):
        create_partitioned_table(table_name=table_name)

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When(f"I create two views"):
        r = node.query(
            f"CREATE MATERIALIZED VIEW {view_name_1} ENGINE = MergeTree() ORDER BY count\
                    POPULATE as SELECT count(*) as count FROM {table_name}"
        )

        r = node.query(
            f"CREATE MATERIALIZED VIEW {view_name_2} ENGINE = MergeTree() ORDER BY max\
            POPULATE as select max(count) as max  from (SELECT count() as count  FROM {table_name} group by id order by id)"
        )

    with Then(f"I perform delete operation"):
        r = delete(table_name=table_name, condition="WHERE x > 50")

    with Then(f"I force merges"):
        optimize_table(table_name=view_name_1)
        optimize_table(table_name=view_name_2)

    with Then("I check views perform correctly"):
        r1 = node.query(f"SELECT count(*) FROM {table_name}")
        r2 = node.query(f"SELECT * FROM {view_name_1}")
        assert r1.output == r2.output
        r1 = node.query(f"SELECT count() FROM {table_name} group by id order by id")
        r2 = node.query(f"SELECT * FROM {view_name_2}")
        assert r1.output == r2.output


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_Views("1.0"))
@Name("views")
def feature(self, node="clickhouse1"):
    """Check that clickhouse DELETE statement is compatible with tables that have one or more views."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
