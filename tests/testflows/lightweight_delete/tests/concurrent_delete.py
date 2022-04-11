import threading

from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *

lock = threading.Lock()


@TestStep
def delete_odd(self, num_partitions, table_name, use_alter_delete=False):
    """Delete all odd `x` rows in all partitions."""
    for i in range(num_partitions):
        delete(
            table_name=table_name,
            condition=f"WHERE id == {i} AND x % 2 == 0",
            delay=random.random() / 2,
            check=True,
            use_alter_delete=use_alter_delete,
        )


@TestStep
def delete_even(self, num_partitions, table_name, use_alter_delete=False):
    """Delete all even `x` rows in all partitions."""
    for i in range(num_partitions):
        delete(
            table_name=table_name,
            condition=f"WHERE id == {i} AND x % 2 == 1",
            delay=random.random() / 2,
            check=True,
            use_alter_delete=use_alter_delete,
        )


@TestStep
def random_delete_without_overlap(self, table_name, del_list):
    while del_list:
        with lock:
            i = del_list.pop()
        condition = "WHERE" + " OR ".join(
            ["(id = " + str(j[0]) + " AND x = " + str(j[1]) + ")" for j in i]
        )
        delete(table_name=table_name, condition=condition)


@TestStep
def random_delete(self, num_partitions, table_name, block_size):
    """Delete percentage of the table in random order."""
    with When("I delete rows from the table by random parts"):

        with By("creating a list of part identifier"):
            delete_order = [
                (i, j) for i in range(num_partitions) for j in range(block_size)
            ]

        with By("randomly shuffle order of parts to be deleted"):
            random.shuffle(delete_order)

        with By("deleting randomly picked groups of rows to delete"):
            left = num_partitions * block_size
            i = 0
            del_list = []
            while i < num_partitions * block_size:
                number_of_rows_to_delete = random.randint(1, 30)
                del_list.append(
                    delete_order[i : i + min(number_of_rows_to_delete, left)]
                )
                i += number_of_rows_to_delete
                left -= number_of_rows_to_delete
            for i in del_list:
                condition = "WHERE" + " OR ".join(
                    ["(id = " + str(j[0]) + " AND x = " + str(j[1]) + ")" for j in i]
                )
                delete(
                    table_name=table_name,
                    condition=condition,
                    delay=random.random() / 20,
                )


@TestScenario
def concurrent_delete_without_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple concurrent DELETE statements targeting different rows.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with Then("I perform concurrent deletes without overlap"):
        Step(name="delete odd rows", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name
        )
        Step(name="delete even rows", test=delete_even, parallel=True)(
            num_partitions=10, table_name=table_name
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "0", error()


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentDelete_AlterDelete(
        "1.0"
    )
)
def concurrent_delete_without_overlap_with_alter_delete(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    concurrent DELETE statement and alter delete targeting different rows.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with Then("I perform concurrent deletes without overlap"):
        Step(name="delete odd rows", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name
        )
        Step(name="delete even rows", test=delete_even, parallel=True)(
            num_partitions=10, table_name=table_name, use_alter_delete=True
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "0", error()


@TestScenario
def concurrent_delete_with_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple concurrent DELETE statements targeting the same rows.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then("I perform concurrent deletes with overlap"):
        Step(name="delete odd first time", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name
        )
        Step(name="delete odd second time", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Compatibility_ConcurrentDelete_AlterDelete(
        "1.0"
    )
)
def concurrent_delete_with_overlap_with_alter_delete(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    concurrent DELETE statement and alter delete targeting the same rows.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=100000,
        )

    with When("I compute expected output"):
        output = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
        ).output

    with Then("I perform concurrent deletes with overlap"):
        Step(name="delete odd first time", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name
        )
        Step(name="delete odd second time", test=delete_odd, parallel=True)(
            num_partitions=10, table_name=table_name, use_alter_delete=True
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == output, error()


@TestOutline
def random_delete_percentage_of_the_table(
    self, percent_to_delete, block_size, partitions, node=None
):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting the same rows when deleting percentage of the table.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=1,
            block_size=block_size,
        )

    with Then("I perform concurrent random deletes with overlap"):
        Step(name="delete random rows first time", test=random_delete, parallel=True)(
            num_partitions=partitions,
            table_name=table_name,
            block_size=block_size * percent_to_delete // 100,
        )
        Step(name="delete random rows second time", test=random_delete, parallel=True)(
            num_partitions=partitions,
            table_name=table_name,
            block_size=block_size * percent_to_delete // 100,
        )

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        if self.context.table_engine == "MergeTree":
            assert (
                r.output
                == f"{block_size * partitions * (100 - percent_to_delete) // 100}"
            ), error()


@TestOutline
def random_delete_percentage_of_the_table_without_overlap(
    self, percent_to_delete, block_size, partitions, node=None
):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting different rows when deleting percentage of the table.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=1,
            block_size=block_size,
        )

    with When("I generate random list of rows to remove"):

        with By("creating a list of rows"):
            delete_order = [
                (i, j) for i in range(partitions) for j in range(block_size)
            ]

        with By("randomly shuffle order of rows to be deleted"):
            random.shuffle(delete_order)

        with By("picking random groups of rows to delete"):
            left = partitions * block_size * percent_to_delete // 100
            i = 0
            del_list = []
            while i < partitions * block_size * percent_to_delete // 100:
                number_of_rows_to_delete = random.randint(1, 50)
                del_list.append(
                    delete_order[i : i + min(number_of_rows_to_delete, left)]
                )
                i += number_of_rows_to_delete
                left -= number_of_rows_to_delete

    with Then("I perform concurrent random deletes without overlap"):
        Step(
            name="delete random rows first time",
            test=random_delete_without_overlap,
            parallel=True,
        )(table_name=table_name, del_list=del_list)
        Step(
            name="delete random rows second time",
            test=random_delete_without_overlap,
            parallel=True,
        )(table_name=table_name, del_list=del_list)

    with Then("I check that rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        if self.context.table_engine == "MergeTree":
            assert (
                r.output
                == f"{block_size * partitions * (100 - percent_to_delete) // 100}"
            ), error()


@TestScenario
def random_delete_entire_table(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting the same rows when deleting entire table.
    """
    random_delete_percentage_of_the_table(
        percent_to_delete=100, block_size=100, partitions=10
    )


@TestScenario
def random_delete_half_of_the_table(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting the same rows when deleting half of the table.
    """
    random_delete_percentage_of_the_table(
        percent_to_delete=50, block_size=100, partitions=10
    )


@TestScenario
def random_delete_25_percent_of_the_table(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting the same rows when deleting 25 percents of the table.
    """
    random_delete_percentage_of_the_table(
        percent_to_delete=25, block_size=100, partitions=10
    )


@TestScenario
def random_delete_75_percent_of_the_table(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting the same rows when deleting 75 percents of the table.
    """
    random_delete_percentage_of_the_table(
        percent_to_delete=75, block_size=100, partitions=10
    )


@TestScenario
def random_delete_entire_table_without_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting different rows when deleting entire table.
    """
    random_delete_percentage_of_the_table_without_overlap(
        percent_to_delete=100, block_size=100, partitions=10
    )


@TestScenario
def random_delete_half_of_the_table_without_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting different rows when deleting half of the table.
    """
    random_delete_percentage_of_the_table_without_overlap(
        percent_to_delete=50, block_size=100, partitions=10
    )


@TestScenario
def random_delete_25_percent_of_the_table_without_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting different rows when deleting 25 percents of the table.
    """
    random_delete_percentage_of_the_table_without_overlap(
        percent_to_delete=25, block_size=100, partitions=10
    )


@TestScenario
def random_delete_75_percent_of_the_table_without_overlap(self, node=None):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple random concurrent DELETE statements targeting different rows when deleting 75 percents of the table.
    """
    random_delete_percentage_of_the_table_without_overlap(
        percent_to_delete=75, block_size=100, partitions=10
    )


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_MultipleDeletes_ConcurrentDelete("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_MultipleDeletes_ConcurrentDeleteOverlap(
        "1.0"
    ),
    RQ_SRS_023_ClickHouse_LightweightDelete_SynchronousOperationOnSingleNode("1.0"),
)
@Name("concurrent delete")
def feature(self, node="clickhouse1"):
    """Check, that clickhouse DELETE statement SHALL perform correctly when there are
    multiple concurrent DELETE statements.
    """
    self.context.node = self.context.cluster.node(node)

    for table_engine in [
        "MergeTree",
        "ReplacingMergeTree",
        "SummingMergeTree",
        "AggregatingMergeTree",
        "CollapsingMergeTree",
        "VersionedCollapsingMergeTree",
        "GraphiteMergeTree",
    ]:

        with Feature(f"{table_engine}"):
            self.context.table_engine = table_engine
            for scenario in loads(current_module(), Scenario):
                scenario()
