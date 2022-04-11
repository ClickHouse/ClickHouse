from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *
import random


@TestOutline
@Requirements()
def random_delete_by_partition_key(
    self, percent_to_delete=100, num_partitions=100, block_size=10, node=None
):
    """Check that clickhouse support deleting rows inside the table
    in random order by deleting all rows in entire partition.

    id, x
    partition 0 (0,0),(0,1)...(0,9)
    partition 1 (1,0),(1,2)...(1,9)
    ...
    DELETE WHERE id in (5,1,...)
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert one thousand entries",
        description=f"{num_partitions} partitions, 1 part, block_size={block_size}",
    ):
        insert(
            table_name=table_name,
            partitions=num_partitions,
            parts_per_partition=1,
            block_size=block_size,
        )

    with When("I delete rows from the table by random parts"):

        with By("creating a list of part identifier"):
            delete_order = list(range(num_partitions))

        with By("randomly shuffle order of parts to be deleted"):
            random.shuffle(delete_order)

        with By("deleting randomly picked groups of rows to delete"):
            left = num_partitions * percent_to_delete // 100
            i = 0
            del_list = []
            while i < num_partitions * percent_to_delete // 100:
                number_of_rows_to_delete = random.randint(1, 11)
                del_list.append(
                    delete_order[i : i + min(number_of_rows_to_delete, left)]
                )
                i += number_of_rows_to_delete
                left -= number_of_rows_to_delete
            for i in del_list:
                condition = "WHERE id in (" + ",".join([str(j) for j in i]) + ")"
                delete(table_name=table_name, condition=condition)

    with Then("I expect rows are deleted"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        if self.context.table_engine == "MergeTree":
            assert output == str(
                num_partitions * (100 - percent_to_delete) * block_size // 100
            ), error()


@TestOutline
@Requirements()
def random_delete_by_partition_key_and_value(
    self, percent_to_delete=100, num_partitions=100, block_size=10, node=None
):
    """Check that clickhouse support deleting rows inside the table
    in random order.

    id, x
    partition 0 (0,0),(0,1)...(0,9)
    partition 1 (1,0),(1,2)...(1,9)
    ...
    DELETE WHERE (id = 3 AND x = 7) OR ...
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert one thousand entries",
        description=f"{num_partitions} partitions, 1 part, block_size={block_size}",
    ):
        insert(
            table_name=table_name,
            partitions=num_partitions,
            parts_per_partition=1,
            block_size=block_size,
        )

    with When("I delete rows from the table by random parts"):

        with By("creating a list of rows"):
            delete_order = [
                (i, j) for i in range(num_partitions) for j in range(block_size)
            ]

        with By("randomly shuffle order of rows to be deleted"):
            random.shuffle(delete_order)

        with By("deleting randomly picked groups of rows to delete"):
            left = num_partitions * block_size * percent_to_delete // 100
            i = 0
            del_list = []
            while i < num_partitions * block_size * percent_to_delete // 100:
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
                delete(table_name=table_name, condition=condition)

    with Then("I expect rows are deleted"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        if self.context.table_engine == "MergeTree":
            assert output == str(
                num_partitions * (100 - percent_to_delete) * block_size // 100
            ), error()


@TestOutline
@Requirements()
def random_delete_by_non_partition_key_modifying_all_partitions(
    self, percent_to_delete=100, num_partitions=100, block_size=10, node=None
):
    """Check that clickhouse support deleting rows inside the table
    in random order when every delete modify all partitions.

    id, x
    partition 0 (0,0),(0,1)...(0,9)
    partition 1 (1,0),(1,2)...(1,9)
    ...
    DELETE WHERE x in (7,3,...) - I will touch all 100 partitions
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert one thousand entries",
        description=f"{num_partitions} partitions, 1 part, block_size={block_size}",
    ):
        insert(
            table_name=table_name,
            partitions=num_partitions,
            parts_per_partition=1,
            block_size=block_size,
        )

    with When("I delete rows from the table by random parts"):

        with By("creating a list of part identifier"):
            delete_order = list(range(block_size))

        with By("randomly shuffle order of parts to be deleted"):
            random.shuffle(delete_order)

        with By("deleting randomly picked groups of rows to delete"):
            left = block_size * percent_to_delete // 100
            i = 0
            del_list = []
            while i < block_size * percent_to_delete // 100:
                number_of_rows_to_delete = random.randint(1, 4)
                del_list.append(
                    delete_order[i : i + min(number_of_rows_to_delete, left)]
                )
                i += number_of_rows_to_delete
                left -= number_of_rows_to_delete

            for i in del_list:
                condition = "WHERE x in (" + ",".join([str(j) for j in i]) + ")"
                delete(table_name=table_name, condition=condition)

    with Then("I expect rows are deleted"):
        output = node.query(f"SELECT count(*) FROM {table_name}").output
        if self.context.table_engine == "MergeTree":
            assert output == str(
                num_partitions * (100 - percent_to_delete) * block_size // 100
            ), error()


@TestOutline
def delete_with_stopped_merges(self, partitions=1, block_size=2, node=None):
    """Check that deleted rows stay deleted after parts are merged
    by performing DELETE after stopping merges and verifying that
    selected rows stay deleted after parts are forced to merge.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When("I insert entries in the first partition"):
            insert(
                table_name=table_name,
                partitions=partitions,
                parts_per_partition=1,
                block_size=block_size,
            )

        with When("I insert entries in the first partition second time"):
            insert(
                table_name=table_name,
                partitions=partitions,
                parts_per_partition=1,
                block_size=block_size,
            )

        with Then("I delete some of the inserted rows when merges are stopped"):
            delete(
                table_name=table_name,
                condition="WHERE x > 0",
                optimize=False,
                settings=[],
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I expect deleted rows to stay deleted"):
        expected_output = "\n".join(
            [
                '{"id":' + f'"{i}"' + ',"x":"0"}\n{"id":' + f'"{i}"' + ',"x":"0"}'
                for i in range(partitions)
            ]
        )
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                output = node.query(
                    f"SELECT * FROM {table_name} ORDER BY id FORMAT JSONEachRow"
                ).output
                if self.context.table_engine == "MergeTree":
                    assert output == expected_output, error()


@TestScenario
def random_delete_by_partition_key_entire_table(self, node=None):
    """Check that clickhouse supports deleting all rows inside the table
    in random order by deleting all rows in entire partition.
    """
    random_delete_by_partition_key(percent_to_delete=100, node=node)


@TestScenario
def random_delete_by_partition_key_half_of_the_table(self, node=None):
    """Check that clickhouse supports deleting half of the rows inside the table
    in random order by deleting all rows in entire partition.
    """
    random_delete_by_partition_key(percent_to_delete=50, node=node)


@TestScenario
def random_delete_by_non_partition_key_modifying_all_partitions_entire_table(
    self, node=None
):
    """Check that clickhouse supports deleting all rows inside the table
    in random order when every delete modify all partitions.
    """
    random_delete_by_non_partition_key_modifying_all_partitions(
        percent_to_delete=100, node=node
    )


@TestScenario
def random_delete_by_non_partition_key_modifying_all_partitions_half_of_the_table(
    self, node=None
):
    """Check that clickhouse supports deleting half of the rows inside the table
    in random order when every delete modify all partitions.
    """
    random_delete_by_non_partition_key_modifying_all_partitions(
        percent_to_delete=50, node=node
    )


@TestScenario
def random_delete_by_partition_key_and_value_entire_table(self, node=None):
    """Check that clickhouse supports deleting all rows inside the table
    in random order.
    """
    random_delete_by_partition_key_and_value(percent_to_delete=100, node=node)


@TestScenario
def random_delete_by_partition_key_and_value_half_of_the_table(self, node=None):
    """Check that clickhouse supports deleting half of the rows inside the table
    in random order.
    """
    random_delete_by_partition_key_and_value(percent_to_delete=50, node=node)


@TestScenario
def delete_with_stop_merges_many_partitions(self, node=None):
    """Check that deleted rows stay deleted after parts are merged hundred partitions."""
    delete_with_stopped_merges(partitions=100, block_size=10, node=node)


@TestScenario
def delete_with_stop_merges_one_partition(self, node=None):
    """Check that deleted rows stay deleted after parts are merged one partition."""
    delete_with_stopped_merges(partitions=1, block_size=100, node=node)


@TestFeature
@Name("specific deletes")
def feature(self, node="clickhouse1"):
    """Check that clickhouse support random deletes and deletes with stop merges."""
    self.context.node = self.context.cluster.node(node)

    # for table_engine in ["MergeTree", "ReplacingMergeTree", "SummingMergeTree",
    #                     "AggregatingMergeTree", "CollapsingMergeTree",
    #                     "VersionedCollapsingMergeTree", "GraphiteMergeTree"]:

    self.context.table_engine = "MergeTree"
    for suite in loads(current_module(), Suite):
        suite()
