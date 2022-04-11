from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *


@TestScenario
def detach_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that detach partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check detach partition after delete perform correctly"):
        detach_partition_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def drop_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that drop partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check drop partition after delete perform correctly"):
        drop_partition_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def attach_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that attach partition after delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check attach partition perform correctly"):
        attach_partition_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def freeze_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that freeze partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check freeze partition perform correctly"):
        freeze_partition_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def unfreeze_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that unfreeze partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check unfreeze partition perform correctly"):
        unfreeze_partition_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def add_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that add column after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check add column perform correctly"):
        add_column_after_delete_in_the_table(
            table_name=table_name, partitions=partitions
        )


@TestScenario
def drop_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that drop column after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check drop column after delete perform correctly"):
        drop_column_after_delete_in_the_table(
            table_name=table_name, partitions=partitions, column_name="x"
        )


@TestScenario
def clear_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that clear column after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check clear column perform correctly"):
        clear_column_after_delete_in_the_table(
            table_name=table_name, partitions=partitions, column_name="x"
        )


@TestScenario
def modify_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that modify column after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check modify column perform correctly"):
        modify_column_after_delete_in_the_table(
            table_name=table_name,
            partitions=partitions,
            column_name="x",
            column_type="String",
            default_expr="DEFAULT '777'",
        )


@TestScenario
def comment_column_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that alter comment column after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with Then("I check comment column perform correctly"):
        comment_column_after_delete_in_the_table(
            table_name=table_name,
            partitions=partitions,
            column_name="x",
            column_comment="hello",
        )


@TestScenario
def move_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that alter move partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name_1 = f"table_{getuid()}_1"

    table_name_2 = f"table_{getuid()}_2"

    with Given("I have tables"):
        create_table(table_name=table_name_1)
        create_table(table_name=table_name_2)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name_1,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )
        insert(
            table_name=table_name_2,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with When("I compute expected output"):
        output11 = node.query(f"SELECT count(*) FROM {table_name_2}").output
        output12 = node.query(
            f"SELECT count(*) FROM {table_name_1} WHERE NOT(x % 2 == 0 OR id != 3)"
        ).output
        output2 = node.query(
            f"SELECT count(*) FROM {table_name_2} WHERE NOT(x % 2 == 0 or id = 3)"
        ).output

    with And(f"i delete half of the rows in {table_name_1}"):
        delete_odd(table_name=table_name_1, num_partitions=partitions)

    with And("I move partition with deleted row into the other table"):
        alter_move_partition(
            table_name_1=table_name_1, table_name_2=table_name_2, partition_expr="3"
        )

    with Then("I check move partition after delete perform correctly"):
        r = node.query(f"SELECT count(*) FROM {table_name_2}")
        assert r.output == str(int(output11) + int(output12)), error()
        r = node.query(f"SELECT count(*) FROM {table_name_1}")
        assert r.output == output2, error()


@TestScenario
def replace_partition_after_delete(
    self, partitions=10, parts_per_partition=1, block_size=100, node=None
):
    """Check that replace partition after lightweight delete perform correctly."""
    if node is None:
        node = self.context.node

    table_name_1 = f"table_{getuid()}_1"

    table_name_2 = f"table_{getuid()}_2"

    with Given("I have tables"):
        create_table(table_name=table_name_1)
        create_table(table_name=table_name_2)

    with When(
        "I insert data into the tables",
        description=f"{partitions} partitions, "
        f"{parts_per_partition} parts in each partition, block_size={block_size}",
    ):

        insert(
            table_name=table_name_1,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )
        insert(
            table_name=table_name_2,
            partitions=partitions,
            parts_per_partition=parts_per_partition,
            block_size=block_size,
        )

    with When("I compute expected output"):
        output1 = node.query(
            f"SELECT count(*) FROM {table_name_1} WHERE NOT(x % 2 == 0 AND id = 3)"
        ).output
        output2 = node.query(
            f"SELECT count(*) FROM {table_name_2} WHERE NOT(x % 2 == 0)"
        ).output

    with And(f"i delete half of the rows in {table_name_1}"):
        delete_odd(table_name=table_name_1, num_partitions=partitions)

    with And("I replace partition with deleted row from other table"):
        alter_replace_partition(
            table_name_1=table_name_1, table_name_2=table_name_2, partition_expr="3"
        )

    with Then("I check replace partition after delete perform correctly"):
        r = node.query(f"SELECT count(*) FROM {table_name_2}")
        assert r.output == output1, error()
        r = node.query(f"SELECT count(*) FROM {table_name_1}")
        assert r.output == output2, error()


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_AlterTableWithParts_Partitions("1.0")
)
@Name("alter after delete")
def feature(self, node="clickhouse1"):
    """Check that clickhouse supports alter operations after lightweight delete."""
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
