from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from lightweight_delete.tests.alter_after_delete import (
    delete_odd,
    alter_detach_partition,
    alter_attach_partition,
    alter_freeze_partition,
    alter_drop_partition,
    alter_add_column,
    alter_clear_column,
    alter_comment_column,
    alter_unfreeze_partition,
)


@TestScenario
def detach_partition_after_delete(self, node=None):
    """Check that detach partition after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 OR id =3)"
            ).output

        with Then("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with Then("I detach third partition"):
            alter_detach_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
@Requirements()
def drop_partition_after_delete(self, node=None):
    """Check that drop partition after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 OR id = 3)"
            ).output

        with Then("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with Then("I drop third partition"):
            alter_drop_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def attach_partition_after_delete(self, node=None):
    """Check that attach partition after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0 AND id != 3)"
            ).output

        with And("I detach partition"):
            alter_detach_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

        with Then("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with Then("I attach third partition"):
            alter_attach_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def freeze_partition_after_delete(self, node=None):
    """Check that freeze partition after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
            ).output

        with Then("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with Then("I freeze third partition"):
            alter_freeze_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then(
        "I check that rows are deleted",
        description="50 rows in reach partition, 10 partitions",
    ):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def unfreeze_partition_after_delete(self, node=None):
    """Check that unfreeze partition after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
            ).output

        with Then("I freeze third partition"):
            alter_freeze_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

        with Then("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with Then("I unfreeze third partition"):
            alter_unfreeze_partition(
                table_name=table_name, partition_expr="3", node=node, settings=[]
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def add_column_after_delete(self, node=None):
    """Check that alter add column after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
            ).output

        with And("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with And("I add column", description="name=added_column type=UInt32 default=7"):
            alter_add_column(
                table_name=table_name,
                column_name="added_column",
                column_type="UInt32",
                default_expr="DEFAULT 7",
                node=node,
                settings=[],
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted and column is added"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def clear_column_after_delete(self, node=None):
    """Check that alter clear column after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
            ).output

        with And("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with And("I clear column", description="name=x"):
        alter_clear_column(
            table_name=table_name,
            column_name="x",
            partition_expr="IN PARTITION 0",
            node=node,
            settings=[],
        )

    with Then("I check that rows are deleted and column is cleared"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestScenario
def comment_column_after_delete(self, node=None):
    """Check that alter comment column after lightweight delete with STOP MERGES perform correctly."""
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    try:
        with When("I stop merges"):
            node.query("SYSTEM STOP MERGES")

        with When(
            "I insert data into the table",
            description="100 partitions 1 part in each block_size=100",
        ):
            insert(
                table_name=table_name,
                partitions=10,
                parts_per_partition=1,
                block_size=100,
            )

        with When("I compute expected output"):
            output = node.query(
                f"SELECT count(*) FROM {table_name} WHERE NOT(x % 2 == 0)"
            ).output

        with And("I delete odd rows from the table"):
            delete_odd(num_partitions=10, table_name=table_name, settings=[])

        with And("I comment column", description="name=x, type=String, DEFAULT '777'"):
            alter_comment_column(
                table_name=table_name,
                column_name="x",
                column_comment="hello",
                node=node,
                settings=[],
            )

    finally:
        with Finally("I resume merges"):
            node.query("SYSTEM START MERGES")

    with Then("I check that rows are deleted and column is commented"):
        for attempt in retries(timeout=30, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == output, error()


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_AlterTableWithParts_Partitions("1.0")
)
@Name("alter after delete with stop merges")
def feature(self, node="clickhouse1"):
    """Check that clickhouse supports alter operations after delete with STOP MERGES."""
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
