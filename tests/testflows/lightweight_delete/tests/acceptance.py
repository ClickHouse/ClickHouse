from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_Acceptance_OnTimeDataset_ConcurrentSelectsAndDeletes(
        "1.0"
    )
)
def acceptance_concurrent_select_delete_execution_time(self, delete_query, node=None):
    """Check that clickhouse keeps reference dataset table usable while the delete reference queries
    are being executed concurrently with the select reference queries.
    No major degradation in query response time SHALL be seen.
    """
    if node is None:
        node = self.context.node

    with Given("I create ontime table"):
        add_ontime_table()

    with When("I restart server"):
        node.restart_clickhouse()

    with When("I perform select query and time it"):
        start_time = time.time()
        select_query_ontime()
        execution_time = time.time() - start_time

    metric("execution_time", execution_time, "s")

    start_time = time.time()
    with Then("I perform concurrent select and delete"):
        Step(name="I perform select operation", test=delete_query, parallel=True)()
        Step(name="I perform delete operation", test=delete_query, parallel=True)()
    execution_time_parallel = time.time() - start_time

    metric("execution_time_parallel", execution_time_parallel, "s")

    with Then("I check execution time does not change a lot"):
        assert (
            1000 * execution_time > execution_time_parallel
        ), error()  # todo rewrite after implementation


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_Acceptance_OnTimeDataset_ConcurrentInsertsAndDeletes(
        "1.0"
    ),
)
def acceptance_concurrent_insert_delete_execution_time(self, delete_query, node=None):
    """Check that clickhouse is not slow down or lockup data ingestion into the reference dataset table
    when delete reference queries are executed concurrently with the insert reference queries.
    """
    if node is None:
        node = self.context.node

    with Given("I create ontime table"):
        add_ontime_table()

    with When("I restart server"):
        node.restart_clickhouse()

    with When("I perform insert query and time it"):
        start_time = time.time()
        insert_query_ontime()
        execution_time = time.time() - start_time

    metric("execution_time", execution_time, "s")

    start_time = time.time()
    with Then("I perform concurrent select and delete"):
        Step(
            name="I perform insert operation", test=insert_query_ontime, parallel=True
        )()
        Step(name="I perform delete operation", test=delete_query, parallel=True)()
    execution_time_parallel = time.time() - start_time

    metric("execution_time_parallel", execution_time_parallel, "s")

    with Then("I check execution time does not change a lot"):
        assert (
            1000 * execution_time > execution_time_parallel
        ), error()  # todo rewrite after implementation


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_Acceptance_OnTimeDataset_DeleteQueryExecutionTime(
        "1.0"
    )
)
def acceptance_delete_execution_time(self, delete_query, node=None):
    """Check that clickhouse execute each query in the delete reference queries
    set against the reference dataset table within 2 sec.
    """

    if node is None:
        node = self.context.node

    with Given("I create ontime table"):
        add_ontime_table()

    with When("I restart server"):
        node.restart_clickhouse()

    with When("I perform delete operation"):
        start_time = time.time()
        delete_query()
        execution_time = time.time() - start_time

    metric("execution_time", execution_time, "s")

    with Then("I check delete execution time is less than 2 seconds"):
        assert execution_time < 1000, error()  # todo rewrite after implementation


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_Acceptance_OnTimeDataset_Inserts(
        "1.0"
    ),
)
def acceptance_insert_execution_time_after_delete(self, delete_query, node=None):
    """Check that clickhouse execute each query in the delete reference queries
    set against the reference dataset table within 2 sec.
    """

    if node is None:
        node = self.context.node

    with Given("I create ontime table"):
        add_ontime_table()

    with When("I restart server"):
        node.restart_clickhouse()

    with When("I perform insert query and time it"):
        start_time = time.time()
        insert_query_ontime()
        execution_time = time.time() - start_time

    metric("execution_time", execution_time, "s")

    with When("I perform delete operation"):
        delete_query()

    with When("I perform insert query on table with deleted rows and time it"):
        start_time = time.time()
        insert_query_ontime()
        execution_time_after_delete = time.time() - start_time

    metric("execution_time_after_delete", execution_time_after_delete, "s")

    with Then("I check execution time does not change a lot"):
        assert (
            execution_time * 1000 > execution_time_after_delete
        ), error()  # todo rewrite after implementation


@TestFeature
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_Performance_ConcurrentQueries("1.0")
)
@Name("acceptance")
def feature(self, node="clickhouse1"):
    """Check that clickhouse works right with delete operations applied to ontime dataset."""
    self.context.node = self.context.cluster.node(node)

    for (i, delete_query) in enumerate(
        [
            delete_query_1_ontime,
            delete_query_2_ontime,
            delete_query_3_ontime,
            delete_query_4_ontime,
            delete_query_5_ontime,
        ]
    ):

        with Feature(f"delete_query_{i}"):
            acceptance_concurrent_select_delete_execution_time(
                delete_query=delete_query
            )
            acceptance_concurrent_insert_delete_execution_time(
                delete_query=delete_query
            )
            acceptance_delete_execution_time(delete_query=delete_query)
            acceptance_insert_execution_time_after_delete(delete_query=delete_query)
