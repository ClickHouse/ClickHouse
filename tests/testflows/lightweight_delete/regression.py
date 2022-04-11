#!/usr/bin/env python3
import os
import sys

from testflows.core import *

append_path(sys.path, "..")

from helpers.cluster import Cluster
from helpers.argparser import argparser as argparser_base
from helpers.common import check_clickhouse_version
from lightweight_delete.requirements import *

xfails = {
    "views/materialized view": [(Fail, "not implemented")],
    "views/live view": [(Fail, "not implemented")],
    "views/window view": [(Fail, "not implemented")],
}

xflags = {}

ffails = {}


def argparser(parser):
    argparser_base(parser)
    parser.add_argument(
        "--use-alter-delete",
        action="store_true",
        help="Use alter delete instead of lightweight delete.",
    )


@TestModule
@ArgumentParser(argparser)
@XFails(xfails)
@XFlags(xflags)
@FFails(ffails)
@Name("lightweight delete")
@Specifications(SRS023_ClickHouse_Lightweight_Delete)
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_DeleteStatement("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_SupportedTableEngines("1.0"),
)
def regression(
    self,
    local,
    clickhouse_binary_path,
    use_alter_delete,
    clickhouse_version,
    stress=None,
    parallel=None,
):
    """Lightweight Delete regression."""
    nodes = {"clickhouse": ("clickhouse1", "clickhouse2", "clickhouse3")}

    self.context.clickhouse_version = clickhouse_version
    self.context.use_alter_delete = use_alter_delete

    if not use_alter_delete:
        xfail(reason="lightweight delete not yet implemented")

    with Cluster(
        local,
        clickhouse_binary_path,
        nodes=nodes,
        docker_compose_project_dir=os.path.join(
            current_dir(), os.path.basename(current_dir()) + "_env"
        ),
    ) as cluster:
        self.context.cluster = cluster
        self.context.stress = stress

        if parallel is not None:
            self.context.parallel = parallel

        if check_clickhouse_version("<22.2")(self):
            skip(reason="only supported on ClickHouse version >= 22.2")

        Feature(
            run=load(
                "lightweight_delete.tests.efficient_physical_data_removal", "feature"
            )
        )
        Feature(
            run=load("lightweight_delete.tests.multiple_delete_limitations", "feature")
        )
        Feature(run=load("lightweight_delete.tests.hard_restart", "feature"))
        Feature(run=load("lightweight_delete.tests.s3", "feature"))
        Feature(run=load("lightweight_delete.tests.lack_of_disk_space", "feature"))
        Feature(run=load("lightweight_delete.tests.multi_disk", "feature"))
        Feature(run=load("lightweight_delete.tests.projections", "feature"))
        Feature(run=load("lightweight_delete.tests.drop_empty_part", "feature"))
        Feature(run=load("lightweight_delete.tests.views", "feature"))
        Feature(run=load("lightweight_delete.tests.compatibility", "feature"))
        Feature(run=load("lightweight_delete.tests.acceptance", "feature"))
        Feature(
            run=load(
                "lightweight_delete.tests.immutable_parts_and_garbage_collection",
                "feature",
            )
        )
        Feature(
            run=load(
                "lightweight_delete.tests.replicated_tables_concurrent_deletes",
                "feature",
            )
        )
        Feature(
            run=load(
                "lightweight_delete.tests.replicated_table_with_concurrent_alter_and_delete",
                "feature",
            )
        )
        Feature(
            run=load(
                "lightweight_delete.tests.replicated_table_with_alter_after_delete_on_single_node",
                "feature",
            )
        )
        Feature(
            run=load("lightweight_delete.tests.nondeterministic_functions", "feature")
        )
        Feature(run=load("lightweight_delete.tests.performance", "feature"))
        Feature(run=load("lightweight_delete.tests.distributed_tables", "feature"))
        Feature(run=load("lightweight_delete.tests.replication_queue", "feature"))
        Feature(run=load("lightweight_delete.tests.replicated_tables", "feature"))
        Feature(
            run=load(
                "lightweight_delete.tests.delete_and_tiered_storage_ttl", "feature"
            )
        )
        Feature(
            run=load(
                "lightweight_delete.tests.alter_after_delete_with_stop_merges",
                "feature",
            )
        )
        Feature(run=load("lightweight_delete.tests.encrypted_disk", "feature"))
        Feature(run=load("lightweight_delete.tests.delete_and_column_ttl", "feature"))
        Feature(run=load("lightweight_delete.tests.alter_after_delete", "feature"))
        Feature(run=load("lightweight_delete.tests.random_concurrent_alter", "feature"))
        Feature(
            run=load("lightweight_delete.tests.concurrent_alter_and_delete", "feature")
        )
        Feature(run=load("lightweight_delete.tests.concurrent_delete", "feature"))
        Feature(run=load("lightweight_delete.tests.invalid_where_clause", "feature"))
        Feature(run=load("lightweight_delete.tests.immediate_removal", "feature"))
        Feature(run=load("lightweight_delete.tests.basic_checks", "feature"))
        Feature(run=load("lightweight_delete.tests.specific_deletes", "feature"))


if main():
    regression()
