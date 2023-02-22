from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *


@TestFeature
@Name("create quota")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check create quota query syntax.

    ```sql
    CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(quota):
        try:
            with Given("I ensure the quota does not already exist"):
                node.query(f"DROP QUOTA IF EXISTS {quota}")
            yield
        finally:
            with Finally("I drop the quota"):
                node.query(f"DROP QUOTA IF EXISTS {quota}")

    def create_quota(quota):
        with And(f"I ensure I do have quota {quota}"):
            node.query(f"CREATE QUOTA OR REPLACE {quota}")

    try:
        with Given("I have a user and a role"):
            node.query(f"CREATE USER user0")
            node.query(f"CREATE ROLE role0")

        with Scenario(
            "I create quota with no options",
            requirements=[RQ_SRS_006_RBAC_Quota_Create("1.0")],
        ):
            with cleanup("quota0"):
                with When("I create a quota with no options"):
                    node.query("CREATE QUOTA quota0")

        with Scenario(
            "I create quota that already exists, throws exception",
            requirements=[RQ_SRS_006_RBAC_Quota_Create("1.0")],
        ):
            quota = "quota0"
            with cleanup(quota):
                create_quota(quota)
                with When(
                    f"I create a quota {quota} that already exists without IF EXISTS, throws exception"
                ):
                    exitcode, message = errors.cannot_insert_quota(name=quota)
                    node.query(
                        f"CREATE QUOTA {quota}", exitcode=exitcode, message=message
                    )
            del quota

        with Scenario(
            "I create quota if not exists, quota does not exist",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_IfNotExists("1.0")],
        ):
            quota = "quota1"
            with cleanup(quota):
                with When(f"I create a quota {quota} with if not exists"):
                    node.query(f"CREATE QUOTA IF NOT EXISTS {quota}")
            del quota

        with Scenario(
            "I create quota if not exists, quota does exist",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_IfNotExists("1.0")],
        ):
            quota = "quota1"
            with cleanup(quota):
                create_quota(quota)
                with When(f"I create a quota {quota} with if not exists"):
                    node.query(f"CREATE QUOTA IF NOT EXISTS {quota}")
            del quota

        with Scenario(
            "I create quota or replace, quota does not exist",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Replace("1.0")],
        ):
            quota = "quota2"
            with cleanup(quota):
                with When(f"I create a quota {quota} with or replace"):
                    node.query(f"CREATE QUOTA OR REPLACE {quota}")
            del quota

        with Scenario(
            "I create quota or replace, quota does exist",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Replace("1.0")],
        ):
            quota = "quota2"
            with cleanup(quota):
                create_quota(quota)
                with When(f"I create a quota {quota} with or replace"):
                    node.query(f"CREATE QUOTA OR REPLACE {quota}")
            del quota

        keys = [
            "none",
            "user name",
            "ip address",
            "client key",
            "client key or user name",
            "client key or ip address",
        ]
        for i, key in enumerate(keys):
            with Scenario(
                f"I create quota keyed by {key}",
                requirements=[
                    RQ_SRS_006_RBAC_Quota_Create_KeyedBy("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_KeyedByOptions("1.0"),
                ],
            ):
                name = f"quota{3 + i}"
                with cleanup(name):
                    with When(f"I create a quota with {key}"):
                        node.query(f"CREATE QUOTA {name} KEYED BY '{key}'")

        with Scenario(
            "I create quota for randomized interval",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Interval_Randomized("1.0")],
        ):
            with cleanup("quota9"):
                with When("I create a quota for randomized interval"):
                    node.query(
                        "CREATE QUOTA quota9 FOR RANDOMIZED INTERVAL 1 DAY NO LIMITS"
                    )

        intervals = ["SECOND", "MINUTE", "HOUR", "DAY", "MONTH"]
        for i, interval in enumerate(intervals):
            with Scenario(
                f"I create quota for interval {interval}",
                requirements=[RQ_SRS_006_RBAC_Quota_Create_Interval("1.0")],
            ):
                name = f"quota{10 + i}"
                with cleanup(name):
                    with When(f"I create a quota for {interval} interval"):
                        node.query(
                            f"CREATE QUOTA {name} FOR INTERVAL 1 {interval} NO LIMITS"
                        )

        constraints = [
            "MAX QUERIES",
            "MAX ERRORS",
            "MAX RESULT ROWS",
            "MAX RESULT BYTES",
            "MAX READ ROWS",
            "MAX READ BYTES",
            "MAX EXECUTION TIME",
            "NO LIMITS",
            "TRACKING ONLY",
        ]
        for i, constraint in enumerate(constraints):
            with Scenario(
                f"I create quota for {constraint.lower()}",
                requirements=[
                    RQ_SRS_006_RBAC_Quota_Create_Queries("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_Errors("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_ResultRows("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_ResultBytes("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_ReadRows("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_ReadBytes("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_ExecutionTime("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_NoLimits("1.0"),
                    RQ_SRS_006_RBAC_Quota_Create_TrackingOnly("1.0"),
                ],
            ):
                name = f"quota{15 + i}"
                with cleanup(name):
                    with When(f"I create quota for {constraint.lower()}"):
                        node.query(
                            f"CREATE QUOTA {name} FOR INTERVAL 1 DAY {constraint}{' 1024' if constraint.startswith('MAX') else ''}"
                        )

        with Scenario(
            "I create quota for multiple constraints",
            requirements=[
                RQ_SRS_006_RBAC_Quota_Create_Interval("1.0"),
                RQ_SRS_006_RBAC_Quota_Create_Queries("1.0"),
            ],
        ):
            with cleanup("quota23"):
                with When(f"I create quota for multiple constraints"):
                    node.query(
                        "CREATE QUOTA quota23 \
                        FOR INTERVAL 1 DAY NO LIMITS, \
                        FOR INTERVAL 2 DAY MAX QUERIES 124, \
                        FOR INTERVAL 1 HOUR TRACKING ONLY"
                    )

        with Scenario(
            "I create quota assigned to one role",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment("1.0")],
        ):
            with cleanup("quota24"):
                with When("I create quota for role"):
                    node.query("CREATE QUOTA quota24 TO role0")

        with Scenario(
            "I create quota to assign to role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(f"I create a quota, assign to role {role}, which does not exist"):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"CREATE QUOTA quota0 TO {role}", exitcode=exitcode, message=message
                )
            del role

        with Scenario(
            "I create quota to assign to all except role that does not exist, throws exception",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment("1.0")],
        ):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(
                f"I create a quota, assign to all except role {role}, which does not exist"
            ):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(
                    f"CREATE QUOTA quota0 TO ALL EXCEPT {role}",
                    exitcode=exitcode,
                    message=message,
                )
            del role

        with Scenario(
            "I create quota assigned to no role",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment_None("1.0")],
        ):
            with When("I create quota for no role"):
                node.query("CREATE QUOTA quota24 TO NONE")

        with Scenario(
            "I create quota assigned to multiple roles",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment("1.0")],
        ):
            with cleanup("quota25"):
                with When("I create quota for multiple roles"):
                    node.query("CREATE QUOTA quota25 TO role0, user0")

        with Scenario(
            "I create quota assigned to all",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment_All("1.0")],
        ):
            with cleanup("quota26"):
                with When("I create quota for all"):
                    node.query("CREATE QUOTA quota26 TO ALL")

        with Scenario(
            "I create quota assigned to all except one role",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment_Except("1.0")],
        ):
            with cleanup("quota27"):
                with When("I create quota for all except one role"):
                    node.query("CREATE QUOTA quota27 TO ALL EXCEPT role0")

        with Scenario(
            "I create quota assigned to all except multiple roles",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Assignment_Except("1.0")],
        ):
            with cleanup("quota28"):
                with When("I create quota for all except multiple roles"):
                    node.query("CREATE QUOTA quota28 TO ALL EXCEPT role0, user0")

        with Scenario(
            "I create quota on cluster",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Cluster("1.0")],
        ):
            try:
                with When("I run create quota command on cluster"):
                    node.query("CREATE QUOTA quota29 ON CLUSTER sharded_cluster")
                with When("I run create quota command on cluster, keyed"):
                    node.query(
                        "CREATE QUOTA OR REPLACE quota29 ON CLUSTER sharded_cluster KEYED BY 'none'"
                    )
                with When("I run create quota command on cluster, interval"):
                    node.query(
                        "CREATE QUOTA OR REPLACE quota29 ON CLUSTER sharded_cluster FOR INTERVAL 1 DAY TRACKING ONLY"
                    )
                with When("I run create quota command on cluster, assign"):
                    node.query(
                        "CREATE QUOTA OR REPLACE quota29 ON CLUSTER sharded_cluster TO ALL"
                    )
            finally:
                with Finally("I drop the quota from cluster"):
                    node.query(
                        "DROP QUOTA IF EXISTS quota29 ON CLUSTER sharded_cluster"
                    )

        with Scenario(
            "I create quota on nonexistent cluster, throws exception",
            requirements=[RQ_SRS_006_RBAC_Quota_Create_Cluster("1.0")],
        ):
            with When("I run create quota on a cluster"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query(
                    "CREATE QUOTA quota0 ON CLUSTER fake_cluster",
                    exitcode=exitcode,
                    message=message,
                )

    finally:
        with Finally("I drop all the users and roles"):
            node.query(f"DROP USER IF EXISTS user0")
            node.query(f"DROP ROLE IF EXISTS role0")
