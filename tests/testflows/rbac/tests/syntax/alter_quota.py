from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *
import rbac.tests.errors as errors

@TestFeature
@Name("alter quota")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check alter quota query syntax.

    ```sql
    ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | MONTH}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
    ```
    """
    node = self.context.cluster.node(node)

    def cleanup_quota(quota):
        with Given(f"I ensure that quota {quota} does not exist"):
            node.query(f"DROP QUOTA IF EXISTS {quota}")

    try:
        with Given("I have a quota, a user, and a role"):
            node.query(f"CREATE QUOTA quota0")
            node.query(f"CREATE USER user0")
            node.query(f"CREATE ROLE role0")

        with Scenario("I alter quota with no options", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter("1.0")]):
            with When("I alter quota"):
                node.query("ALTER QUOTA quota0")

        with Scenario("I alter quota that does not exist, throws an exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter("1.0")]):
            quota = "quota1"
            cleanup_quota(quota)
            with When(f"I alter quota {quota}, which does not exist"):
                exitcode, message = errors.quota_not_found_in_disk(name=quota)
                node.query(f"ALTER QUOTA {quota}", exitcode=exitcode, message=message)
            del quota

        with Scenario("I alter quota with if exists, quota does exist", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_IfExists("1.0")]):
            node.query("ALTER QUOTA IF EXISTS quota0")

        with Scenario("I alter quota with if exists, quota does not exist", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_IfExists("1.0")]):
            quota = "quota1"
            cleanup_quota(quota)
            with When(f"I alter quota {quota}, which does not exist, with IF EXISTS"):
                node.query(f"ALTER QUOTA IF EXISTS {quota}")
            del quota

        with Scenario("I alter quota using rename, target available", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Rename("1.0")]):
            node.query("ALTER QUOTA quota0 RENAME TO quota0")

        with Scenario("I alter quota using rename, target unavailable", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Rename("1.0")]):
            new_quota = "quota1"

            try:
                with Given(f"Ensure target name {new_quota} is NOT available"):
                    node.query(f"CREATE QUOTA IF NOT EXISTS {new_quota}")

                with When(f"I try to rename to {new_quota}"):
                    exitcode, message = errors.cannot_rename_quota(name="quota0", name_new=new_quota)
                    node.query(f"ALTER QUOTA quota0 RENAME TO {new_quota}", exitcode=exitcode, message=message)
            finally:
                with Finally(f"I cleanup target name {new_quota}"):
                    node.query(f"DROP QUOTA IF EXISTS {new_quota}")

            del new_quota

        keys = ['none', 'user name', 'ip address', 'client key', 'client key or user name', 'client key or ip address']
        for key in keys:
            with Scenario(f"I alter quota keyed by {key}", flags=TE, requirements=[
                    RQ_SRS_006_RBAC_Quota_Alter_KeyedBy("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_KeyedByOptions("1.0")]):
                with When("I alter quota with a key"):
                    node.query(f"ALTER QUOTA quota0 KEYED BY '{key}'")

        with Scenario("I alter quota for randomized interval", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Interval_Randomized("1.0")]):
            with When("I alter quota on a randomized interval"):
                node.query("ALTER QUOTA quota0 FOR RANDOMIZED INTERVAL 1 DAY NO LIMITS")

        intervals = ['SECOND', 'MINUTE', 'HOUR', 'DAY', 'MONTH']
        for i, interval in enumerate(intervals):
            with Scenario(f"I alter quota for interval {interval}", flags=TE, requirements=[
                    RQ_SRS_006_RBAC_Quota_Alter_Interval("1.0")]):
                with When(f"I alter quota for {interval}"):
                    node.query(f"ALTER QUOTA quota0 FOR INTERVAL 1 {interval} NO LIMITS")

        constraints = ['MAX QUERIES', 'MAX ERRORS', 'MAX RESULT ROWS',
            'MAX RESULT BYTES', 'MAX READ ROWS', 'MAX READ BYTES', 'MAX EXECUTION TIME',
            'NO LIMITS', 'TRACKING ONLY']
        for i, constraint in enumerate(constraints):
            with Scenario(f"I alter quota for {constraint.lower()}", flags=TE, requirements=[
                    RQ_SRS_006_RBAC_Quota_Alter_Queries("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_Errors("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_ResultRows("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_ReadRows("1.0"),
                    RQ_SRS_006_RBAC_Quota_ALter_ResultBytes("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_ReadBytes("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_ExecutionTime("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_NoLimits("1.0"),
                    RQ_SRS_006_RBAC_Quota_Alter_TrackingOnly("1.0")]):
                with When("I alter quota for a constraint"):
                    node.query(f"ALTER QUOTA quota0 FOR INTERVAL 1 DAY {constraint}{' 1024' if constraint.startswith('MAX') else ''}")

        with Scenario("I create quota for multiple constraints", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Interval("1.0"),
                RQ_SRS_006_RBAC_Quota_Alter_Queries("1.0")]):
            node.query("ALTER QUOTA quota0 \
                 FOR INTERVAL 1 DAY NO LIMITS, \
                 FOR INTERVAL 2 DAY MAX QUERIES 124, \
                 FOR INTERVAL 1 MONTH TRACKING ONLY")

        with Scenario("I alter quota to assign to one role", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment("1.0")]):
            with When("I alter quota to a role"):
                node.query("ALTER QUOTA quota0 TO role0")

        with Scenario("I alter quota to assign to role that does not exist, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment("1.0")]):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(f"I alter a quota, assign to role {role}, which does not exist"):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(f"ALTER QUOTA quota0 TO {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I alter quota to assign to all except role that does not exist, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment("1.0")]):
            role = "role1"
            with Given(f"I drop {role} if it exists"):
                node.query(f"DROP ROLE IF EXISTS {role}")
            with Then(f"I alter a quota, assign to all except role {role}, which does not exist"):
                exitcode, message = errors.role_not_found_in_disk(name=role)
                node.query(f"ALTER QUOTA quota0 TO ALL EXCEPT {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I alter quota to assign to one role and one user", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment("1.0")]):
            with When("I alter quota to a role and a user"):
                node.query("ALTER QUOTA quota0 TO role0, user0")

        with Scenario("I alter quota assigned to none", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment_None("1.0")]):
            with When("I alter quota to none"):
                node.query("ALTER QUOTA quota0 TO NONE")

        with Scenario("I alter quota to assign to all", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment_All("1.0")]):
            with When("I alter quota to all"):
                node.query("ALTER QUOTA quota0 TO ALL")

        with Scenario("I alter quota to assign to all except one role", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment_Except("1.0")]):
            with When("I alter quota to all except one role"):
                node.query("ALTER QUOTA quota0 TO ALL EXCEPT role0")

        with Scenario("I alter quota to assign to all except multiple roles", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Assignment_Except("1.0")]):
            with When("I alter quota to all except one multiple roles"):
                node.query("ALTER QUOTA quota0 TO ALL EXCEPT role0, user0")

        with Scenario("I alter quota on cluster", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Cluster("1.0")]):
            try:
                with Given("I have a quota on a cluster"):
                    node.query("CREATE QUOTA quota1 ON CLUSTER sharded_cluster")

                with When("I run alter quota command on a cluster"):
                    node.query("ALTER QUOTA quota1 ON CLUSTER sharded_cluster")
                with And("I run alter quota command on a cluster with a key"):
                    node.query("ALTER QUOTA quota1 ON CLUSTER sharded_cluster KEYED BY 'none'")
                with And("I run alter quota command on a cluster with an interval"):
                    node.query("ALTER QUOTA quota1 ON CLUSTER sharded_cluster FOR INTERVAL 1 DAY TRACKING ONLY")
                with And("I run alter quota command on a cluster for all"):
                    node.query("ALTER QUOTA quota1 ON CLUSTER sharded_cluster TO ALL")
            finally:
                with Finally("I drop the quota"):
                    node.query("DROP QUOTA IF EXISTS quota1 ON CLUSTER sharded_cluster")

        with Scenario("I alter quota on nonexistent cluster, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_Quota_Alter_Cluster("1.0")]):
            with When("I run alter quota on a cluster"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("ALTER QUOTA quota0 ON CLUSTER fake_cluster", exitcode=exitcode, message=message)

    finally:
        with Finally("I drop the quota and all the users and roles"):
            node.query(f"DROP QUOTA IF EXISTS quota0")
            node.query(f"DROP USER IF EXISTS user0")
            node.query(f"DROP ROLE IF EXISTS role0")
