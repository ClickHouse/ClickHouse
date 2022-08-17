from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestSuite
def privileges_granted_directly(self, node=None):
    """Check that a user is able to execute `CREATE ROW POLICY` with privileges are granted directly.
    """

    user_name = f"user_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):

        Suite(run=create_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[user_name,user_name]) for row in create_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestSuite
def privileges_granted_via_role(self, node=None):
    """Check that a user is able to execute `CREATE ROW POLICY` with privileges are granted through a role.
    """

    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"), role(node, f"{role_name}"):

        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")

        Suite(run=create_row_policy,
            examples=Examples("privilege grant_target_name user_name", [
                tuple(list(row)+[role_name,user_name]) for row in create_row_policy.examples
            ], args=Args(name="privilege={privilege}", format_name=True)))

@TestOutline(Suite)
@Examples("privilege",[
    ("ALL",),
    ("ACCESS MANAGEMENT",),
    ("CREATE ROW POLICY",),
    ("CREATE POLICY",),
])
def create_row_policy(self, privilege, grant_target_name, user_name, node=None):
    """Check that user is only able to execute `CREATE ROW POLICY` when they have the necessary privilege.
    """
    exitcode, message = errors.not_enough_privileges(name=user_name)

    if node is None:
        node = self.context.node

    with Scenario("CREATE ROW POLICY without privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When("I grant the user NONE privilege"):
                node.query(f"GRANT NONE TO {grant_target_name}")

            with And("I grant the user USAGE privilege"):
                node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

            with Then("I check the user can't create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

    with Scenario("CREATE ROW POLICY with privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

    with Scenario("CREATE ROW POLICY on cluster"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with Then("I check the user can create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}", settings = [("user", f"{user_name}")])

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON CLUSTER sharded_cluster ON {table_name}")

    with Scenario("CREATE ROW POLICY with revoked privilege"):
        create_row_policy_name = f"create_row_policy_{getuid()}"
        table_name = f"table_name_{getuid()}"

        try:
            with When(f"I grant {privilege}"):
                node.query(f"GRANT {privilege} ON *.* TO {grant_target_name}")

            with And(f"I revoke {privilege}"):
                node.query(f"REVOKE {privilege} ON *.* FROM {grant_target_name}")

            with Then("I check the user cannot create a row policy"):
                node.query(f"CREATE ROW POLICY {create_row_policy_name} ON {table_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {create_row_policy_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Restriction("1.0")
)
def no_grants(self, node=None):
    """Check that user is unable to select from a table without a row policy
    after a row policy with a condition has been created on that table.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name}")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

            with When("I create a row policy with a condition"):
                node.query(f"CREATE ROW POLICY OR REPLACE {pol_name} ON {table_name} FOR SELECT USING 1")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '' == output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Access_Permissive("1.0"),
)
def permissive(self, node=None):
    """Check that user is able to see from a table when they have a PERMISSIVE policy.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Access_Restrictive("1.0")
)
def restrictive(self, node=None):
    """Check that user is able to see values they have a RESTRICTIVE policy for.
    """

    table_name = f"table_{getuid()}"
    perm_pol_name = f"perm_pol_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a permissive row policy"):
                node.query(f"CREATE ROW POLICY {perm_pol_name} ON {table_name} FOR SELECT USING y=1 OR y=2 TO default")

            with And("I have a restrictive row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS RESTRICTIVE FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the restrictive row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the permissive row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {perm_pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_ForSelect("1.0"),
)
def for_select(self, node=None):
    """Check that user is able to see values allowed by the row policy condition in the FOR SELECT clause.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a restrictive row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Condition("1.0")
)
def condition(self, node=None):
    """Check that user is able to see values allowed by the row policy condition.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a restrictive row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_IfNotExists("1.0")
)
def if_not_exists(self, node=None):
    """Check that a row policy created using IF NOT EXISTS does not replace a row policy with the same name.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

            with When("I create another row policy with the same name using IF NOT EXISTS"):
                node.query(f"CREATE ROW POLICY IF NOT EXISTS {pol_name} ON {table_name}")

            with Then("I select from the table again"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Replace("1.0")
)
def or_replace(self, node=None):
    """Check that a row policy created using OR REPLACE does replace the row policy with the same name.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

            with When("I create another row policy with the same name using OR REPLACE"):
                node.query(f"CREATE ROW POLICY OR REPLACE {pol_name} ON {table_name} AS RESTRICTIVE FOR SELECT USING 1 TO default")

            with Then("I can no longer select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert output == '', error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_OnCluster("1.0")
)
def on_cluster(self, node=None):
    """Check that a row policy created using ON CLUSTER applies to the nodes of the cluster correctly.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name} FOR SELECT USING 1")

        with When("I insert some values into the table on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("I insert some values into the table on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

        with And("I select from another node on the cluster"):
            output = node2.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE {table_name} ON CLUSTER sharded_cluster")

@TestScenario
def diff_policies_on_diff_nodes(self, node=None):
    """Check that a row policy created on a node, does not effect a different node.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy on one node"):
            node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with When("I insert some values into the table on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("I insert some values into the table on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {table_name}").output
            assert '' == output, error()

        with And("I select from another node on the cluster"):
            output = node2.query(f"SELECT * FROM {table_name}").output
            assert '1' in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE {table_name} ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Assignment("1.0"),
)
def assignment(self, node=None):
    """Check that user is able to see rows from a table when they have PERMISSIVE policy assigned to them.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_None("1.0"),
)
def assignment_none(self, node=None):
    """Check that no one is affected when a row policy is assigned to NONE.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO NONE")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '' == output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_All("1.0"),
)
def assignment_all(self, node=None):
    """Check that everyone is effected with a row policy is assigned to ALL.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO ALL")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_AllExcept("1.0"),
)
def assignment_all_except(self, node=None):
    """Check that everyone is except the specified user is effect by a row policy assigned to ALL EXCEPT.
    """

    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO ALL EXCEPT default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '' == output, error()

        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def nested_view(self, node=None):
    """Check that if a user has a row policy on a table and a view is created on that table,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with And("I create a view on the table"):
                node.query(f"CREATE VIEW {view_name} AS SELECT * FROM {table_name}")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def nested_live_view_after_policy(self, node=None):
    """Check that if a user has a row policy on a table and a live view is created on that table,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I add allow_experimental_live_view to the default query settings"):
                default_query_settings = getsattr(current().context, "default_query_settings", [])
                default_query_settings.append(("allow_experimental_live_view", 1))

            with And("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with And("I create a live view on the table"):
                node.query(f"CREATE LIVE VIEW {view_name} AS SELECT * FROM {table_name}")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the live view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

            with And("I remove allow_experimental_live_view from the default query settings", flags=TE):
                if default_query_settings:
                    try:
                        default_query_settings.pop(default_query_settings.index(("allow_experimental_live_view", 1)))
                    except ValueError:
                        pass

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def nested_live_view_before_policy(self, node=None):
    """Check that if a live view exists on a table and then a row policy is created,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I add allow_experimental_live_view to the default query settings"):
                default_query_settings = getsattr(current().context, "default_query_settings", [])
                default_query_settings.append(("allow_experimental_live_view", 1))

            with And("There is a live view on the table"):
                node.query(f"CREATE LIVE VIEW {view_name} AS SELECT * FROM {table_name}")

            with And("There is a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("I insert values into the table"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the live view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

            with And("I remove allow_experimental_live_view from the default query settings", flags=TE):
                if default_query_settings:
                    try:
                        default_query_settings.pop(default_query_settings.index(("allow_experimental_live_view", 1)))
                    except ValueError:
                        pass

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def nested_mat_view_after_policy(self, node=None):
    """Check that if a user has a row policy on a table and a materialized view is created on that table,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with When("I create a view on the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}")

            with And("I insert some values on the table"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def nested_mat_view_before_policy(self, node=None):
    """Check that if a materialized view exists on a table and then a row policy is created,
    the user is only able to select rows specified by the assigned policies from the view.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a view on the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory AS SELECT * FROM {table_name}")

            with And("I have some values on the table"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with When("I create a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
def populate_mat_view(self, node=None):
    """Check that if a user has a row policy on a table and a materialized view is created using POPULATE from that table,
    the user can only select the rows from the materialized view specified in the row policy.
    """

    table_name = f"table_{getuid()}"
    view_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with table(node, table_name):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

            with And("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1),(2)")

            with When("I create a mat view with POPULATE from the table"):
                node.query(f"CREATE MATERIALIZED VIEW {view_name} ENGINE = Memory POPULATE AS SELECT * FROM {table_name}")

            with Then("I try to select from the view"):
                output = node.query(f"SELECT * FROM {view_name}").output
                assert '1' in output and '2' not in output, error()

        finally:
            with Finally("I drop the row policy", flags=TE):
                node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

            with And("I drop the materialized view", flags=TE):
                node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table(self, node=None):
    """Check that if a user has a row policy on a table and a distributed table is created on that table,
    the user is only able to select rows specified by the assigned policies from the distributed table.
    """

    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name} FOR SELECT USING 1")

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {dist_table_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with When("I insert some values into the table on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {dist_table_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table_diff_policies_on_diff_nodes(self, node=None):
    """Check that the user can only access the rows of the distributed table that are allowed
    by row policies on the the source tables. The row policies are different on different nodes.
    """

    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} FOR SELECT USING 1")

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {dist_table_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with When("I insert some values into the table on the first node"):
            node.query(f"INSERT INTO {table_name} (x) VALUES (1)")

        with And("I insert some values into the table on the second node"):
            node2.query(f"INSERT INTO {table_name} (x) VALUES (2)")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {dist_table_name}").output
            assert '2' in output and '1' not in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0")
)
def dist_table_on_dist_table(self, node=None):
    """Check that if a user has a row policy on a table and a distributed table is created on that table,
    and another distributed table is created on top of that,
    the user is only able to access rows on any of the tables specified by the assigned policies.
    """
    table_name = f"table_{getuid()}"
    dist_table_name = f"dist_table_{getuid()}"
    dist_table_2_name = f"dist_table_2_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node
        node2 = self.context.node2

    try:
        with Given("I have a table on a cluster"):
            node.query(f"CREATE TABLE {table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Memory")

        with And("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON CLUSTER sharded_cluster ON {table_name} FOR SELECT USING 1")

        with And("I have a distributed table on a cluster"):
            node.query(f"CREATE TABLE {dist_table_name} ON CLUSTER sharded_cluster (x UInt64) ENGINE = Distributed(sharded_cluster, default, {table_name}, rand())")

        with And("I have a distributed table on the other distributed table"):
            node.query(f"CREATE TABLE {dist_table_2_name} (x UInt64) ENGINE = Distributed(sharded_cluster, default, {dist_table_name}, rand())")

        with When("I insert some values into the table on the first node"):
            node.query(f"INSERT INTO {dist_table_2_name} (x) VALUES (1)")

        with Then("I select from the table"):
            output = node.query(f"SELECT * FROM {dist_table_2_name}").output
            assert '' == output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON CLUSTER sharded_cluster ON {table_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name} ON CLUSTER sharded_cluster")

        with And("I drop the distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_name} ON CLUSTER sharded_cluster")

        with And("I drop the outer distributed table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {dist_table_2_name}")

@TestScenario
def no_table(self, node=None):
    """Check that row policy is not created when the table is not specified.
    """
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    with When("I try to create a row policy without a table"):
        node.query(f"CREATE ROW POLICY {pol_name}",
            exitcode=62, message='Exception: Syntax error')

    with And("I try to create a row policy on a database"):
        node.query(f"CREATE ROW POLICY {pol_name} ON default.*",
            exitcode=62, message='Exception: Syntax error')

@TestScenario
def policy_before_table(self, node=None):
    """Check that if the policy is created before the table,
    then it is still applied correctly.
    """
    table_name = f"table_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    try:
        with Given("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING y=1 TO default")

        with table(node, table_name):
            with When("The table has some values"):
                node.query(f"INSERT INTO {table_name} (y) VALUES (1), (2)")

            with Then("I try to select from the table"):
                output = node.query(f"SELECT * FROM {table_name}").output
                assert '1' in output and '2' not in output, error()

    finally:
        with Finally("I drop the row policy"):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RowPolicy_Nesting("1.0"),
)
def dict(self, node=None):
    """Check that if a user has a row policy on a table and a dictionary is created on that table,
    the user is only able to select rows specified by the assigned policies from the dict.
    """

    table_name = f"table_{getuid()}"
    dict_name = f"view_{getuid()}"
    pol_name = f"pol_{getuid()}"

    if node is None:
        node = self.context.node

    try:
        with Given("I have a row policy"):
            node.query(f"CREATE ROW POLICY {pol_name} ON {table_name} AS PERMISSIVE FOR SELECT USING key=1 TO default")

        with And("I have a table"):
            node.query(f"CREATE TABLE {table_name} (key UInt64, val UInt64 DEFAULT 5) ENGINE = Memory")

        with When("The table has some values"):
            node.query(f"INSERT INTO {table_name} (key) VALUES (1),(2)")

        with And("I create a dict on the table"):
            node.query(f"CREATE DICTIONARY {dict_name} (key UInt64 DEFAULT 0, val UInt64 DEFAULT 5) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE {table_name} PASSWORD '' DB 'default')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())")

        with Then("I try to select from the dict"):
            output = node.query(f"SELECT * FROM {dict_name}").output
            assert '1' in output and '2' not in output, error()

    finally:
        with Finally("I drop the row policy", flags=TE):
            node.query(f"DROP ROW POLICY IF EXISTS {pol_name} ON {table_name}")

        with And("I drop the materialized view", flags=TE):
            node.query(f"DROP DICTIONARY IF EXISTS {dict_name}")

        with And("I drop the table", flags=TE):
            node.query(f"DROP TABLE IF EXISTS {table_name}")

@TestFeature
@Name("create row policy")
@Requirements(
    RQ_SRS_006_RBAC_Privileges_CreateRowPolicy("1.0"),
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of CREATE ROW POLICY.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.node2 = self.context.cluster.node("clickhouse2")

    Suite(run=privileges_granted_directly, setup=instrument_clickhouse_server_log)
    Suite(run=privileges_granted_via_role, setup=instrument_clickhouse_server_log)

    Scenario(run=no_grants, setup=instrument_clickhouse_server_log)
    Scenario(run=permissive, setup=instrument_clickhouse_server_log)
    Scenario(run=restrictive, setup=instrument_clickhouse_server_log)
    Scenario(run=for_select, setup=instrument_clickhouse_server_log)
    Scenario(run=condition, setup=instrument_clickhouse_server_log)
    Scenario(run=if_not_exists, setup=instrument_clickhouse_server_log)
    Scenario(run=or_replace, setup=instrument_clickhouse_server_log)
    Scenario(run=on_cluster, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_none, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_all, setup=instrument_clickhouse_server_log)
    Scenario(run=assignment_all_except, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_view, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_live_view_before_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_live_view_after_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_mat_view_before_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=nested_mat_view_after_policy, setup=instrument_clickhouse_server_log)
    Scenario(run=populate_mat_view, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table_on_dist_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dist_table_diff_policies_on_diff_nodes, setup=instrument_clickhouse_server_log)
    Scenario(run=diff_policies_on_diff_nodes, setup=instrument_clickhouse_server_log)
    Scenario(run=no_table, setup=instrument_clickhouse_server_log)
    Scenario(run=policy_before_table, setup=instrument_clickhouse_server_log)
    Scenario(run=dict, setup=instrument_clickhouse_server_log)
