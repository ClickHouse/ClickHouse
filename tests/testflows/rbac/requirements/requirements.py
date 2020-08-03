# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.200723.1011705.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Requirement

RQ_SRS_006_RBAC = Requirement(
        name='RQ.SRS-006.RBAC',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support role based access control.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Login = Requirement(
        name='RQ.SRS-006.RBAC.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL only allow access to the server for a given\n'
        'user only when correct username and password are used during\n'
        'the connection to the server.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Login_DefaultUser = Requirement(
        name='RQ.SRS-006.RBAC.Login.DefaultUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the **default user** when no username and password\n'
        'are specified during the connection to the server.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User = Requirement(
        name='RQ.SRS-006.RBAC.User',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creation and manipulation of\n'
        'one or more **user** accounts to which roles, privileges,\n'
        'settings profile, quotas and row policies can be assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Roles = Requirement(
        name='RQ.SRS-006.RBAC.User.Roles',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **roles**\n'
        'to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Privileges = Requirement(
        name='RQ.SRS-006.RBAC.User.Privileges',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more privileges to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Variables = Requirement(
        name='RQ.SRS-006.RBAC.User.Variables',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more variables to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Variables_Constraints = Requirement(
        name='RQ.SRS-006.RBAC.User.Variables.Constraints',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning min, max and read-only constraints\n'
        'for the variables that can be set and read by the **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_SettingsProfile = Requirement(
        name='RQ.SRS-006.RBAC.User.SettingsProfile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **settings profiles**\n'
        'to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Quotas = Requirement(
        name='RQ.SRS-006.RBAC.User.Quotas',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **quotas** to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_RowPolicies = Requirement(
        name='RQ.SRS-006.RBAC.User.RowPolicies',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **row policies** to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_AccountLock = Requirement(
        name='RQ.SRS-006.RBAC.User.AccountLock',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support locking and unlocking of **user** accounts.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_AccountLock_DenyAccess = Requirement(
        name='RQ.SRS-006.RBAC.User.AccountLock.DenyAccess',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL deny access to the user whose account is locked.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_DefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.User.DefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning a default role to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_RoleSelection = Requirement(
        name='RQ.SRS-006.RBAC.User.RoleSelection',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support selection of one or more **roles** from the available roles\n'
        'that are assigned to a **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_ShowCreate = Requirement(
        name='RQ.SRS-006.RBAC.User.ShowCreate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the command of how **user** account was created.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_ShowPrivileges = Requirement(
        name='RQ.SRS-006.RBAC.User.ShowPrivileges',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support listing the privileges of the **user**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role = Requirement(
        name='RQ.SRS-006.RBAC.Role',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClikHouse] SHALL support creation and manipulation of **roles**\n'
        'to which privileges, settings profile, quotas and row policies can be\n'
        'assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Privileges = Requirement(
        name='RQ.SRS-006.RBAC.Role.Privileges',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more privileges to a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Variables = Requirement(
        name='RQ.SRS-006.RBAC.Role.Variables',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more variables to a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_SettingsProfile = Requirement(
        name='RQ.SRS-006.RBAC.Role.SettingsProfile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **settings profiles**\n'
        'to a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Quotas = Requirement(
        name='RQ.SRS-006.RBAC.Role.Quotas',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **quotas** to a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_RowPolicies = Requirement(
        name='RQ.SRS-006.RBAC.Role.RowPolicies',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning one or more **row policies** to a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Usage = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Usage',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **usage** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Select = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Select',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **select** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_SelectColumns = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.SelectColumns',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **select columns** privilege\n'
        'for a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Insert = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Insert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **insert** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Delete = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Delete',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **delete** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Alter = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **alter** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Create = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **create** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_Drop = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **drop** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_All = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL include in the **all** privilege the same rights\n'
        'as provided by **usage**, **select**, **select columns**,\n'
        '**insert**, **delete**, **alter**, **create**, and **drop** privileges.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_All_GrantRevoke = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.All.GrantRevoke',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **all** privileges\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_GrantOption = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.GrantOption',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **grant option** privilege\n'
        'for a database or a specific table to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Privileges_AdminOption = Requirement(
        name='RQ.SRS-006.RBAC.Privileges.AdminOption',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting or revoking **admin option** privilege\n'
        'to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Insert = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Insert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `INSERT INTO` statements\n'
        'to be executed unless the user has the **insert** privilege for the destination table\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Select = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Select',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `SELECT` statements\n'
        'to be executed unless the user has the **select** or **select columns** privilege\n'
        'for the destination table either because of the explicit grant\n'
        'or through one of the roles assigned to the user.\n'
        'If the the user only has the **select columns**\n'
        'privilege then only the specified columns SHALL be available for reading.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Create = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `CREATE` statements\n'
        'to be executed unless the user has the **create** privilege for the destination database\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Alter = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `ALTER` statements\n'
        'to be executed unless the user has the **alter** privilege for the destination table\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Drop = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `DROP` statements\n'
        'to be executed unless the user has the **drop** privilege for the destination database\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Drop_Table = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Drop.Table',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `DROP TABLE` statements\n'
        'to be executed unless the user has the **drop** privilege for the destination database or the table\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_GrantRevoke = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.GrantRevoke',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any `GRANT` or `REVOKE` statements\n'
        'to be executed unless the user has the **grant option** privilege\n'
        'for the privilege of the destination table\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Use = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Use',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow the `USE` statement to be executed\n'
        'unless the user has at least one of the privileges for the database\n'
        'or the table inside that database\n'
        'either because of the explicit grant or through one of the roles assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RequiredPrivileges_Admin = Requirement(
        name='RQ.SRS-006.RBAC.RequiredPrivileges.Admin',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL not allow any of the following statements\n'
        '\n'
        '* `SYSTEM`\n'
        '* `SHOW`\n'
        '* `ATTACH`\n'
        '* `CHECK TABLE`\n'
        '* `DESCRIBE TABLE`\n'
        '* `DETACH`\n'
        '* `EXISTS`\n'
        '* `KILL QUERY`\n'
        '* `KILL MUTATION`\n'
        '* `OPTIMIZE`\n'
        '* `RENAME`\n'
        '* `TRUNCATE`\n'
        '\n'
        'to be executed unless the user has the **admin option** privilege\n'
        'through one of the roles with **admin option** privilege assigned to the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_PartialRevokes = Requirement(
        name='RQ.SRS-006.RBAC.PartialRevokes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support partial revoking of privileges granted\n'
        'to a **user** or a **role**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creation and manipulation of **settings profiles**\n'
        'that can include value definition for one or more variables and can\n'
        'can be assigned to one or more **users** or **roles**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Constraints = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Constraints',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning min, max and read-only constraints\n'
        'for the variables specified in the **settings profile**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_ShowCreate = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.ShowCreate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the command of how **setting profile** was created.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas = Requirement(
        name='RQ.SRS-006.RBAC.Quotas',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creation and manipulation of **quotas**\n'
        'that can be used to limit resource usage by a **user** or a **role**\n'
        'over a period of time.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_Keyed = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.Keyed',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating **quotas** that are keyed\n'
        'so that a quota is tracked separately for each key value.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_Queries = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.Queries',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **queries** quota to limit the total number of requests.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_Errors = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.Errors',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **errors** quota to limit the number of queries that threw an exception.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ResultRows = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ResultRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **result rows** quota to limit the\n'
        'the total number of rows given as the result.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ReadRows = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ReadRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **read rows** quota to limit the total\n'
        'number of source rows read from tables for running the query on all remote servers.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ResultBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ResultBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **result bytes** quota to limit the total number\n'
        'of bytes that can be returned as the result.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ReadBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ReadBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **read bytes** quota to limit the total number\n'
        'of source bytes read from tables for running the query on all remote servers.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ExecutionTime = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ExecutionTime',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting **execution time** quota to limit the maximum\n'
        'query execution time.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quotas_ShowCreate = Requirement(
        name='RQ.SRS-006.RBAC.Quotas.ShowCreate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the command of how **quota** was created.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creation and manipulation of table **row policies**\n'
        'that can be used to limit access to the table contents for a **user** or a **role**\n'
        'using a specified **condition**.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Condition = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Condition',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support row policy **conditions** that can be any SQL\n'
        'expression that returns a boolean.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowCreate = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowCreate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the command of how **row policy** was created.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Use_DefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.User.Use.DefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL by default use default role or roles assigned\n'
        'to the user if specified.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Use_AllRolesWhenNoDefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL by default use all the roles assigned to the user\n'
        'if no default role or roles are specified for the user.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create = Requirement(
        name='RQ.SRS-006.RBAC.User.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating **user** accounts using `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_IfNotExists = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.IfNotExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE USER` statement\n'
        'to skip raising an exception if a user with the same **name** already exists.\n'
        'If the `IF NOT EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a user with the same **name** already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Replace = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Replace',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE USER` statement\n'
        'to replace existing user account if already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_NoPassword = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.NoPassword',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying no password when creating\n'
        'user account using `IDENTIFIED WITH NO_PASSWORD` clause .\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_NoPassword_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.NoPassword.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use no password for the user when connecting to the server\n'
        'when an account was created with `IDENTIFIED WITH NO_PASSWORD` clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_PlainText = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying plaintext password when creating\n'
        'user account using `IDENTIFIED WITH PLAINTEXT_PASSWORD BY` clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_PlainText_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.PlainText.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the plaintext password passed by the user when connecting to the server\n'
        'when an account was created with `IDENTIFIED WITH PLAINTEXT_PASSWORD` clause\n'
        'and compare the password with the one used in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_Sha256Password = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.Sha256Password',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying SHA256\n'
        'to some password when creating user account using `IDENTIFIED WITH SHA256_PASSWORD BY` or `IDENTIFIED BY`\n'
        'clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_Sha256Password_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.Sha256Password.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL calculate `SHA256` of the password passed by the user when connecting to the server\n'
        "when an account was created with `IDENTIFIED WITH SHA256_PASSWORD` or with 'IDENTIFIED BY' clause\n"
        'and compare the calculated hash to the one used in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_Sha256Hash = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying SHA256\n'
        'to some already calculated hash when creating user account using `IDENTIFIED WITH SHA256_HASH`\n'
        'clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_Sha256Hash_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL calculate `SHA256` of the already calculated hash passed by\n'
        'the user when connecting to the server\n'
        'when an account was created with `IDENTIFIED WITH SHA256_HASH` clause\n'
        'and compare the calculated hash to the one used in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Password = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying SHA1 two times\n'
        'to a password when creating user account using `IDENTIFIED WITH DOUBLE_SHA1_PASSWORD`\n'
        'clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Password_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL calculate `SHA1` two times over the password passed by\n'
        'the user when connecting to the server\n'
        'when an account was created with `IDENTIFIED WITH DOUBLE_SHA1_PASSWORD` clause\n'
        'and compare the calculated value to the one used in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Hash = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying SHA1 two times\n'
        'to a hash when creating user account using `IDENTIFIED WITH DOUBLE_SHA1_HASH`\n'
        'clause.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Password_DoubleSha1Hash_Login = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash.Login',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL calculate `SHA1` two times over the hash passed by\n'
        'the user when connecting to the server\n'
        'when an account was created with `IDENTIFIED WITH DOUBLE_SHA1_HASH` clause\n'
        'and compare the calculated value to the one used in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Name = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Name',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more hostnames from\n'
        'which user can access the server using the `HOST NAME` clause\n'
        'in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Regexp = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Regexp',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more regular expressions\n'
        'to match hostnames from which user can access the server\n'
        'using the `HOST REGEXP` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_IP = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.IP',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more IP address or subnet from\n'
        'which user can access the server using the `HOST IP` clause in the\n'
        '`CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Any = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Any',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `HOST ANY` clause in the `CREATE USER` statement\n'
        'to indicate that user can access the server from any host.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_None = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support fobidding access from any host using `HOST NONE` clause in the\n'
        '`CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Local = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Local',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting user access to local only using `HOST LOCAL` clause in the\n'
        '`CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Like = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Like',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying host using `LIKE` command syntax using the\n'
        '`HOST LIKE` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Host_Default = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Host.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support user access to server from any host\n'
        'if no `HOST` clause is specified in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_DefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.DefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more default roles\n'
        'using `DEFAULT ROLE` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_DefaultRole_None = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.DefaultRole.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying no default roles\n'
        'using `DEFAULT ROLE NONE` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_DefaultRole_All = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.DefaultRole.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying all roles to be used as default\n'
        'using `DEFAULT ROLE ALL` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Settings = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Settings',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying settings and profile\n'
        'using `SETTINGS` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying cluster on which the user\n'
        'will be created using `ON CLUSTER` clause in the `CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Create_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.User.Create.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for `CREATE USER` statement.\n'
        '\n'
        '```sql\n'
        'CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]\n'
        "    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]\n"
        "    [HOST {LOCAL | NAME 'name' | NAME REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]\n"
        '    [DEFAULT ROLE role [,...]]\n'
        "    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]\n"
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering **user** accounts using `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_OrderOfEvaluation = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.OrderOfEvaluation',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support evaluating `ALTER USER` statement from left to right\n'
        'where things defined on the right override anything that was previously defined on\n'
        'the left.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER USER` statement\n'
        'to skip raising an exception (producing a warning instead) if a user with the specified **name** does not exist. If the `IF EXISTS` clause is not specified then an exception SHALL be raised if a user with the **name** does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the cluster the user is on\n'
        'when altering user account using `ON CLUSTER` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Rename = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Rename',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying a new name for the user when\n'
        'altering user account using `RENAME` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Password_PlainText = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Password.PlainText',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying plaintext password when altering\n'
        'user account using `IDENTIFIED WITH PLAINTEXT_PASSWORD BY` or\n'
        'using shorthand `IDENTIFIED BY` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Password_Sha256Password = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Password.Sha256Password',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying SHA256\n'
        'to some password as identification when altering user account using\n'
        '`IDENTIFIED WITH SHA256_PASSWORD` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Password_DoubleSha1Password = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Password.DoubleSha1Password',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the result of applying Double SHA1\n'
        'to some password as identification when altering user account using\n'
        '`IDENTIFIED WITH DOUBLE_SHA1_PASSWORD` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_AddDrop = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.AddDrop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering user by adding and dropping access to hosts with the `ADD HOST` or the `DROP HOST`in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_Local = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.Local',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting user access to local only using `HOST LOCAL` clause in the\n'
        '`ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_Name = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.Name',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more hostnames from\n'
        'which user can access the server using the `HOST NAME` clause\n'
        'in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_Regexp = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.Regexp',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more regular expressions\n'
        'to match hostnames from which user can access the server\n'
        'using the `HOST REGEXP` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_IP = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.IP',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more IP address or subnet from\n'
        'which user can access the server using the `HOST IP` clause in the\n'
        '`ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_Like = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.Like',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying sone or more similar hosts using `LIKE` command syntax using the `HOST LIKE` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_Any = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.Any',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying `HOST ANY` clause in the `ALTER USER` statement\n'
        'to indicate that user can access the server from any host.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Host_None = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Host.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support fobidding access from any host using `HOST NONE` clause in the\n'
        '`ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_DefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.DefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more default roles\n'
        'using `DEFAULT ROLE` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_DefaultRole_All = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.DefaultRole.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying all roles to be used as default\n'
        'using `DEFAULT ROLE ALL` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_DefaultRole_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.DefaultRole.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more roles which will not be used as default\n'
        'using `DEFAULT ROLE ALL EXCEPT` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Settings = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Settings',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying one or more variables\n'
        'using `SETTINGS` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Settings_Min = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Settings.Min',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying a minimum value for the variable specifed using `SETTINGS` with `MIN` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Settings_Max = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Settings.Max',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying a maximum value for the variable specifed using `SETTINGS` with `MAX` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Settings_Profile = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Settings.Profile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying the name of a profile for the variable specifed using `SETTINGS` with `PROFILE` clause in the `ALTER USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Alter_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.User.Alter.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `ALTER USER` statement.\n'
        '\n'
        '```sql\n'
        'ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]\n'
        '    [RENAME TO new_name]\n'
        "    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]\n"
        "    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]\n"
        '    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]\n'
        "    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]\n"
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting or changing granted roles to default for one or more\n'
        'users using `SET DEFAULT ROLE` statement which\n'
        'SHALL permanently change the default roles for the user or users if successful.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole_CurrentUser = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting or changing granted roles to default for\n'
        'the current user using `CURRENT_USER` clause in the `SET DEFAULT ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole_All = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting or changing all granted roles to default\n'
        'for one or more users using `ALL` clause in the `SET DEFAULT ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting or changing all granted roles except those specified\n'
        'to default for one or more users using `ALL EXCEPT` clause in the `SET DEFAULT ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole_None = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing all granted roles from default\n'
        'for one or more users using `NONE` clause in the `SET DEFAULT ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetDefaultRole_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.SetDefaultRole.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `SET DEFAULT ROLE` statement.\n'
        '\n'
        '```sql\n'
        'SET DEFAULT ROLE\n'
        '    {NONE | role [,...] | ALL | ALL EXCEPT role [,...]}\n'
        '    TO {user|CURRENT_USER} [,...]\n'
        '\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole = Requirement(
        name='RQ.SRS-006.RBAC.SetRole',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support activating role or roles for the current user\n'
        'using `SET ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole_Default = Requirement(
        name='RQ.SRS-006.RBAC.SetRole.Default',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support activating default roles for the current user\n'
        'using `DEFAULT` clause in the `SET ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole_None = Requirement(
        name='RQ.SRS-006.RBAC.SetRole.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support activating no roles for the current user\n'
        'using `NONE` clause in the `SET ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole_All = Requirement(
        name='RQ.SRS-006.RBAC.SetRole.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support activating all roles for the current user\n'
        'using `ALL` clause in the `SET ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.SetRole.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support activating all roles except those specified\n'
        'for the current user using `ALL EXCEPT` clause in the `SET ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SetRole_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.SetRole.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '```sql\n'
        'SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_ShowCreateUser = Requirement(
        name='RQ.SRS-006.RBAC.User.ShowCreateUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE USER` statement used to create the current user object\n'
        'using the `SHOW CREATE USER` statement with `CURRENT_USER` or no argument.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_ShowCreateUser_For = Requirement(
        name='RQ.SRS-006.RBAC.User.ShowCreateUser.For',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE USER` statement used to create the specified user object\n'
        'using the `FOR` clause in the `SHOW CREATE USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_ShowCreateUser_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.User.ShowCreateUser.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the following syntax for `SHOW CREATE USER` statement.\n'
        '\n'
        '```sql\n'
        'SHOW CREATE USER [name | CURRENT_USER]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Drop = Requirement(
        name='RQ.SRS-006.RBAC.User.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing a user account using `DROP USER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Drop_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.User.Drop.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP USER` statement\n'
        'to skip raising an exception if the user account does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a user does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Drop_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.User.Drop.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP USER` statement\n'
        'to specify the name of the cluster the user should be dropped from.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_User_Drop_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.User.Drop.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for `DROP USER` statement\n'
        '\n'
        '```sql\n'
        'DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating a **role** using `CREATE ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create_IfNotExists = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create.IfNotExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE ROLE` statement\n'
        'to raising an exception if a role with the same **name** already exists.\n'
        'If the `IF NOT EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a role with the same **name** already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create_Replace = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create.Replace',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE ROLE` statement\n'
        'to replace existing role if it already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create_Settings = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create.Settings',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying settings and profile using `SETTINGS`\n'
        'clause in the `CREATE ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `CREATE ROLE` statement\n'
        '\n'
        '``` sql\n'
        'CREATE ROLE [IF NOT EXISTS | OR REPLACE] name\n'
        "    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]\n"
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Role.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL make the role available to be linked with users, privileges, quotas and\n'
        'settings profiles after the successful execution of the `CREATE ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering one **role** using `ALTER ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering one **role** using `ALTER ROLE IF EXISTS` statement, where no exception\n'
        'will be thrown if the role does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering one **role** using `ALTER ROLE role ON CLUSTER` statement to specify the\n'
        'cluster location of the specified role.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_Rename = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.Rename',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering one **role** using `ALTER ROLE role RENAME TO` statement which renames the\n'
        'role to a specified new name. If the new name already exists, that an exception SHALL be raised unless the\n'
        '`IF EXISTS` clause is specified, by which no exception will be raised and nothing will change.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_Settings = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.Settings',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the settings of one **role** using `ALTER ROLE role SETTINGS ...` statement.\n'
        'Altering variable values, creating max and min values, specifying readonly or writable, and specifying the\n'
        'profiles for which this alter change shall be applied to, are all supported, using the following syntax.\n'
        '\n'
        '```sql\n'
        "[SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]\n"
        '```\n'
        '\n'
        'One or more variables and profiles may be specified as shown above.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL alter the abilities granted by the role\n'
        'from all the users to which the role was assigned after the successful execution\n'
        'of the `ALTER ROLE` statement. Operations in progress SHALL be allowed to complete as is, but any new operation that requires the privileges that not otherwise granted to the user SHALL fail.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Alter_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Role.Alter.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '```sql\n'
        'ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]\n'
        '    [RENAME TO new_name]\n'
        "    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]\n"
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Drop = Requirement(
        name='RQ.SRS-006.RBAC.Role.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing one or more roles using `DROP ROLE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Drop_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.Role.Drop.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP ROLE` statement\n'
        'to skip raising an exception if the role does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a role does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Drop_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Role.Drop.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP ROLE` statement to specify the cluster from which to drop the specified role.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Role.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the abilities granted by the role\n'
        'from all the users to which the role was assigned after the successful execution\n'
        'of the `DROP ROLE` statement. Operations in progress SHALL be allowed to complete\n'
        'but any new operation that requires the privileges that not otherwise granted to\n'
        'the user SHALL fail.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_Drop_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Role.Drop.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `DROP ROLE` statement\n'
        '\n'
        '``` sql\n'
        'DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_ShowCreate = Requirement(
        name='RQ.SRS-006.RBAC.Role.ShowCreate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support viewing the settings for a role upon creation with the `SHOW CREATE ROLE`\n'
        'statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Role_ShowCreate_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Role.ShowCreate.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `SHOW CREATE ROLE` command.\n'
        '\n'
        '```sql\n'
        'SHOW CREATE ROLE name\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_To = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.To',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting privileges to one or more users or roles using `TO` clause\n'
        'in the `GRANT PRIVILEGE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_To_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.To.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL grant privileges to any set of users and/or roles specified in the `TO` clause of the grant statement.\n'
        'Any new operation by one of the specified users or roles with the granted privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_ToCurrentUser = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting privileges to current user using `TO CURRENT_USER` clause\n'
        'in the `GRANT PRIVILEGE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Select = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Select',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **select** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT SELECT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Select.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **select** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT SELECT` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **select** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_SelectColumns = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **select columns** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT SELECT(columns)` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_SelectColumns_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **select columns** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT SELECT(columns)` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **select columns** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Insert = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Insert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **insert** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT INSERT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Insert_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Insert.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **insert** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT INSERT` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **insert** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Alter = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **alter** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT ALTER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **alter** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT ALTER` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **alter** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Create = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **create** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT CREATE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **create** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT CREATE` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **create** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Drop = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **drop** privilege to one or more users or roles\n'
        'for a database or a table using the `GRANT DROP` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **drop** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT DROP` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **drop** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Truncate = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Truncate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **truncate** privilege to one or more users or roles\n'
        'for a database or a table using `GRANT TRUNCATE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Truncate_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Truncate.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **truncate** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT TRUNCATE` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **truncate** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Optimize = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Optimize',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **optimize** privilege to one or more users or roles\n'
        'for a database or a table using `GRANT OPTIMIZE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Optimize_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Optimize.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **optimize** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT OPTIMIZE` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **optimize** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Show = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Show',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **show** privilege to one or more users or roles\n'
        'for a database or a table using `GRANT SHOW` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Show_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Show.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **show** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT SHOW` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **show** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_KillQuery = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.KillQuery',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **kill query** privilege to one or more users or roles\n'
        'for a database or a table using `GRANT KILL QUERY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_KillQuery_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.KillQuery.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **kill query** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT KILL QUERY` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **kill query** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_AccessManagement = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **access management** privileges to one or more users or roles\n'
        'for a database or a table using `GRANT ACCESS MANAGEMENT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_AccessManagement_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **access management** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT ACCESS MANAGEMENT` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **access management** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_System = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.System',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **system** privileges to one or more users or roles\n'
        'for a database or a table using `GRANT SYSTEM` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_System_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.System.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **system** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT SYSTEM` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **system** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Introspection = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Introspection',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **introspection** privileges to one or more users or roles\n'
        'for a database or a table using `GRANT INTROSPECTION` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Introspection_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Introspection.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **introspection** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT INTROSPECTION` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **introspection** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Sources = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Sources',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **sources** privileges to one or more users or roles\n'
        'for a database or a table using `GRANT SOURCES` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Sources_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Sources.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **sources** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT SOURCES` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **sources** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_DictGet = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.DictGet',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **dictGet** privilege to one or more users or roles\n'
        'for a database or a table using `GRANT dictGet` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_DictGet_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.DictGet.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **dictGet** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT dictGet` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires the **dictGet** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_None = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting no privileges to one or more users or roles\n'
        'for a database or a table using `GRANT NONE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_None_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.None.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add no privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT NONE` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires no privileges SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_All = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **all** privileges to one or more users or roles\n'
        'for a database or a table using the `GRANT ALL` or `GRANT ALL PRIVILEGES` statements.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_All_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.All.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **all** privileges to the specified users or roles\n'
        'after the successful execution of the `GRANT ALL` or `GRANT ALL PRIVILEGES` statement.\n'
        'Any new operation by a user or a user that has the specified role\n'
        'which requires one or more privileges that are part of the **all**\n'
        'privileges SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_GrantOption = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.GrantOption',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the **grant option** privilege to one or more users or roles\n'
        'for a database or a table using the `WITH GRANT OPTION` clause in the `GRANT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_GrantOption_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.GrantOption.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **grant option** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT` statement with the `WITH GRANT OPTION` clause\n'
        'for the privilege that was specified in the statement.\n'
        'Any new `GRANT` statements executed by a user or a user that has the specified role\n'
        'which requires **grant option** for the privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_On = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `ON` clause in the `GRANT` privilege statement\n'
        'which SHALL allow to specify one or more tables to which the privilege SHALL\n'
        'be granted using the following patterns\n'
        '\n'
        '* `*.*` any table in any database\n'
        '* `database.*` any table in the specified database\n'
        '* `database.table` specific table in the specified database\n'
        '* `*` any table in the current database\n'
        '* `table` specific table in the current database\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_On_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.On.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL grant privilege on a table specified in the `ON` clause.\n'
        'Any new operation by user or role with privilege on the granted table SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting the privilege **some_privilege** to one or more users or roles\n'
        'for a database or a table using the `GRANT some_privilege(column)` statement for one column.\n'
        'Multiple columns will be supported with `GRANT some_privilege(column1, column2...)` statement.\n'
        'The privileges will be granted for only the specified columns.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL grant the privilege **some_privilege** to the specified users or roles\n'
        'after the successful execution of the `GRANT some_privilege(column)` statement for the specified column.\n'
        'Granting of the privilege **some_privilege** over multiple columns SHALL happen after the successful\n'
        'execution of the `GRANT some_privilege(column1, column2...)` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the privilege **some_privilege** over specified columns SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying cluster on which to grant privileges using the `ON CLUSTER`\n'
        'clause in the `GRANT PRIVILEGE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Privilege_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Privilege.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `GRANT` statement that\n'
        'grants explicit privileges to a user or a role.\n'
        '\n'
        '```sql\n'
        'GRANT [ON CLUSTER cluster_name]\n'
        '    privilege {SELECT | SELECT(columns) | INSERT | ALTER | CREATE | DROP | TRUNCATE | OPTIMIZE | SHOW | KILL QUERY | ACCESS MANAGEMENT | SYSTEM | INTROSPECTION | SOURCES | dictGet | NONE |ALL \t[PRIVILEGES]} [, ...]\n'
        '    ON {*.* | database.* | database.table | * | table}\n'
        '    TO {user | role | CURRENT_USER} [,...]\n'
        '    [WITH GRANT OPTION]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking privileges to one or more users or roles\n'
        'for a database or a table on some specific cluster using the `REVOKE ON CLUSTER cluster_name` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Cluster_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Cluster.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove some privilege from the specified users or roles\n'
        'on cluster **cluster_name** after the successful execution of the\n'
        '`REVOKE ON CLUSTER cluster_name some_privilege` statement. Any new operation by a user or a user\n'
        'that had the specified role which requires that privilege on cluster **cluster_name** SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Any = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Any',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking ANY privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE some_privilege` statement.\n'
        '**some_privilege** refers to any Clickhouse defined privilege, whose hierarchy includes\n'
        'SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,\n'
        'SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Any_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Any.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **some_privilege** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE some_privilege` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the privilege **some_privilege** SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Select = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Select',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **select** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE SELECT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Select_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Select.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **select** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE SELECT` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **select** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Insert = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Insert',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **insert** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE INSERT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Insert_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Insert.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **insert** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE INSERT` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **insert** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Alter = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **alter** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE ALTER` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **alter** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE ALTER` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **alter** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Create = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **create** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE CREATE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **create** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE CREATE` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **create** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Drop = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **drop** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE DROP` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **drop** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE DROP` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **drop** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Truncate = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Truncate',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **truncate** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE TRUNCATE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Truncate_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Truncate.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **truncate** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE TRUNCATE` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **truncate** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Optimize = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Optimize',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **optimize** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE OPTIMIZE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Optimize_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Optimize.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **optimize** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE OPTMIZE` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **optimize** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Show = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Show',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **show** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE SHOW` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Show_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Show.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **show** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE SHOW` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **show** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_KillQuery = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **kill query** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE KILL QUERY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_KillQuery_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **kill query** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE KILL QUERY` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **kill query** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_AccessManagement = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **access management** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE ACCESS MANAGEMENT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_AccessManagement_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **access management** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE ACCESS MANAGEMENT` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **access management** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_System = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.System',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **system** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE SYSTEM` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_System_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.System.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **system** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE SYSTEM` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **system** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Introspection = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Introspection',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **introspection** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE INTROSPECTION` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Introspection_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Introspection.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **introspection** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE INTROSPECTION` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **introspection** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Sources = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Sources',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **sources** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE SOURCES` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Sources_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Sources.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **sources** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE SOURCES` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **sources** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_DictGet = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.DictGet',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the **dictGet** privilege to one or more users or roles\n'
        'for a database or a table using the `REVOKE dictGet` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_DictGet_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.DictGet.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **dictGet** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE dictGet` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the **dictGet** privilege SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking the privilege **some_privilege** to one or more users or roles\n'
        'for a database or a table using the `REVOKE some_privilege(column)` statement for one column.\n'
        'Multiple columns will be supported with `REVOKE some_privilege(column1, column2...)` statement.\n'
        'The privileges will be revoked for only the specified columns.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the privilege **some_privilege** from the specified users or roles\n'
        'after the successful execution of the `REVOKE some_privilege(column)` statement for the specified column.\n'
        'Removal of the privilege **some_privilege** over multiple columns SHALL happen after the successful\n'
        'execution of the `REVOKE some_privilege(column1, column2...)` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires the privilege **some_privilege** over specified SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Multiple = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Multiple',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking MULTIPLE **privileges** to one or more users or roles\n'
        'for a database or a table using the `REVOKE privilege1, privilege2...` statement.\n'
        '**privileges** refers to any set of Clickhouse defined privilege, whose hierarchy includes\n'
        'SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,\n'
        'SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Multiple_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Multiple.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **privileges** from the specified users or roles\n'
        'after the successful execution of the `REVOKE privilege1, privilege2...` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires any of the **privileges** SHALL fail if user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_All = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking **all** privileges to one or more users or roles\n'
        'for a database or a table using the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statements.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_All_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.All.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove **all** privileges from the specified users or roles\n'
        'after the successful execution of the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires one or more privileges that are part of **all**\n'
        'privileges SHALL fail.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_None = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking **no** privileges to one or more users or roles\n'
        'for a database or a table using the `REVOKE NONE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_None_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.None.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove **no** privileges from the specified users or roles\n'
        'after the successful execution of the `REVOKE NONE` statement.\n'
        'Any new operation by a user or a user that had the specified role\n'
        'shall have the same effect after this command as it did before this command.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_On = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `ON` clause in the `REVOKE` privilege statement\n'
        'which SHALL allow to specify one or more tables to which the privilege SHALL\n'
        'be revoked using the following patterns\n'
        '\n'
        '* `db.table` specific table in the specified database\n'
        '* `db.*` any table in the specified database\n'
        '* `*.*` any table in any database\n'
        '* `table` specific table in the current database\n'
        '* `*` any table in the current database\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_On_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.On.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the specificed priviliges from the specified one or more tables\n'
        'indicated with the `ON` clause in the `REVOKE` privilege statement.\n'
        'The tables will be indicated using the following patterns\n'
        '\n'
        '* `db.table` specific table in the specified database\n'
        '* `db.*` any table in the specified database\n'
        '* `*.*` any table in any database\n'
        '* `table` specific table in the current database\n'
        '* `*` any table in the current database\n'
        '\n'
        'Any new operation by a user or a user that had the specified role\n'
        'which requires one or more privileges on the revoked tables SHALL fail.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_From = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.From',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `FROM` clause in the `REVOKE` privilege statement\n'
        'which SHALL allow to specify one or more users to which the privilege SHALL\n'
        'be revoked using the following patterns\n'
        '\n'
        '* `{user | CURRENT_USER} [,...]` some combination of users by name, which may include the current user\n'
        '* `ALL` all users\n'
        '* `ALL EXCEPT {user | CURRENT_USER} [,...]` the logical reverse of the first pattern\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_From_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.From.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove **priviliges** to any set of users specified in the `FROM` clause\n'
        'in the `REVOKE` privilege statement. The details of the removed **privileges** will be specified\n'
        'in the other clauses. Any new operation by one of the specified users whose **privileges** have been\n'
        'revoked SHALL fail. The patterns that expand the `FROM` clause are listed below\n'
        '\n'
        '* `{user | CURRENT_USER} [,...]` some combination of users by name, which may include the current user\n'
        '* `ALL` all users\n'
        '* `ALL EXCEPT {user | CURRENT_USER} [,...]` the logical reverse of the first pattern\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Privilege_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Privilege.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `REVOKE` statement that\n'
        'revokes explicit privileges of a user or a role.\n'
        '\n'
        '```sql\n'
        'REVOKE [ON CLUSTER cluster_name] privilege\n'
        '    [(column_name [,...])] [,...]\n'
        '    ON {db.table|db.*|*.*|table|*}\n'
        '    FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]\n'
        '```\n'
        '<!-- old syntax, for reference -->\n'
        '<!-- ```sql\n'
        'REVOKE [GRANT OPTION FOR]\n'
        '    {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]\n'
        '    ON {*.* | database.* | database.table | * | table}\n'
        '    FROM user_or_role [, user_or_role ...]\n'
        '``` -->\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_PartialRevoke_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.PartialRevoke.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support partial revokes by using `partial_revokes` variable\n'
        'that can be set or unset using the following syntax.\n'
        '\n'
        'To disable partial revokes the `partial_revokes` variable SHALL be set to `0`\n'
        '\n'
        '```sql\n'
        'SET partial_revokes = 0\n'
        '```\n'
        '\n'
        'To enable partial revokes the `partial revokes` variable SHALL be set to `1`\n'
        '\n'
        '```sql\n'
        'SET partial_revokes = 1\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_PartialRevoke_Effect = Requirement(
        name='RQ.SRS-006.RBAC.PartialRevoke.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'FIXME: Need to be defined.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting one or more roles to\n'
        'one or more users or roles using the `GRANT` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add all the privileges that are assigned to the role\n'
        'which is granted to the user or the role to which `GRANT` role statement is applied.\n'
        'Any new operation that requires the privileges included in the role\n'
        'SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_CurrentUser = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.CurrentUser',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting one or more roles to current user using\n'
        '`TO CURRENT_USER` clause in the `GRANT` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_CurrentUser_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.CurrentUser.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add all the privileges that are assigned to the role\n'
        'which is granted to the current user via the `GRANT` statement. Any new operation that\n'
        'requires the privileges included in the role SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_AdminOption = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.AdminOption',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support granting `admin option` privilege\n'
        'to one or more users or roles using the `WITH ADMIN OPTION` clause\n'
        'in the `GRANT` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_AdminOption_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.AdminOption.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL add the **admin option** privilege to the specified users or roles\n'
        'after the successful execution of the `GRANT` role statement with the `WITH ADMIN OPTION` clause.\n'
        'Any new **system queries** statements executed by a user or a user that has the specified role\n'
        'which requires the **admin option** privilege SHALL succeed.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying cluster on which the user is to be granted one or more roles\n'
        'using `ON CLUSTER` clause in the `GRANT` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Grant_Role_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Grant.Role.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for `GRANT` role statement\n'
        '\n'
        '``` sql\n'
        'GRANT\n'
        '    ON CLUSTER cluster_name\n'
        '    role [, role ...]\n'
        '    TO {user | role | CURRENT_USER} [,...]\n'
        '    [WITH ADMIN OPTION]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking one or more roles from\n'
        'one or more users or roles using the `REVOKE` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove all the privileges that are assigned to the role\n'
        'that is being revoked from the user or the role to which the `REVOKE` role statement is applied.\n'
        'Any new operation, by the user or users that have the role which included the role being revoked,\n'
        'that requires the privileges included in the role SHALL fail if the user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Keywords = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Keywords',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking one or more roles from\n'
        'special groupings of one or more users or roles with the `ALL`, `ALL EXCEPT`,\n'
        'and `CURRENT_USER` keywords.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Keywords_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Keywords.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove all the privileges that are assigned to the role\n'
        'that is being revoked from the user or the role to which the `REVOKE` role statement with the specified keywords is applied.\n'
        'Any new operation, by the user or users that have the role which included the role being revoked,\n'
        'that requires the privileges included in the role SHALL fail if the user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking one or more roles from\n'
        'one or more users or roles from one or more clusters\n'
        'using the `REVOKE ON CLUSTER` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Cluster_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Cluster.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove all the privileges that are assigned to the role\n'
        'that is being revoked from the user or the role from the cluster(s)\n'
        'to which the `REVOKE ON CLUSTER` role statement is applied.\n'
        'Any new operation, by the user or users that have the role which included the role being revoked,\n'
        'that requires the privileges included in the role SHALL fail if the user does not have it otherwise.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_AdminOption = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.AdminOption',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support revoking `admin option` privilege\n'
        'in one or more users or roles using the `ADMIN OPTION FOR` clause\n'
        'in the `REVOKE` role statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_AdminOption_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.AdminOption.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove the **admin option** privilege from the specified users or roles\n'
        'after the successful execution of the `REVOKE` role statement with the `ADMIN OPTION FOR` clause.\n'
        'Any new **system queries** statements executed by a user or a user that has the specified role\n'
        'which requires the **admin option** privilege SHALL fail.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Revoke_Role_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Revoke.Role.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `REVOKE` role statement\n'
        '\n'
        '```sql\n'
        'REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR]\n'
        '    role [,...]\n'
        '    FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Show_Grants = Requirement(
        name='RQ.SRS-006.RBAC.Show.Grants',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support listing all the privileges granted to current user and role\n'
        'using the `SHOW GRANTS` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Show_Grants_For = Requirement(
        name='RQ.SRS-006.RBAC.Show.Grants.For',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support listing all the privileges granted to a user or a role\n'
        'using the `FOR` clause in the `SHOW GRANTS` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Show_Grants_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Show.Grants.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[Clickhouse] SHALL use the following syntax for the `SHOW GRANTS` statement\n'
        '\n'
        '``` sql\n'
        'SHOW GRANTS [FOR user_or_role]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating settings profile using the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new profile after the `CREATE SETTINGS PROFILE` statement\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_IfNotExists = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.IfNotExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE SETTINGS PROFILE` statement\n'
        'to skip raising an exception if a settings profile with the same **name** already exists.\n'
        'If `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if\n'
        'a settings profile with the same **name** already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Replace = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Replace',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE SETTINGS PROFILE` statement\n'
        'to replace existing settings profile if it already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Variables = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Variables',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning values and constraints to one or more\n'
        'variables in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Value = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning variable value in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Value_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new variable values after `CREATE SETTINGS PROFILE` statement is\n'
        'successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support setting `MIN`, `MAX`, `READONLY`, and `WRITABLE`\n'
        'constraints for the variables in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Variables_Constraints_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new variable constraints after `CREATE SETTINGS PROFILE` statement is\n'
        'successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning settings profile to one or more users\n'
        'or roles in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning settings profile to no users or roles using\n'
        '`TO NONE` clause in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning settings profile to all current users and roles\n'
        'using `TO ALL` clause in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Assignment_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment to one or more users or roles using\n'
        'the `ALL EXCEPT` clause in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Inherit = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Inherit',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support inheriting profile settings from indicated profile using\n'
        'the `INHERIT` clause in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying what cluster to create settings profile on\n'
        'using `ON CLUSTER` clause in the `CREATE SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Create_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Create.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `CREATE SETTINGS PROFILE` statement.\n'
        '\n'
        '``` sql\n'
        'CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name\n'
        '    [ON CLUSTER cluster_name]\n'
        "    [SET varname [= value] [MIN min] [MAX max] [READONLY|WRITABLE] | [INHERIT 'profile_name'] [,...]]\n"
        '    [TO {user_or_role [,...] | NONE | ALL | ALL EXCEPT user_or_role [,...]}]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering settings profile using the `ALTER STETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the updated settings profile after `ALTER SETTINGS PROFILE`\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned or SHALL raise an exception if the settings profile does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER SETTINGS PROFILE` statement\n'
        'to not raise exception if a settings profile does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a settings profile does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Rename = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Rename',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support renaming settings profile using the `RANAME TO` clause\n'
        'in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering values and constraints of one or more\n'
        'variables in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering value of the variable in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Value_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the new value of the variable after `ALTER SETTINGS PROFILE`\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering `MIN`, `MAX`, `READONLY`, and `WRITABLE`\n'
        'constraints for the variables in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Variables_Constraints_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new constraints after `ALTER SETTINGS PROFILE`\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the settings profile is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning settings profile to one or more users\n'
        'or roles using the `TO` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL unset all the variables and constraints that were defined in the settings profile\n'
        'in all users and roles to which the settings profile was previously assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning settings profile to no users or roles using the\n'
        '`TO NONE` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning settings profile to all current users and roles\n'
        'using the `TO ALL` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment to one or more users or roles using\n'
        'the `TO ALL EXCEPT` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_Inherit = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Inherit',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the settings profile by inheriting settings from\n'
        'specified profile using `INHERIT` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Assignment_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the settings profile on a specified cluster using\n'
        '`ON CLUSTER` clause in the `ALTER SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Alter_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Alter.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `ALTER SETTINGS PROFILE` statement.\n'
        '\n'
        '``` sql\n'
        'ALTER SETTINGS PROFILE [IF EXISTS] name\n'
        '    [ON CLUSTER cluster_name]\n'
        '    [RENAME TO new_name]\n'
        "    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]\n"
        '    [TO {user_or_role [,...] | NONE | ALL | ALL EXCEPT user_or_role [,...]]}\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Drop = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing one or more settings profiles using the `DROP SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL unset all the variables and constraints that were defined in the settings profile\n'
        'in all the users and roles to which the settings profile was assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Drop_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Drop.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP SETTINGS PROFILE` statement\n'
        'to skip raising an exception if the settings profile does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if a settings profile does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Drop_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Drop.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support dropping one or more settings profiles on specified cluster using\n'
        '`ON CLUSTER` clause in the `DROP SETTINGS PROFILE` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_Drop_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.Drop.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `DROP SETTINGS PROFILE` statement\n'
        '\n'
        '``` sql\n'
        'DROP SETTINGS PROFILE [IF EXISTS] name [,name,...]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_SettingsProfile_ShowCreateSettingsProfile = Requirement(
        name='RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE SETTINGS PROFILE` statement used to create the settings profile\n'
        'using the `SHOW CREATE SETTINGS PROFILE` statement with the following syntax\n'
        '\n'
        '``` sql\n'
        'SHOW CREATE SETTINGS PROFILE name\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating quotas using the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new limits specified by the quota after the `CREATE QUOTA` statement\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the quota is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_IfNotExists = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.IfNotExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE QUOTA` statement\n'
        'to skip raising an exception if a quota with the same **name** already exists.\n'
        'If `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if\n'
        'a quota with the same **name** already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Replace = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Replace',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE QUOTA` statement\n'
        'to replace existing quota if it already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating quotas on a specific cluster with the\n'
        '`ON CLUSTER` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Interval = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Interval',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support defining the quota interval that specifies\n'
        'a period of time over for which the quota SHALL apply using the\n'
        '`FOR INTERVAL` clause in the `CREATE QUOTA` statement.\n'
        '\n'
        'This statement SHALL also support a number and a time period which will be one\n'
        'of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:\n'
        '\n'
        '`FOR INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some real number\n'
        'to define the interval.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Interval_Randomized = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Interval.Randomized',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support defining the quota randomized interval that specifies\n'
        'a period of time over for which the quota SHALL apply using the\n'
        '`FOR RANDOMIZED INTERVAL` clause in the `CREATE QUOTA` statement.\n'
        '\n'
        'This statement SHALL also support a number and a time period which will be one\n'
        'of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:\n'
        '\n'
        '`FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some\n'
        'real number to define the interval.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Queries = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Queries',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting number of requests over a period of time\n'
        'using the `QUERIES` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Errors = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Errors',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting number of queries that threw an exception\n'
        'using the `ERRORS` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_ResultRows = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.ResultRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the total number of rows given as the result\n'
        'using the `RESULT ROWS` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_ReadRows = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.ReadRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the total number of source rows read from tables\n'
        'for running the query on all remote servers\n'
        'using the `READ ROWS` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_ResultBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.ResultBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the total number of bytes that can be returned as the result\n'
        'using the `RESULT BYTES` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_ReadBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.ReadBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the total number of source bytes read from tables\n'
        'for running the query on all remote servers\n'
        'using the `READ BYTES` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_ExecutionTime = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.ExecutionTime',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the maximum query execution time\n'
        'using the `EXECUTION TIME` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_NoLimits = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.NoLimits',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the maximum query execution time\n'
        'using the `NO LIMITS` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_TrackingOnly = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.TrackingOnly',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the maximum query execution time\n'
        'using the `TRACKING ONLY` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_KeyedBy = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.KeyedBy',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support to track quota for some key\n'
        'following the `KEYED BY` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_KeyedByOptions = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.KeyedByOptions',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support to track quota separately for some parameter\n'
        "using the `KEYED BY 'parameter'` clause in the `CREATE QUOTA` statement.\n"
        '\n'
        "'parameter' can be one of:\n"
        "`{'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}`\n"
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning quota to one or more users\n'
        'or roles using the `TO` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning quota to no users or roles using\n'
        '`TO NONE` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning quota to all current users and roles\n'
        'using `TO ALL` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Assignment_Except = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Assignment.Except',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment of quota to one or more users or roles using\n'
        'the `EXCEPT` clause in the `CREATE QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Create_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Create.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `CREATE QUOTA` statement\n'
        '\n'
        '```sql\n'
        'CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]\n'
        "    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]\n"
        '    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}\n'
        '        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |\n'
        '         NO LIMITS | TRACKING ONLY} [,...]]\n'
        '    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering quotas using the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use new limits specified by the updated quota after the `ALTER QUOTA` statement\n'
        'is successfully executed for any new operations performed by all the users and roles to which\n'
        'the quota is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER QUOTA` statement\n'
        'to skip raising an exception if a quota does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be raised if\n'
        'a quota does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Rename = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Rename',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `RENAME TO` clause in the `ALTER QUOTA` statement\n'
        'to rename the quota to the specified name.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering quotas on a specific cluster with the\n'
        '`ON CLUSTER` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Interval = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Interval',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support redefining the quota interval that specifies\n'
        'a period of time over for which the quota SHALL apply using the\n'
        '`FOR INTERVAL` clause in the `ALTER QUOTA` statement.\n'
        '\n'
        'This statement SHALL also support a number and a time period which will be one\n'
        'of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:\n'
        '\n'
        '`FOR INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some real number\n'
        'to define the interval.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Interval_Randomized = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Interval.Randomized',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support redefining the quota randomized interval that specifies\n'
        'a period of time over for which the quota SHALL apply using the\n'
        '`FOR RANDOMIZED INTERVAL` clause in the `ALTER QUOTA` statement.\n'
        '\n'
        'This statement SHALL also support a number and a time period which will be one\n'
        'of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:\n'
        '\n'
        '`FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some\n'
        'real number to define the interval.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Queries = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Queries',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of number of requests over a period of time\n'
        'using the `QUERIES` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Errors = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Errors',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of number of queries that threw an exception\n'
        'using the `ERRORS` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_ResultRows = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.ResultRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of the total number of rows given as the result\n'
        'using the `RESULT ROWS` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_ReadRows = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.ReadRows',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of the total number of source rows read from tables\n'
        'for running the query on all remote servers\n'
        'using the `READ ROWS` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ALter_ResultBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ALter.ResultBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of the total number of bytes that can be returned as the result\n'
        'using the `RESULT BYTES` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_ReadBytes = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.ReadBytes',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of the total number of source bytes read from tables\n'
        'for running the query on all remote servers\n'
        'using the `READ BYTES` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_ExecutionTime = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.ExecutionTime',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering the limit of the maximum query execution time\n'
        'using the `EXECUTION TIME` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_NoLimits = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.NoLimits',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the maximum query execution time\n'
        'using the `NO LIMITS` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_TrackingOnly = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.TrackingOnly',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support limiting the maximum query execution time\n'
        'using the `TRACKING ONLY` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_KeyedBy = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.KeyedBy',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering quota to track quota separately for some key\n'
        'following the `KEYED BY` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_KeyedByOptions = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.KeyedByOptions',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering quota to track quota separately for some parameter\n'
        "using the `KEYED BY 'parameter'` clause in the `ALTER QUOTA` statement.\n"
        '\n'
        "'parameter' can be one of:\n"
        "`{'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}`\n"
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning quota to one or more users\n'
        'or roles using the `TO` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning quota to no users or roles using\n'
        '`TO NONE` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning quota to all current users and roles\n'
        'using `TO ALL` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Assignment_Except = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Assignment.Except',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment of quota to one or more users or roles using\n'
        'the `EXCEPT` clause in the `ALTER QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Alter_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Alter.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `ALTER QUOTA` statement\n'
        '\n'
        '``` sql\n'
        'ALTER QUOTA [IF EXIST] name\n'
        '    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]\n'
        '    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]\n'
        '    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Drop = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing one or more quotas using the `DROP QUOTA` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL unset all the limits that were defined in the quota\n'
        'in all the users and roles to which the quota was assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Drop_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Drop.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP QUOTA` statement\n'
        'to skip raising an exception when the quota does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if the quota does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Drop_Cluster = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Drop.Cluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP QUOTA` statement\n'
        'to indicate the cluster the quota to be dropped is located on.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_Drop_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Quota.Drop.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `DROP QUOTA` statement\n'
        '\n'
        '``` sql\n'
        'DROP QUOTA [IF EXISTS] name [,name...]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowQuotas = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowQuotas',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing all of the current quotas\n'
        'using the `SHOW QUOTAS` statement with the following syntax\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowQuotas_IntoOutfile = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowQuotas.IntoOutfile',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `INTO OUTFILE` clause in the `SHOW QUOTAS` statement to define an outfile by some given string literal.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowQuotas_Format = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowQuotas.Format',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `FORMAT` clause in the `SHOW QUOTAS` statement to define a format for the output quota list.\n'
        '\n'
        'The types of valid formats are many, listed in output column:\n'
        'https://clickhouse.tech/docs/en/interfaces/formats/\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowQuotas_Settings = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowQuotas.Settings',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `SETTINGS` clause in the `SHOW QUOTAS` statement to define settings in the showing of all quotas.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowQuotas_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowQuotas.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using the `SHOW QUOTAS` statement\n'
        'with the following syntax\n'
        '``` sql\n'
        'SHOW QUOTAS\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Name = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Name',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE QUOTA` statement used to create the quota with some given name\n'
        'using the `SHOW CREATE QUOTA` statement with the following syntax\n'
        '\n'
        '``` sql\n'
        'SHOW CREATE QUOTA name\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Current = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Current',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE QUOTA` statement used to create the CURRENT quota\n'
        'using the `SHOW CREATE QUOTA CURRENT` statement or the shorthand form\n'
        '`SHOW CREATE QUOTA`\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_Quota_ShowCreateQuota_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax when\n'
        'using the `SHOW CREATE QUOTA` statement.\n'
        '\n'
        '```sql\n'
        'SHOW CREATE QUOTA [name | CURRENT]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support creating row policy using the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Effect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the new row policy to control access to the specified table\n'
        'after the `CREATE ROW POLICY` statement is successfully executed\n'
        'for any new operations on the table performed by all the users and roles to which\n'
        'the row policy is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_IfNotExists = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.IfNotExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE ROW POLICY` statement\n'
        'to skip raising an exception if a row policy with the same **name** already exists.\n'
        'If the `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if\n'
        'a row policy with the same **name** already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Replace = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Replace',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE ROW POLICY` statement\n'
        'to replace existing row policy if it already exists.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying cluster on which to create the role policy\n'
        'using the `ON CLUSTER` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_On = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying table on which to create the role policy\n'
        'using the `ON` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Access = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Access',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support allowing or restricting access to rows using the\n'
        '`AS` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Access_Permissive = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Access.Permissive',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support allowing access to rows using the\n'
        '`AS PERMISSIVE` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Access_Restrictive = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Access.Restrictive',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support restricting access to rows using the\n'
        '`AS RESTRICTIVE` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_ForSelect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.ForSelect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying which rows are affected\n'
        'using the `FOR SELECT` clause in the `CREATE ROW POLICY` statement.\n'
        'REQUIRES CONFIRMATION\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Condition = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Condition',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying a condition that\n'
        'that can be any SQL expression which returns a boolean using the `USING`\n'
        'clause in the `CREATE ROW POLOCY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Condition_Effect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Condition.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL check the condition specified in the row policy using the\n'
        '`USING` clause in the `CREATE ROW POLICY` statement. The users or roles\n'
        'to which the row policy is assigned SHALL only see data for which\n'
        'the condition evaluates to the boolean value of `true`.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning row policy to one or more users\n'
        'or roles using the `TO` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning row policy to no users or roles using\n'
        'the `TO NONE` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support assigning row policy to all current users and roles\n'
        'using `TO ALL` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment of row policy to one or more users or roles using\n'
        'the `ALL EXCEPT` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Create_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Create.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `CRETE ROW POLICY` statement\n'
        '\n'
        '``` sql\n'
        'CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table\n'
        '    [AS {PERMISSIVE | RESTRICTIVE}]\n'
        '    [FOR SELECT]\n'
        '    [USING condition]\n'
        '    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering row policy using the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Effect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL use the updated row policy to control access to the specified table\n'
        'after the `ALTER ROW POLICY` statement is successfully executed\n'
        'for any new operations on the table performed by all the users and roles to which\n'
        'the row policy is assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the `IF EXISTS` clause in the `ALTER ROW POLICY` statement\n'
        'to skip raising an exception if a row policy does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be raised if\n'
        'a row policy does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_ForSelect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.ForSelect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support modifying rows on which to apply the row policy\n'
        'using the `FOR SELECT` clause in the `ALTER ROW POLICY` statement.\n'
        'REQUIRES FUNCTION CONFIRMATION.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying cluster on which to alter the row policy\n'
        'using the `ON CLUSTER` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_On = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support specifying table on which to alter the row policy\n'
        'using the `ON` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Rename = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Rename',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support renaming the row policy using the `RENAME` clause\n'
        'in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Access = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Access',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support altering access to rows using the\n'
        '`AS` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Permissive = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support permitting access to rows using the\n'
        '`AS PERMISSIVE` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Restrictive = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support restricting access to rows using the\n'
        '`AS RESTRICTIVE` clause in the `CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Condition = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Condition',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support re-specifying the row policy condition\n'
        'using the `USING` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Condition_Effect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL check the new condition specified for the row policy using the\n'
        '`USING` clause in the `ALTER ROW POLICY` statement. The users or roles\n'
        'to which the row policy is assigned SHALL only see data for which\n'
        'the new condition evaluates to the boolean value of `true`.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Condition_None = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing the row policy condition\n'
        'using the `USING NONE` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning row policy to one or more users\n'
        'or roles using the `TO` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_None = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.None',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning row policy to no users or roles using\n'
        'the `TO NONE` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_All = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.All',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support reassigning row policy to all current users and roles\n'
        'using the `TO ALL` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_AllExcept = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.AllExcept',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support excluding assignment of row policy to one or more users or roles using\n'
        'the `ALL EXCEPT` clause in the `ALTER ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Alter_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Alter.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `ALTER ROW POLICY` statement\n'
        '\n'
        '``` sql\n'
        'ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table\n'
        '    [RENAME TO new_name]\n'
        '    [AS {PERMISSIVE | RESTRICTIVE}]\n'
        '    [FOR SELECT]\n'
        '    [USING {condition | NONE}][,...]\n'
        '    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing one or more row policies using the `DROP ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop_Effect = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop.Effect',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL remove checking the condition defined in the row policy\n'
        'in all the users and roles to which the row policy was assigned.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop_IfExists = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop.IfExists',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support using the `IF EXISTS` clause in the `DROP ROW POLICY` statement\n'
        'to skip raising an exception when the row policy does not exist.\n'
        'If the `IF EXISTS` clause is not specified then an exception SHALL be\n'
        'raised if the row policy does not exist.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop_On = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing row policy from one or more specified tables\n'
        'using the `ON` clause in the `DROP ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop_OnCluster = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop.OnCluster',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support removing row policy from specified cluster\n'
        'using the `ON CLUSTER` clause in the `DROP ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_Drop_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.Drop.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for the `DROP ROW POLICY` statement.\n'
        '\n'
        '``` sql\n'
        'DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing the `CREATE ROW POLICY` statement used to create the row policy\n'
        'using the `SHOW CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy_On = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing statement used to create row policy on specific table\n'
        'using the `ON` in the `SHOW CREATE ROW POLICY` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowCreateRowPolicy_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for `SHOW CREATE ROW POLICY`.\n'
        '\n'
        '``` sql\n'
        'SHOW CREATE [ROW] POLICY name ON [database.]table\n'
        '```\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing row policies using the `SHOW ROW POLICIES` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies_On = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.On',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support showing row policies on a specific table\n'
        'using the `ON` clause in the `SHOW ROW POLICIES` statement.\n'
        ),
        link=None
    )

RQ_SRS_006_RBAC_RowPolicy_ShowRowPolicies_Syntax = Requirement(
        name='RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.Syntax',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL support the following syntax for `SHOW ROW POLICIES`.\n'
        '\n'
        '```sql\n'
        'SHOW [ROW] POLICIES [ON [database.]table]\n'
        '```\n'
        ),
        link=None
    )
