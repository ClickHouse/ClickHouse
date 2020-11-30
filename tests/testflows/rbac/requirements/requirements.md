# SRS-006 ClickHouse Role Based Access Control
# Software Requirements Specification

## Table of Contents

* 1 [Revision History](#revision-history)
* 2 [Introduction](#introduction)
* 3 [Terminology](#terminology)
* 4 [Privilege Definitions](#privilege-definitions)
* 5 [Requirements](#requirements)
  * 5.1 [Generic](#generic)
    * 5.1.1 [RQ.SRS-006.RBAC](#rqsrs-006rbac)
    * 5.1.2 [Login](#login)
      * 5.1.2.1 [RQ.SRS-006.RBAC.Login](#rqsrs-006rbaclogin)
      * 5.1.2.2 [RQ.SRS-006.RBAC.Login.DefaultUser](#rqsrs-006rbaclogindefaultuser)
    * 5.1.3 [User](#user)
      * 5.1.3.1 [RQ.SRS-006.RBAC.User](#rqsrs-006rbacuser)
      * 5.1.3.2 [RQ.SRS-006.RBAC.User.Roles](#rqsrs-006rbacuserroles)
      * 5.1.3.3 [RQ.SRS-006.RBAC.User.Privileges](#rqsrs-006rbacuserprivileges)
      * 5.1.3.4 [RQ.SRS-006.RBAC.User.Variables](#rqsrs-006rbacuservariables)
      * 5.1.3.5 [RQ.SRS-006.RBAC.User.Variables.Constraints](#rqsrs-006rbacuservariablesconstraints)
      * 5.1.3.6 [RQ.SRS-006.RBAC.User.SettingsProfile](#rqsrs-006rbacusersettingsprofile)
      * 5.1.3.7 [RQ.SRS-006.RBAC.User.Quotas](#rqsrs-006rbacuserquotas)
      * 5.1.3.8 [RQ.SRS-006.RBAC.User.RowPolicies](#rqsrs-006rbacuserrowpolicies)
      * 5.1.3.9 [RQ.SRS-006.RBAC.User.AccountLock](#rqsrs-006rbacuseraccountlock)
      * 5.1.3.10 [RQ.SRS-006.RBAC.User.AccountLock.DenyAccess](#rqsrs-006rbacuseraccountlockdenyaccess)
      * 5.1.3.11 [RQ.SRS-006.RBAC.User.DefaultRole](#rqsrs-006rbacuserdefaultrole)
      * 5.1.3.12 [RQ.SRS-006.RBAC.User.RoleSelection](#rqsrs-006rbacuserroleselection)
      * 5.1.3.13 [RQ.SRS-006.RBAC.User.ShowCreate](#rqsrs-006rbacusershowcreate)
      * 5.1.3.14 [RQ.SRS-006.RBAC.User.ShowPrivileges](#rqsrs-006rbacusershowprivileges)
    * 5.1.4 [Role](#role)
      * 5.1.4.1 [RQ.SRS-006.RBAC.Role](#rqsrs-006rbacrole)
      * 5.1.4.2 [RQ.SRS-006.RBAC.Role.Privileges](#rqsrs-006rbacroleprivileges)
      * 5.1.4.3 [RQ.SRS-006.RBAC.Role.Variables](#rqsrs-006rbacrolevariables)
      * 5.1.4.4 [RQ.SRS-006.RBAC.Role.SettingsProfile](#rqsrs-006rbacrolesettingsprofile)
      * 5.1.4.5 [RQ.SRS-006.RBAC.Role.Quotas](#rqsrs-006rbacrolequotas)
      * 5.1.4.6 [RQ.SRS-006.RBAC.Role.RowPolicies](#rqsrs-006rbacrolerowpolicies)
    * 5.1.5 [Partial Revokes](#partial-revokes)
      * 5.1.5.1 [RQ.SRS-006.RBAC.PartialRevokes](#rqsrs-006rbacpartialrevokes)
    * 5.1.6 [Settings Profile](#settings-profile)
      * 5.1.6.1 [RQ.SRS-006.RBAC.SettingsProfile](#rqsrs-006rbacsettingsprofile)
      * 5.1.6.2 [RQ.SRS-006.RBAC.SettingsProfile.Constraints](#rqsrs-006rbacsettingsprofileconstraints)
      * 5.1.6.3 [RQ.SRS-006.RBAC.SettingsProfile.ShowCreate](#rqsrs-006rbacsettingsprofileshowcreate)
    * 5.1.7 [Quotas](#quotas)
      * 5.1.7.1 [RQ.SRS-006.RBAC.Quotas](#rqsrs-006rbacquotas)
      * 5.1.7.2 [RQ.SRS-006.RBAC.Quotas.Keyed](#rqsrs-006rbacquotaskeyed)
      * 5.1.7.3 [RQ.SRS-006.RBAC.Quotas.Queries](#rqsrs-006rbacquotasqueries)
      * 5.1.7.4 [RQ.SRS-006.RBAC.Quotas.Errors](#rqsrs-006rbacquotaserrors)
      * 5.1.7.5 [RQ.SRS-006.RBAC.Quotas.ResultRows](#rqsrs-006rbacquotasresultrows)
      * 5.1.7.6 [RQ.SRS-006.RBAC.Quotas.ReadRows](#rqsrs-006rbacquotasreadrows)
      * 5.1.7.7 [RQ.SRS-006.RBAC.Quotas.ResultBytes](#rqsrs-006rbacquotasresultbytes)
      * 5.1.7.8 [RQ.SRS-006.RBAC.Quotas.ReadBytes](#rqsrs-006rbacquotasreadbytes)
      * 5.1.7.9 [RQ.SRS-006.RBAC.Quotas.ExecutionTime](#rqsrs-006rbacquotasexecutiontime)
      * 5.1.7.10 [RQ.SRS-006.RBAC.Quotas.ShowCreate](#rqsrs-006rbacquotasshowcreate)
    * 5.1.8 [Row Policy](#row-policy)
      * 5.1.8.1 [RQ.SRS-006.RBAC.RowPolicy](#rqsrs-006rbacrowpolicy)
      * 5.1.8.2 [RQ.SRS-006.RBAC.RowPolicy.Condition](#rqsrs-006rbacrowpolicycondition)
      * 5.1.8.3 [RQ.SRS-006.RBAC.RowPolicy.ShowCreate](#rqsrs-006rbacrowpolicyshowcreate)
  * 5.2 [Specific](#specific)
      * 5.2.8.1 [RQ.SRS-006.RBAC.User.Use.DefaultRole](#rqsrs-006rbacuserusedefaultrole)
      * 5.2.8.2 [RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole](#rqsrs-006rbacuseruseallroleswhennodefaultrole)
      * 5.2.8.3 [RQ.SRS-006.RBAC.User.Create](#rqsrs-006rbacusercreate)
      * 5.2.8.4 [RQ.SRS-006.RBAC.User.Create.IfNotExists](#rqsrs-006rbacusercreateifnotexists)
      * 5.2.8.5 [RQ.SRS-006.RBAC.User.Create.Replace](#rqsrs-006rbacusercreatereplace)
      * 5.2.8.6 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword](#rqsrs-006rbacusercreatepasswordnopassword)
      * 5.2.8.7 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword.Login](#rqsrs-006rbacusercreatepasswordnopasswordlogin)
      * 5.2.8.8 [RQ.SRS-006.RBAC.User.Create.Password.PlainText](#rqsrs-006rbacusercreatepasswordplaintext)
      * 5.2.8.9 [RQ.SRS-006.RBAC.User.Create.Password.PlainText.Login](#rqsrs-006rbacusercreatepasswordplaintextlogin)
      * 5.2.8.10 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password](#rqsrs-006rbacusercreatepasswordsha256password)
      * 5.2.8.11 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password.Login](#rqsrs-006rbacusercreatepasswordsha256passwordlogin)
      * 5.2.8.12 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash](#rqsrs-006rbacusercreatepasswordsha256hash)
      * 5.2.8.13 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash.Login](#rqsrs-006rbacusercreatepasswordsha256hashlogin)
      * 5.2.8.14 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password](#rqsrs-006rbacusercreatepassworddoublesha1password)
      * 5.2.8.15 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password.Login](#rqsrs-006rbacusercreatepassworddoublesha1passwordlogin)
      * 5.2.8.16 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash](#rqsrs-006rbacusercreatepassworddoublesha1hash)
      * 5.2.8.17 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash.Login](#rqsrs-006rbacusercreatepassworddoublesha1hashlogin)
      * 5.2.8.18 [RQ.SRS-006.RBAC.User.Create.Host.Name](#rqsrs-006rbacusercreatehostname)
      * 5.2.8.19 [RQ.SRS-006.RBAC.User.Create.Host.Regexp](#rqsrs-006rbacusercreatehostregexp)
      * 5.2.8.20 [RQ.SRS-006.RBAC.User.Create.Host.IP](#rqsrs-006rbacusercreatehostip)
      * 5.2.8.21 [RQ.SRS-006.RBAC.User.Create.Host.Any](#rqsrs-006rbacusercreatehostany)
      * 5.2.8.22 [RQ.SRS-006.RBAC.User.Create.Host.None](#rqsrs-006rbacusercreatehostnone)
      * 5.2.8.23 [RQ.SRS-006.RBAC.User.Create.Host.Local](#rqsrs-006rbacusercreatehostlocal)
      * 5.2.8.24 [RQ.SRS-006.RBAC.User.Create.Host.Like](#rqsrs-006rbacusercreatehostlike)
      * 5.2.8.25 [RQ.SRS-006.RBAC.User.Create.Host.Default](#rqsrs-006rbacusercreatehostdefault)
      * 5.2.8.26 [RQ.SRS-006.RBAC.User.Create.DefaultRole](#rqsrs-006rbacusercreatedefaultrole)
      * 5.2.8.27 [RQ.SRS-006.RBAC.User.Create.DefaultRole.None](#rqsrs-006rbacusercreatedefaultrolenone)
      * 5.2.8.28 [RQ.SRS-006.RBAC.User.Create.DefaultRole.All](#rqsrs-006rbacusercreatedefaultroleall)
      * 5.2.8.29 [RQ.SRS-006.RBAC.User.Create.Settings](#rqsrs-006rbacusercreatesettings)
      * 5.2.8.30 [RQ.SRS-006.RBAC.User.Create.OnCluster](#rqsrs-006rbacusercreateoncluster)
      * 5.2.8.31 [RQ.SRS-006.RBAC.User.Create.Syntax](#rqsrs-006rbacusercreatesyntax)
      * 5.2.8.32 [RQ.SRS-006.RBAC.User.Alter](#rqsrs-006rbacuseralter)
      * 5.2.8.33 [RQ.SRS-006.RBAC.User.Alter.OrderOfEvaluation](#rqsrs-006rbacuseralterorderofevaluation)
      * 5.2.8.34 [RQ.SRS-006.RBAC.User.Alter.IfExists](#rqsrs-006rbacuseralterifexists)
      * 5.2.8.35 [RQ.SRS-006.RBAC.User.Alter.Cluster](#rqsrs-006rbacuseraltercluster)
      * 5.2.8.36 [RQ.SRS-006.RBAC.User.Alter.Rename](#rqsrs-006rbacuseralterrename)
      * 5.2.8.37 [RQ.SRS-006.RBAC.User.Alter.Password.PlainText](#rqsrs-006rbacuseralterpasswordplaintext)
      * 5.2.8.38 [RQ.SRS-006.RBAC.User.Alter.Password.Sha256Password](#rqsrs-006rbacuseralterpasswordsha256password)
      * 5.2.8.39 [RQ.SRS-006.RBAC.User.Alter.Password.DoubleSha1Password](#rqsrs-006rbacuseralterpassworddoublesha1password)
      * 5.2.8.40 [RQ.SRS-006.RBAC.User.Alter.Host.AddDrop](#rqsrs-006rbacuseralterhostadddrop)
      * 5.2.8.41 [RQ.SRS-006.RBAC.User.Alter.Host.Local](#rqsrs-006rbacuseralterhostlocal)
      * 5.2.8.42 [RQ.SRS-006.RBAC.User.Alter.Host.Name](#rqsrs-006rbacuseralterhostname)
      * 5.2.8.43 [RQ.SRS-006.RBAC.User.Alter.Host.Regexp](#rqsrs-006rbacuseralterhostregexp)
      * 5.2.8.44 [RQ.SRS-006.RBAC.User.Alter.Host.IP](#rqsrs-006rbacuseralterhostip)
      * 5.2.8.45 [RQ.SRS-006.RBAC.User.Alter.Host.Like](#rqsrs-006rbacuseralterhostlike)
      * 5.2.8.46 [RQ.SRS-006.RBAC.User.Alter.Host.Any](#rqsrs-006rbacuseralterhostany)
      * 5.2.8.47 [RQ.SRS-006.RBAC.User.Alter.Host.None](#rqsrs-006rbacuseralterhostnone)
      * 5.2.8.48 [RQ.SRS-006.RBAC.User.Alter.DefaultRole](#rqsrs-006rbacuseralterdefaultrole)
      * 5.2.8.49 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.All](#rqsrs-006rbacuseralterdefaultroleall)
      * 5.2.8.50 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.AllExcept](#rqsrs-006rbacuseralterdefaultroleallexcept)
      * 5.2.8.51 [RQ.SRS-006.RBAC.User.Alter.Settings](#rqsrs-006rbacuseraltersettings)
      * 5.2.8.52 [RQ.SRS-006.RBAC.User.Alter.Settings.Min](#rqsrs-006rbacuseraltersettingsmin)
      * 5.2.8.53 [RQ.SRS-006.RBAC.User.Alter.Settings.Max](#rqsrs-006rbacuseraltersettingsmax)
      * 5.2.8.54 [RQ.SRS-006.RBAC.User.Alter.Settings.Profile](#rqsrs-006rbacuseraltersettingsprofile)
      * 5.2.8.55 [RQ.SRS-006.RBAC.User.Alter.Syntax](#rqsrs-006rbacuseraltersyntax)
      * 5.2.8.56 [RQ.SRS-006.RBAC.SetDefaultRole](#rqsrs-006rbacsetdefaultrole)
      * 5.2.8.57 [RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser](#rqsrs-006rbacsetdefaultrolecurrentuser)
      * 5.2.8.58 [RQ.SRS-006.RBAC.SetDefaultRole.All](#rqsrs-006rbacsetdefaultroleall)
      * 5.2.8.59 [RQ.SRS-006.RBAC.SetDefaultRole.AllExcept](#rqsrs-006rbacsetdefaultroleallexcept)
      * 5.2.8.60 [RQ.SRS-006.RBAC.SetDefaultRole.None](#rqsrs-006rbacsetdefaultrolenone)
      * 5.2.8.61 [RQ.SRS-006.RBAC.SetDefaultRole.Syntax](#rqsrs-006rbacsetdefaultrolesyntax)
      * 5.2.8.62 [RQ.SRS-006.RBAC.SetRole](#rqsrs-006rbacsetrole)
      * 5.2.8.63 [RQ.SRS-006.RBAC.SetRole.Default](#rqsrs-006rbacsetroledefault)
      * 5.2.8.64 [RQ.SRS-006.RBAC.SetRole.None](#rqsrs-006rbacsetrolenone)
      * 5.2.8.65 [RQ.SRS-006.RBAC.SetRole.All](#rqsrs-006rbacsetroleall)
      * 5.2.8.66 [RQ.SRS-006.RBAC.SetRole.AllExcept](#rqsrs-006rbacsetroleallexcept)
      * 5.2.8.67 [RQ.SRS-006.RBAC.SetRole.Syntax](#rqsrs-006rbacsetrolesyntax)
      * 5.2.8.68 [RQ.SRS-006.RBAC.User.ShowCreateUser](#rqsrs-006rbacusershowcreateuser)
      * 5.2.8.69 [RQ.SRS-006.RBAC.User.ShowCreateUser.For](#rqsrs-006rbacusershowcreateuserfor)
      * 5.2.8.70 [RQ.SRS-006.RBAC.User.ShowCreateUser.Syntax](#rqsrs-006rbacusershowcreateusersyntax)
      * 5.2.8.71 [RQ.SRS-006.RBAC.User.Drop](#rqsrs-006rbacuserdrop)
      * 5.2.8.72 [RQ.SRS-006.RBAC.User.Drop.IfExists](#rqsrs-006rbacuserdropifexists)
      * 5.2.8.73 [RQ.SRS-006.RBAC.User.Drop.OnCluster](#rqsrs-006rbacuserdroponcluster)
      * 5.2.8.74 [RQ.SRS-006.RBAC.User.Drop.Syntax](#rqsrs-006rbacuserdropsyntax)
      * 5.2.8.75 [RQ.SRS-006.RBAC.Role.Create](#rqsrs-006rbacrolecreate)
      * 5.2.8.76 [RQ.SRS-006.RBAC.Role.Create.IfNotExists](#rqsrs-006rbacrolecreateifnotexists)
      * 5.2.8.77 [RQ.SRS-006.RBAC.Role.Create.Replace](#rqsrs-006rbacrolecreatereplace)
      * 5.2.8.78 [RQ.SRS-006.RBAC.Role.Create.Settings](#rqsrs-006rbacrolecreatesettings)
      * 5.2.8.79 [RQ.SRS-006.RBAC.Role.Create.Syntax](#rqsrs-006rbacrolecreatesyntax)
      * 5.2.8.80 [RQ.SRS-006.RBAC.Role.Alter](#rqsrs-006rbacrolealter)
      * 5.2.8.81 [RQ.SRS-006.RBAC.Role.Alter.IfExists](#rqsrs-006rbacrolealterifexists)
      * 5.2.8.82 [RQ.SRS-006.RBAC.Role.Alter.Cluster](#rqsrs-006rbacrolealtercluster)
      * 5.2.8.83 [RQ.SRS-006.RBAC.Role.Alter.Rename](#rqsrs-006rbacrolealterrename)
      * 5.2.8.84 [RQ.SRS-006.RBAC.Role.Alter.Settings](#rqsrs-006rbacrolealtersettings)
      * 5.2.8.85 [RQ.SRS-006.RBAC.Role.Alter.Syntax](#rqsrs-006rbacrolealtersyntax)
      * 5.2.8.86 [RQ.SRS-006.RBAC.Role.Drop](#rqsrs-006rbacroledrop)
      * 5.2.8.87 [RQ.SRS-006.RBAC.Role.Drop.IfExists](#rqsrs-006rbacroledropifexists)
      * 5.2.8.88 [RQ.SRS-006.RBAC.Role.Drop.Cluster](#rqsrs-006rbacroledropcluster)
      * 5.2.8.89 [RQ.SRS-006.RBAC.Role.Drop.Syntax](#rqsrs-006rbacroledropsyntax)
      * 5.2.8.90 [RQ.SRS-006.RBAC.Role.ShowCreate](#rqsrs-006rbacroleshowcreate)
      * 5.2.8.91 [RQ.SRS-006.RBAC.Role.ShowCreate.Syntax](#rqsrs-006rbacroleshowcreatesyntax)
      * 5.2.8.92 [RQ.SRS-006.RBAC.Grant.Privilege.To](#rqsrs-006rbacgrantprivilegeto)
      * 5.2.8.93 [RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser](#rqsrs-006rbacgrantprivilegetocurrentuser)
      * 5.2.8.94 [RQ.SRS-006.RBAC.Grant.Privilege.Select](#rqsrs-006rbacgrantprivilegeselect)
      * 5.2.8.95 [RQ.SRS-006.RBAC.Grant.Privilege.Insert](#rqsrs-006rbacgrantprivilegeinsert)
      * 5.2.8.96 [RQ.SRS-006.RBAC.Grant.Privilege.Alter](#rqsrs-006rbacgrantprivilegealter)
      * 5.2.8.97 [RQ.SRS-006.RBAC.Grant.Privilege.Create](#rqsrs-006rbacgrantprivilegecreate)
      * 5.2.8.98 [RQ.SRS-006.RBAC.Grant.Privilege.Drop](#rqsrs-006rbacgrantprivilegedrop)
      * 5.2.8.99 [RQ.SRS-006.RBAC.Grant.Privilege.Truncate](#rqsrs-006rbacgrantprivilegetruncate)
      * 5.2.8.100 [RQ.SRS-006.RBAC.Grant.Privilege.Optimize](#rqsrs-006rbacgrantprivilegeoptimize)
      * 5.2.8.101 [RQ.SRS-006.RBAC.Grant.Privilege.Show](#rqsrs-006rbacgrantprivilegeshow)
      * 5.2.8.102 [RQ.SRS-006.RBAC.Grant.Privilege.KillQuery](#rqsrs-006rbacgrantprivilegekillquery)
      * 5.2.8.103 [RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement](#rqsrs-006rbacgrantprivilegeaccessmanagement)
      * 5.2.8.104 [RQ.SRS-006.RBAC.Grant.Privilege.System](#rqsrs-006rbacgrantprivilegesystem)
      * 5.2.8.105 [RQ.SRS-006.RBAC.Grant.Privilege.Introspection](#rqsrs-006rbacgrantprivilegeintrospection)
      * 5.2.8.106 [RQ.SRS-006.RBAC.Grant.Privilege.Sources](#rqsrs-006rbacgrantprivilegesources)
      * 5.2.8.107 [RQ.SRS-006.RBAC.Grant.Privilege.DictGet](#rqsrs-006rbacgrantprivilegedictget)
      * 5.2.8.108 [RQ.SRS-006.RBAC.Grant.Privilege.None](#rqsrs-006rbacgrantprivilegenone)
      * 5.2.8.109 [RQ.SRS-006.RBAC.Grant.Privilege.All](#rqsrs-006rbacgrantprivilegeall)
      * 5.2.8.110 [RQ.SRS-006.RBAC.Grant.Privilege.GrantOption](#rqsrs-006rbacgrantprivilegegrantoption)
      * 5.2.8.111 [RQ.SRS-006.RBAC.Grant.Privilege.On](#rqsrs-006rbacgrantprivilegeon)
      * 5.2.8.112 [RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns](#rqsrs-006rbacgrantprivilegeprivilegecolumns)
      * 5.2.8.113 [RQ.SRS-006.RBAC.Grant.Privilege.OnCluster](#rqsrs-006rbacgrantprivilegeoncluster)
      * 5.2.8.114 [RQ.SRS-006.RBAC.Grant.Privilege.Syntax](#rqsrs-006rbacgrantprivilegesyntax)
      * 5.2.8.115 [RQ.SRS-006.RBAC.Revoke.Privilege.Cluster](#rqsrs-006rbacrevokeprivilegecluster)
      * 5.2.8.116 [RQ.SRS-006.RBAC.Revoke.Privilege.Any](#rqsrs-006rbacrevokeprivilegeany)
      * 5.2.8.117 [RQ.SRS-006.RBAC.Revoke.Privilege.Select](#rqsrs-006rbacrevokeprivilegeselect)
      * 5.2.8.118 [RQ.SRS-006.RBAC.Revoke.Privilege.Insert](#rqsrs-006rbacrevokeprivilegeinsert)
      * 5.2.8.119 [RQ.SRS-006.RBAC.Revoke.Privilege.Alter](#rqsrs-006rbacrevokeprivilegealter)
      * 5.2.8.120 [RQ.SRS-006.RBAC.Revoke.Privilege.Create](#rqsrs-006rbacrevokeprivilegecreate)
      * 5.2.8.121 [RQ.SRS-006.RBAC.Revoke.Privilege.Drop](#rqsrs-006rbacrevokeprivilegedrop)
      * 5.2.8.122 [RQ.SRS-006.RBAC.Revoke.Privilege.Truncate](#rqsrs-006rbacrevokeprivilegetruncate)
      * 5.2.8.123 [RQ.SRS-006.RBAC.Revoke.Privilege.Optimize](#rqsrs-006rbacrevokeprivilegeoptimize)
      * 5.2.8.124 [RQ.SRS-006.RBAC.Revoke.Privilege.Show](#rqsrs-006rbacrevokeprivilegeshow)
      * 5.2.8.125 [RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery](#rqsrs-006rbacrevokeprivilegekillquery)
      * 5.2.8.126 [RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement](#rqsrs-006rbacrevokeprivilegeaccessmanagement)
      * 5.2.8.127 [RQ.SRS-006.RBAC.Revoke.Privilege.System](#rqsrs-006rbacrevokeprivilegesystem)
      * 5.2.8.128 [RQ.SRS-006.RBAC.Revoke.Privilege.Introspection](#rqsrs-006rbacrevokeprivilegeintrospection)
      * 5.2.8.129 [RQ.SRS-006.RBAC.Revoke.Privilege.Sources](#rqsrs-006rbacrevokeprivilegesources)
      * 5.2.8.130 [RQ.SRS-006.RBAC.Revoke.Privilege.DictGet](#rqsrs-006rbacrevokeprivilegedictget)
      * 5.2.8.131 [RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns](#rqsrs-006rbacrevokeprivilegeprivelegecolumns)
      * 5.2.8.132 [RQ.SRS-006.RBAC.Revoke.Privilege.Multiple](#rqsrs-006rbacrevokeprivilegemultiple)
      * 5.2.8.133 [RQ.SRS-006.RBAC.Revoke.Privilege.All](#rqsrs-006rbacrevokeprivilegeall)
      * 5.2.8.134 [RQ.SRS-006.RBAC.Revoke.Privilege.None](#rqsrs-006rbacrevokeprivilegenone)
      * 5.2.8.135 [RQ.SRS-006.RBAC.Revoke.Privilege.On](#rqsrs-006rbacrevokeprivilegeon)
      * 5.2.8.136 [RQ.SRS-006.RBAC.Revoke.Privilege.From](#rqsrs-006rbacrevokeprivilegefrom)
      * 5.2.8.137 [RQ.SRS-006.RBAC.Revoke.Privilege.Syntax](#rqsrs-006rbacrevokeprivilegesyntax)
      * 5.2.8.138 [RQ.SRS-006.RBAC.PartialRevoke.Syntax](#rqsrs-006rbacpartialrevokesyntax)
      * 5.2.8.139 [RQ.SRS-006.RBAC.Grant.Role](#rqsrs-006rbacgrantrole)
      * 5.2.8.140 [RQ.SRS-006.RBAC.Grant.Role.CurrentUser](#rqsrs-006rbacgrantrolecurrentuser)
      * 5.2.8.141 [RQ.SRS-006.RBAC.Grant.Role.AdminOption](#rqsrs-006rbacgrantroleadminoption)
      * 5.2.8.142 [RQ.SRS-006.RBAC.Grant.Role.OnCluster](#rqsrs-006rbacgrantroleoncluster)
      * 5.2.8.143 [RQ.SRS-006.RBAC.Grant.Role.Syntax](#rqsrs-006rbacgrantrolesyntax)
      * 5.2.8.144 [RQ.SRS-006.RBAC.Revoke.Role](#rqsrs-006rbacrevokerole)
      * 5.2.8.145 [RQ.SRS-006.RBAC.Revoke.Role.Keywords](#rqsrs-006rbacrevokerolekeywords)
      * 5.2.8.146 [RQ.SRS-006.RBAC.Revoke.Role.Cluster](#rqsrs-006rbacrevokerolecluster)
      * 5.2.8.147 [RQ.SRS-006.RBAC.Revoke.AdminOption](#rqsrs-006rbacrevokeadminoption)
      * 5.2.8.148 [RQ.SRS-006.RBAC.Revoke.Role.Syntax](#rqsrs-006rbacrevokerolesyntax)
      * 5.2.8.149 [RQ.SRS-006.RBAC.Show.Grants](#rqsrs-006rbacshowgrants)
      * 5.2.8.150 [RQ.SRS-006.RBAC.Show.Grants.For](#rqsrs-006rbacshowgrantsfor)
      * 5.2.8.151 [RQ.SRS-006.RBAC.Show.Grants.Syntax](#rqsrs-006rbacshowgrantssyntax)
      * 5.2.8.152 [RQ.SRS-006.RBAC.SettingsProfile.Create](#rqsrs-006rbacsettingsprofilecreate)
      * 5.2.8.153 [RQ.SRS-006.RBAC.SettingsProfile.Create.IfNotExists](#rqsrs-006rbacsettingsprofilecreateifnotexists)
      * 5.2.8.154 [RQ.SRS-006.RBAC.SettingsProfile.Create.Replace](#rqsrs-006rbacsettingsprofilecreatereplace)
      * 5.2.8.155 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables](#rqsrs-006rbacsettingsprofilecreatevariables)
      * 5.2.8.156 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value](#rqsrs-006rbacsettingsprofilecreatevariablesvalue)
      * 5.2.8.157 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints](#rqsrs-006rbacsettingsprofilecreatevariablesconstraints)
      * 5.2.8.158 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment](#rqsrs-006rbacsettingsprofilecreateassignment)
      * 5.2.8.159 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.None](#rqsrs-006rbacsettingsprofilecreateassignmentnone)
      * 5.2.8.160 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.All](#rqsrs-006rbacsettingsprofilecreateassignmentall)
      * 5.2.8.161 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilecreateassignmentallexcept)
      * 5.2.8.162 [RQ.SRS-006.RBAC.SettingsProfile.Create.Inherit](#rqsrs-006rbacsettingsprofilecreateinherit)
      * 5.2.8.163 [RQ.SRS-006.RBAC.SettingsProfile.Create.OnCluster](#rqsrs-006rbacsettingsprofilecreateoncluster)
      * 5.2.8.164 [RQ.SRS-006.RBAC.SettingsProfile.Create.Syntax](#rqsrs-006rbacsettingsprofilecreatesyntax)
      * 5.2.8.165 [RQ.SRS-006.RBAC.SettingsProfile.Alter](#rqsrs-006rbacsettingsprofilealter)
      * 5.2.8.166 [RQ.SRS-006.RBAC.SettingsProfile.Alter.IfExists](#rqsrs-006rbacsettingsprofilealterifexists)
      * 5.2.8.167 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Rename](#rqsrs-006rbacsettingsprofilealterrename)
      * 5.2.8.168 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables](#rqsrs-006rbacsettingsprofilealtervariables)
      * 5.2.8.169 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value](#rqsrs-006rbacsettingsprofilealtervariablesvalue)
      * 5.2.8.170 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints](#rqsrs-006rbacsettingsprofilealtervariablesconstraints)
      * 5.2.8.171 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment](#rqsrs-006rbacsettingsprofilealterassignment)
      * 5.2.8.172 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.None](#rqsrs-006rbacsettingsprofilealterassignmentnone)
      * 5.2.8.173 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.All](#rqsrs-006rbacsettingsprofilealterassignmentall)
      * 5.2.8.174 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilealterassignmentallexcept)
      * 5.2.8.175 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Inherit](#rqsrs-006rbacsettingsprofilealterassignmentinherit)
      * 5.2.8.176 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.OnCluster](#rqsrs-006rbacsettingsprofilealterassignmentoncluster)
      * 5.2.8.177 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Syntax](#rqsrs-006rbacsettingsprofilealtersyntax)
      * 5.2.8.178 [RQ.SRS-006.RBAC.SettingsProfile.Drop](#rqsrs-006rbacsettingsprofiledrop)
      * 5.2.8.179 [RQ.SRS-006.RBAC.SettingsProfile.Drop.IfExists](#rqsrs-006rbacsettingsprofiledropifexists)
      * 5.2.8.180 [RQ.SRS-006.RBAC.SettingsProfile.Drop.OnCluster](#rqsrs-006rbacsettingsprofiledroponcluster)
      * 5.2.8.181 [RQ.SRS-006.RBAC.SettingsProfile.Drop.Syntax](#rqsrs-006rbacsettingsprofiledropsyntax)
      * 5.2.8.182 [RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile](#rqsrs-006rbacsettingsprofileshowcreatesettingsprofile)
      * 5.2.8.183 [RQ.SRS-006.RBAC.Quota.Create](#rqsrs-006rbacquotacreate)
      * 5.2.8.184 [RQ.SRS-006.RBAC.Quota.Create.IfNotExists](#rqsrs-006rbacquotacreateifnotexists)
      * 5.2.8.185 [RQ.SRS-006.RBAC.Quota.Create.Replace](#rqsrs-006rbacquotacreatereplace)
      * 5.2.8.186 [RQ.SRS-006.RBAC.Quota.Create.Cluster](#rqsrs-006rbacquotacreatecluster)
      * 5.2.8.187 [RQ.SRS-006.RBAC.Quota.Create.Interval](#rqsrs-006rbacquotacreateinterval)
      * 5.2.8.188 [RQ.SRS-006.RBAC.Quota.Create.Interval.Randomized](#rqsrs-006rbacquotacreateintervalrandomized)
      * 5.2.8.189 [RQ.SRS-006.RBAC.Quota.Create.Queries](#rqsrs-006rbacquotacreatequeries)
      * 5.2.8.190 [RQ.SRS-006.RBAC.Quota.Create.Errors](#rqsrs-006rbacquotacreateerrors)
      * 5.2.8.191 [RQ.SRS-006.RBAC.Quota.Create.ResultRows](#rqsrs-006rbacquotacreateresultrows)
      * 5.2.8.192 [RQ.SRS-006.RBAC.Quota.Create.ReadRows](#rqsrs-006rbacquotacreatereadrows)
      * 5.2.8.193 [RQ.SRS-006.RBAC.Quota.Create.ResultBytes](#rqsrs-006rbacquotacreateresultbytes)
      * 5.2.8.194 [RQ.SRS-006.RBAC.Quota.Create.ReadBytes](#rqsrs-006rbacquotacreatereadbytes)
      * 5.2.8.195 [RQ.SRS-006.RBAC.Quota.Create.ExecutionTime](#rqsrs-006rbacquotacreateexecutiontime)
      * 5.2.8.196 [RQ.SRS-006.RBAC.Quota.Create.NoLimits](#rqsrs-006rbacquotacreatenolimits)
      * 5.2.8.197 [RQ.SRS-006.RBAC.Quota.Create.TrackingOnly](#rqsrs-006rbacquotacreatetrackingonly)
      * 5.2.8.198 [RQ.SRS-006.RBAC.Quota.Create.KeyedBy](#rqsrs-006rbacquotacreatekeyedby)
      * 5.2.8.199 [RQ.SRS-006.RBAC.Quota.Create.KeyedByOptions](#rqsrs-006rbacquotacreatekeyedbyoptions)
      * 5.2.8.200 [RQ.SRS-006.RBAC.Quota.Create.Assignment](#rqsrs-006rbacquotacreateassignment)
      * 5.2.8.201 [RQ.SRS-006.RBAC.Quota.Create.Assignment.None](#rqsrs-006rbacquotacreateassignmentnone)
      * 5.2.8.202 [RQ.SRS-006.RBAC.Quota.Create.Assignment.All](#rqsrs-006rbacquotacreateassignmentall)
      * 5.2.8.203 [RQ.SRS-006.RBAC.Quota.Create.Assignment.Except](#rqsrs-006rbacquotacreateassignmentexcept)
      * 5.2.8.204 [RQ.SRS-006.RBAC.Quota.Create.Syntax](#rqsrs-006rbacquotacreatesyntax)
      * 5.2.8.205 [RQ.SRS-006.RBAC.Quota.Alter](#rqsrs-006rbacquotaalter)
      * 5.2.8.206 [RQ.SRS-006.RBAC.Quota.Alter.IfExists](#rqsrs-006rbacquotaalterifexists)
      * 5.2.8.207 [RQ.SRS-006.RBAC.Quota.Alter.Rename](#rqsrs-006rbacquotaalterrename)
      * 5.2.8.208 [RQ.SRS-006.RBAC.Quota.Alter.Cluster](#rqsrs-006rbacquotaaltercluster)
      * 5.2.8.209 [RQ.SRS-006.RBAC.Quota.Alter.Interval](#rqsrs-006rbacquotaalterinterval)
      * 5.2.8.210 [RQ.SRS-006.RBAC.Quota.Alter.Interval.Randomized](#rqsrs-006rbacquotaalterintervalrandomized)
      * 5.2.8.211 [RQ.SRS-006.RBAC.Quota.Alter.Queries](#rqsrs-006rbacquotaalterqueries)
      * 5.2.8.212 [RQ.SRS-006.RBAC.Quota.Alter.Errors](#rqsrs-006rbacquotaaltererrors)
      * 5.2.8.213 [RQ.SRS-006.RBAC.Quota.Alter.ResultRows](#rqsrs-006rbacquotaalterresultrows)
      * 5.2.8.214 [RQ.SRS-006.RBAC.Quota.Alter.ReadRows](#rqsrs-006rbacquotaalterreadrows)
      * 5.2.8.215 [RQ.SRS-006.RBAC.Quota.ALter.ResultBytes](#rqsrs-006rbacquotaalterresultbytes)
      * 5.2.8.216 [RQ.SRS-006.RBAC.Quota.Alter.ReadBytes](#rqsrs-006rbacquotaalterreadbytes)
      * 5.2.8.217 [RQ.SRS-006.RBAC.Quota.Alter.ExecutionTime](#rqsrs-006rbacquotaalterexecutiontime)
      * 5.2.8.218 [RQ.SRS-006.RBAC.Quota.Alter.NoLimits](#rqsrs-006rbacquotaalternolimits)
      * 5.2.8.219 [RQ.SRS-006.RBAC.Quota.Alter.TrackingOnly](#rqsrs-006rbacquotaaltertrackingonly)
      * 5.2.8.220 [RQ.SRS-006.RBAC.Quota.Alter.KeyedBy](#rqsrs-006rbacquotaalterkeyedby)
      * 5.2.8.221 [RQ.SRS-006.RBAC.Quota.Alter.KeyedByOptions](#rqsrs-006rbacquotaalterkeyedbyoptions)
      * 5.2.8.222 [RQ.SRS-006.RBAC.Quota.Alter.Assignment](#rqsrs-006rbacquotaalterassignment)
      * 5.2.8.223 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.None](#rqsrs-006rbacquotaalterassignmentnone)
      * 5.2.8.224 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.All](#rqsrs-006rbacquotaalterassignmentall)
      * 5.2.8.225 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.Except](#rqsrs-006rbacquotaalterassignmentexcept)
      * 5.2.8.226 [RQ.SRS-006.RBAC.Quota.Alter.Syntax](#rqsrs-006rbacquotaaltersyntax)
      * 5.2.8.227 [RQ.SRS-006.RBAC.Quota.Drop](#rqsrs-006rbacquotadrop)
      * 5.2.8.228 [RQ.SRS-006.RBAC.Quota.Drop.IfExists](#rqsrs-006rbacquotadropifexists)
      * 5.2.8.229 [RQ.SRS-006.RBAC.Quota.Drop.Cluster](#rqsrs-006rbacquotadropcluster)
      * 5.2.8.230 [RQ.SRS-006.RBAC.Quota.Drop.Syntax](#rqsrs-006rbacquotadropsyntax)
      * 5.2.8.231 [RQ.SRS-006.RBAC.Quota.ShowQuotas](#rqsrs-006rbacquotashowquotas)
      * 5.2.8.232 [RQ.SRS-006.RBAC.Quota.ShowQuotas.IntoOutfile](#rqsrs-006rbacquotashowquotasintooutfile)
      * 5.2.8.233 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Format](#rqsrs-006rbacquotashowquotasformat)
      * 5.2.8.234 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Settings](#rqsrs-006rbacquotashowquotassettings)
      * 5.2.8.235 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Syntax](#rqsrs-006rbacquotashowquotassyntax)
      * 5.2.8.236 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Name](#rqsrs-006rbacquotashowcreatequotaname)
      * 5.2.8.237 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Current](#rqsrs-006rbacquotashowcreatequotacurrent)
      * 5.2.8.238 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Syntax](#rqsrs-006rbacquotashowcreatequotasyntax)
      * 5.2.8.239 [RQ.SRS-006.RBAC.RowPolicy.Create](#rqsrs-006rbacrowpolicycreate)
      * 5.2.8.240 [RQ.SRS-006.RBAC.RowPolicy.Create.IfNotExists](#rqsrs-006rbacrowpolicycreateifnotexists)
      * 5.2.8.241 [RQ.SRS-006.RBAC.RowPolicy.Create.Replace](#rqsrs-006rbacrowpolicycreatereplace)
      * 5.2.8.242 [RQ.SRS-006.RBAC.RowPolicy.Create.OnCluster](#rqsrs-006rbacrowpolicycreateoncluster)
      * 5.2.8.243 [RQ.SRS-006.RBAC.RowPolicy.Create.On](#rqsrs-006rbacrowpolicycreateon)
      * 5.2.8.244 [RQ.SRS-006.RBAC.RowPolicy.Create.Access](#rqsrs-006rbacrowpolicycreateaccess)
      * 5.2.8.245 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Permissive](#rqsrs-006rbacrowpolicycreateaccesspermissive)
      * 5.2.8.246 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Restrictive](#rqsrs-006rbacrowpolicycreateaccessrestrictive)
      * 5.2.8.247 [RQ.SRS-006.RBAC.RowPolicy.Create.ForSelect](#rqsrs-006rbacrowpolicycreateforselect)
      * 5.2.8.248 [RQ.SRS-006.RBAC.RowPolicy.Create.Condition](#rqsrs-006rbacrowpolicycreatecondition)
      * 5.2.8.249 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment](#rqsrs-006rbacrowpolicycreateassignment)
      * 5.2.8.250 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.None](#rqsrs-006rbacrowpolicycreateassignmentnone)
      * 5.2.8.251 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.All](#rqsrs-006rbacrowpolicycreateassignmentall)
      * 5.2.8.252 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.AllExcept](#rqsrs-006rbacrowpolicycreateassignmentallexcept)
      * 5.2.8.253 [RQ.SRS-006.RBAC.RowPolicy.Create.Syntax](#rqsrs-006rbacrowpolicycreatesyntax)
      * 5.2.8.254 [RQ.SRS-006.RBAC.RowPolicy.Alter](#rqsrs-006rbacrowpolicyalter)
      * 5.2.8.255 [RQ.SRS-006.RBAC.RowPolicy.Alter.IfExists](#rqsrs-006rbacrowpolicyalterifexists)
      * 5.2.8.256 [RQ.SRS-006.RBAC.RowPolicy.Alter.ForSelect](#rqsrs-006rbacrowpolicyalterforselect)
      * 5.2.8.257 [RQ.SRS-006.RBAC.RowPolicy.Alter.OnCluster](#rqsrs-006rbacrowpolicyalteroncluster)
      * 5.2.8.258 [RQ.SRS-006.RBAC.RowPolicy.Alter.On](#rqsrs-006rbacrowpolicyalteron)
      * 5.2.8.259 [RQ.SRS-006.RBAC.RowPolicy.Alter.Rename](#rqsrs-006rbacrowpolicyalterrename)
      * 5.2.8.260 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access](#rqsrs-006rbacrowpolicyalteraccess)
      * 5.2.8.261 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive](#rqsrs-006rbacrowpolicyalteraccesspermissive)
      * 5.2.8.262 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive](#rqsrs-006rbacrowpolicyalteraccessrestrictive)
      * 5.2.8.263 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition](#rqsrs-006rbacrowpolicyaltercondition)
      * 5.2.8.264 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.None](#rqsrs-006rbacrowpolicyalterconditionnone)
      * 5.2.8.265 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment](#rqsrs-006rbacrowpolicyalterassignment)
      * 5.2.8.266 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.None](#rqsrs-006rbacrowpolicyalterassignmentnone)
      * 5.2.8.267 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.All](#rqsrs-006rbacrowpolicyalterassignmentall)
      * 5.2.8.268 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.AllExcept](#rqsrs-006rbacrowpolicyalterassignmentallexcept)
      * 5.2.8.269 [RQ.SRS-006.RBAC.RowPolicy.Alter.Syntax](#rqsrs-006rbacrowpolicyaltersyntax)
      * 5.2.8.270 [RQ.SRS-006.RBAC.RowPolicy.Drop](#rqsrs-006rbacrowpolicydrop)
      * 5.2.8.271 [RQ.SRS-006.RBAC.RowPolicy.Drop.IfExists](#rqsrs-006rbacrowpolicydropifexists)
      * 5.2.8.272 [RQ.SRS-006.RBAC.RowPolicy.Drop.On](#rqsrs-006rbacrowpolicydropon)
      * 5.2.8.273 [RQ.SRS-006.RBAC.RowPolicy.Drop.OnCluster](#rqsrs-006rbacrowpolicydroponcluster)
      * 5.2.8.274 [RQ.SRS-006.RBAC.RowPolicy.Drop.Syntax](#rqsrs-006rbacrowpolicydropsyntax)
      * 5.2.8.275 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy](#rqsrs-006rbacrowpolicyshowcreaterowpolicy)
      * 5.2.8.276 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.On](#rqsrs-006rbacrowpolicyshowcreaterowpolicyon)
      * 5.2.8.277 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.Syntax](#rqsrs-006rbacrowpolicyshowcreaterowpolicysyntax)
      * 5.2.8.278 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies](#rqsrs-006rbacrowpolicyshowrowpolicies)
      * 5.2.8.279 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.On](#rqsrs-006rbacrowpolicyshowrowpolicieson)
      * 5.2.8.280 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.Syntax](#rqsrs-006rbacrowpolicyshowrowpoliciessyntax)
    * 5.2.9 [Table Privileges](#table-privileges)
      * 5.2.9.1 [RQ.SRS-006.RBAC.Table.PublicTables](#rqsrs-006rbactablepublictables)
      * 5.2.9.2 [RQ.SRS-006.RBAC.Table.ShowTables](#rqsrs-006rbactableshowtables)
      * 5.2.9.3 [Distributed Tables](#distributed-tables)
        * 5.2.9.3.1 [RQ.SRS-006.RBAC.Table.DistributedTable.Create](#rqsrs-006rbactabledistributedtablecreate)
        * 5.2.9.3.2 [RQ.SRS-006.RBAC.Table.DistributedTable.Select](#rqsrs-006rbactabledistributedtableselect)
        * 5.2.9.3.3 [RQ.SRS-006.RBAC.Table.DistributedTable.Insert](#rqsrs-006rbactabledistributedtableinsert)
        * 5.2.9.3.4 [RQ.SRS-006.RBAC.Table.DistributedTable.SpecialTables](#rqsrs-006rbactabledistributedtablespecialtables)
        * 5.2.9.3.5 [RQ.SRS-006.RBAC.Table.DistributedTable.LocalUser](#rqsrs-006rbactabledistributedtablelocaluser)
        * 5.2.9.3.6 [RQ.SRS-006.RBAC.Table.DistributedTable.SameUserDifferentNodesDifferentPrivileges](#rqsrs-006rbactabledistributedtablesameuserdifferentnodesdifferentprivileges)
    * 5.2.10 [Views](#views)
      * 5.2.10.1 [View](#view)
        * 5.2.10.1.1 [RQ.SRS-006.RBAC.View](#rqsrs-006rbacview)
        * 5.2.10.1.2 [RQ.SRS-006.RBAC.View.Create](#rqsrs-006rbacviewcreate)
        * 5.2.10.1.3 [RQ.SRS-006.RBAC.View.Select](#rqsrs-006rbacviewselect)
        * 5.2.10.1.4 [RQ.SRS-006.RBAC.View.Drop](#rqsrs-006rbacviewdrop)
      * 5.2.10.2 [Materialized View](#materialized-view)
        * 5.2.10.2.1 [RQ.SRS-006.RBAC.MaterializedView](#rqsrs-006rbacmaterializedview)
        * 5.2.10.2.2 [RQ.SRS-006.RBAC.MaterializedView.Create](#rqsrs-006rbacmaterializedviewcreate)
        * 5.2.10.2.3 [RQ.SRS-006.RBAC.MaterializedView.Select](#rqsrs-006rbacmaterializedviewselect)
        * 5.2.10.2.4 [RQ.SRS-006.RBAC.MaterializedView.Select.TargetTable](#rqsrs-006rbacmaterializedviewselecttargettable)
        * 5.2.10.2.5 [RQ.SRS-006.RBAC.MaterializedView.Select.SourceTable](#rqsrs-006rbacmaterializedviewselectsourcetable)
        * 5.2.10.2.6 [RQ.SRS-006.RBAC.MaterializedView.Drop](#rqsrs-006rbacmaterializedviewdrop)
        * 5.2.10.2.7 [RQ.SRS-006.RBAC.MaterializedView.ModifyQuery](#rqsrs-006rbacmaterializedviewmodifyquery)
        * 5.2.10.2.8 [RQ.SRS-006.RBAC.MaterializedView.Insert](#rqsrs-006rbacmaterializedviewinsert)
        * 5.2.10.2.9 [RQ.SRS-006.RBAC.MaterializedView.Insert.SourceTable](#rqsrs-006rbacmaterializedviewinsertsourcetable)
        * 5.2.10.2.10 [RQ.SRS-006.RBAC.MaterializedView.Insert.TargetTable](#rqsrs-006rbacmaterializedviewinserttargettable)
      * 5.2.10.3 [Live View](#live-view)
        * 5.2.10.3.1 [RQ.SRS-006.RBAC.LiveView](#rqsrs-006rbacliveview)
        * 5.2.10.3.2 [RQ.SRS-006.RBAC.LiveView.Create](#rqsrs-006rbacliveviewcreate)
        * 5.2.10.3.3 [RQ.SRS-006.RBAC.LiveView.Select](#rqsrs-006rbacliveviewselect)
        * 5.2.10.3.4 [RQ.SRS-006.RBAC.LiveView.Drop](#rqsrs-006rbacliveviewdrop)
        * 5.2.10.3.5 [RQ.SRS-006.RBAC.LiveView.Refresh](#rqsrs-006rbacliveviewrefresh)
    * 5.2.11 [Privileges](#privileges)
      * 5.2.11.1 [RQ.SRS-006.RBAC.Privileges.Usage](#rqsrs-006rbacprivilegesusage)
      * 5.2.11.2 [Select](#select)
        * 5.2.11.2.1 [RQ.SRS-006.RBAC.Privileges.Select](#rqsrs-006rbacprivilegesselect)
        * 5.2.11.2.2 [RQ.SRS-006.RBAC.Privileges.Select.Grant](#rqsrs-006rbacprivilegesselectgrant)
        * 5.2.11.2.3 [RQ.SRS-006.RBAC.Privileges.Select.Revoke](#rqsrs-006rbacprivilegesselectrevoke)
        * 5.2.11.2.4 [RQ.SRS-006.RBAC.Privileges.Select.Column](#rqsrs-006rbacprivilegesselectcolumn)
        * 5.2.11.2.5 [RQ.SRS-006.RBAC.Privileges.Select.Cluster](#rqsrs-006rbacprivilegesselectcluster)
        * 5.2.11.2.6 [RQ.SRS-006.RBAC.Privileges.Select.TableEngines](#rqsrs-006rbacprivilegesselecttableengines)
      * 5.2.11.3 [Insert](#insert)
        * 5.2.11.3.1 [RQ.SRS-006.RBAC.Privileges.Insert](#rqsrs-006rbacprivilegesinsert)
        * 5.2.11.3.2 [RQ.SRS-006.RBAC.Privileges.Insert.Grant](#rqsrs-006rbacprivilegesinsertgrant)
        * 5.2.11.3.3 [RQ.SRS-006.RBAC.Privileges.Insert.Revoke](#rqsrs-006rbacprivilegesinsertrevoke)
        * 5.2.11.3.4 [RQ.SRS-006.RBAC.Privileges.Insert.Column](#rqsrs-006rbacprivilegesinsertcolumn)
        * 5.2.11.3.5 [RQ.SRS-006.RBAC.Privileges.Insert.Cluster](#rqsrs-006rbacprivilegesinsertcluster)
        * 5.2.11.3.6 [RQ.SRS-006.RBAC.Privileges.Insert.TableEngines](#rqsrs-006rbacprivilegesinserttableengines)
      * 5.2.11.4 [AlterColumn](#altercolumn)
        * 5.2.11.4.1 [RQ.SRS-006.RBAC.Privileges.AlterColumn](#rqsrs-006rbacprivilegesaltercolumn)
        * 5.2.11.4.2 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Grant](#rqsrs-006rbacprivilegesaltercolumngrant)
        * 5.2.11.4.3 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Revoke](#rqsrs-006rbacprivilegesaltercolumnrevoke)
        * 5.2.11.4.4 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Column](#rqsrs-006rbacprivilegesaltercolumncolumn)
        * 5.2.11.4.5 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Cluster](#rqsrs-006rbacprivilegesaltercolumncluster)
        * 5.2.11.4.6 [RQ.SRS-006.RBAC.Privileges.AlterColumn.TableEngines](#rqsrs-006rbacprivilegesaltercolumntableengines)
      * 5.2.11.5 [AlterIndex](#alterindex)
        * 5.2.11.5.1 [RQ.SRS-006.RBAC.Privileges.AlterIndex](#rqsrs-006rbacprivilegesalterindex)
        * 5.2.11.5.2 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Grant](#rqsrs-006rbacprivilegesalterindexgrant)
        * 5.2.11.5.3 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Revoke](#rqsrs-006rbacprivilegesalterindexrevoke)
        * 5.2.11.5.4 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Cluster](#rqsrs-006rbacprivilegesalterindexcluster)
        * 5.2.11.5.5 [RQ.SRS-006.RBAC.Privileges.AlterIndex.TableEngines](#rqsrs-006rbacprivilegesalterindextableengines)
      * 5.2.11.6 [AlterConstraint](#alterconstraint)
        * 5.2.11.6.1 [RQ.SRS-006.RBAC.Privileges.AlterConstraint](#rqsrs-006rbacprivilegesalterconstraint)
        * 5.2.11.6.2 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Grant](#rqsrs-006rbacprivilegesalterconstraintgrant)
        * 5.2.11.6.3 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Revoke](#rqsrs-006rbacprivilegesalterconstraintrevoke)
        * 5.2.11.6.4 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Cluster](#rqsrs-006rbacprivilegesalterconstraintcluster)
        * 5.2.11.6.5 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.TableEngines](#rqsrs-006rbacprivilegesalterconstrainttableengines)
      * 5.2.11.7 [AlterTTL](#alterttl)
        * 5.2.11.7.1 [RQ.SRS-006.RBAC.Privileges.AlterTTL](#rqsrs-006rbacprivilegesalterttl)
        * 5.2.11.7.2 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Grant](#rqsrs-006rbacprivilegesalterttlgrant)
        * 5.2.11.7.3 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Revoke](#rqsrs-006rbacprivilegesalterttlrevoke)
        * 5.2.11.7.4 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Cluster](#rqsrs-006rbacprivilegesalterttlcluster)
        * 5.2.11.7.5 [RQ.SRS-006.RBAC.Privileges.AlterTTL.TableEngines](#rqsrs-006rbacprivilegesalterttltableengines)
      * 5.2.11.8 [AlterSettings](#altersettings)
        * 5.2.11.8.1 [RQ.SRS-006.RBAC.Privileges.AlterSettings](#rqsrs-006rbacprivilegesaltersettings)
        * 5.2.11.8.2 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Grant](#rqsrs-006rbacprivilegesaltersettingsgrant)
        * 5.2.11.8.3 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Revoke](#rqsrs-006rbacprivilegesaltersettingsrevoke)
        * 5.2.11.8.4 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Cluster](#rqsrs-006rbacprivilegesaltersettingscluster)
        * 5.2.11.8.5 [RQ.SRS-006.RBAC.Privileges.AlterSettings.TableEngines](#rqsrs-006rbacprivilegesaltersettingstableengines)
      * 5.2.11.9 [Alter Update](#alter-update)
        * 5.2.11.9.1 [RQ.SRS-006.RBAC.Privileges.AlterUpdate](#rqsrs-006rbacprivilegesalterupdate)
        * 5.2.11.9.2 [RQ.SRS-006.RBAC.Privileges.AlterUpdate.Access](#rqsrs-006rbacprivilegesalterupdateaccess)
        * 5.2.11.9.3 [RQ.SRS-006.RBAC.Privileges.AlterUpdate.TableEngines](#rqsrs-006rbacprivilegesalterupdatetableengines)
      * 5.2.11.10 [Alter Delete](#alter-delete)
        * 5.2.11.10.1 [RQ.SRS-006.RBAC.Privileges.AlterDelete](#rqsrs-006rbacprivilegesalterdelete)
        * 5.2.11.10.2 [RQ.SRS-006.RBAC.Privileges.AlterDelete.Access](#rqsrs-006rbacprivilegesalterdeleteaccess)
        * 5.2.11.10.3 [RQ.SRS-006.RBAC.Privileges.AlterDelete.TableEngines](#rqsrs-006rbacprivilegesalterdeletetableengines)
      * 5.2.11.11 [Alter Freeze Partition](#alter-freeze-partition)
        * 5.2.11.11.1 [RQ.SRS-006.RBAC.Privileges.AlterFreeze](#rqsrs-006rbacprivilegesalterfreeze)
        * 5.2.11.11.2 [RQ.SRS-006.RBAC.Privileges.AlterFreeze.Access](#rqsrs-006rbacprivilegesalterfreezeaccess)
        * 5.2.11.11.3 [RQ.SRS-006.RBAC.Privileges.AlterFreeze.TableEngines](#rqsrs-006rbacprivilegesalterfreezetableengines)
      * 5.2.11.12 [Alter Fetch Partition](#alter-fetch-partition)
        * 5.2.11.12.1 [RQ.SRS-006.RBAC.Privileges.AlterFetch](#rqsrs-006rbacprivilegesalterfetch)
        * 5.2.11.12.2 [RQ.SRS-006.RBAC.Privileges.AlterFetch.Access](#rqsrs-006rbacprivilegesalterfetchaccess)
        * 5.2.11.12.3 [RQ.SRS-006.RBAC.Privileges.AlterFetch.TableEngines](#rqsrs-006rbacprivilegesalterfetchtableengines)
      * 5.2.11.13 [Alter Move Partition](#alter-move-partition)
        * 5.2.11.13.1 [RQ.SRS-006.RBAC.Privileges.AlterMove](#rqsrs-006rbacprivilegesaltermove)
        * 5.2.11.13.2 [RQ.SRS-006.RBAC.Privileges.AlterMove.Access](#rqsrs-006rbacprivilegesaltermoveaccess)
        * 5.2.11.13.3 [RQ.SRS-006.RBAC.Privileges.AlterMove.TableEngines](#rqsrs-006rbacprivilegesaltermovetableengines)
      * 5.2.11.14 [Grant Option](#grant-option)
        * 5.2.11.14.1 [RQ.SRS-006.RBAC.Privileges.GrantOption](#rqsrs-006rbacprivilegesgrantoption)
      * 5.2.11.15 [RQ.SRS-006.RBAC.Privileges.Delete](#rqsrs-006rbacprivilegesdelete)
      * 5.2.11.16 [RQ.SRS-006.RBAC.Privileges.Alter](#rqsrs-006rbacprivilegesalter)
      * 5.2.11.17 [RQ.SRS-006.RBAC.Privileges.Create](#rqsrs-006rbacprivilegescreate)
      * 5.2.11.18 [RQ.SRS-006.RBAC.Privileges.Drop](#rqsrs-006rbacprivilegesdrop)
      * 5.2.11.19 [RQ.SRS-006.RBAC.Privileges.All](#rqsrs-006rbacprivilegesall)
      * 5.2.11.20 [RQ.SRS-006.RBAC.Privileges.All.GrantRevoke](#rqsrs-006rbacprivilegesallgrantrevoke)
      * 5.2.11.21 [RQ.SRS-006.RBAC.Privileges.AdminOption](#rqsrs-006rbacprivilegesadminoption)
    * 5.2.12 [Required Privileges](#required-privileges)
      * 5.2.12.1 [RQ.SRS-006.RBAC.RequiredPrivileges.Create](#rqsrs-006rbacrequiredprivilegescreate)
      * 5.2.12.2 [RQ.SRS-006.RBAC.RequiredPrivileges.Alter](#rqsrs-006rbacrequiredprivilegesalter)
      * 5.2.12.3 [RQ.SRS-006.RBAC.RequiredPrivileges.Drop](#rqsrs-006rbacrequiredprivilegesdrop)
      * 5.2.12.4 [RQ.SRS-006.RBAC.RequiredPrivileges.Drop.Table](#rqsrs-006rbacrequiredprivilegesdroptable)
      * 5.2.12.5 [RQ.SRS-006.RBAC.RequiredPrivileges.GrantRevoke](#rqsrs-006rbacrequiredprivilegesgrantrevoke)
      * 5.2.12.6 [RQ.SRS-006.RBAC.RequiredPrivileges.Use](#rqsrs-006rbacrequiredprivilegesuse)
      * 5.2.12.7 [RQ.SRS-006.RBAC.RequiredPrivileges.Admin](#rqsrs-006rbacrequiredprivilegesadmin)
* 6 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a GitHub repository.

All the updates are tracked using the [Git]'s revision history.

* GitHub repository: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/rbac/requirements/requirements.md
* Revision history: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/rbac/requirements/requirements.md

## Introduction

[ClickHouse] currently has support for only basic access control. Users can be defined to allow
access to specific databases and dictionaries. A profile is used for the user
that can specify a read-only mode as well as a set of quotas that can limit user's resource
consumption. Beyond this basic functionality there is no way to control access rights within
a database. A user can either be denied access, have read-only rights or have complete access
to the whole database on the server.

In many cases a more granular access control is needed where one can control user's access in
a much more granular approach. A typical solution to this problem in the **SQL** world
is provided by implementing **RBAC (role-based access control)**.
For example a version of **RBAC** is implemented by both [MySQL] and [PostgreSQL].

[ClickHouse] shall implement **RBAC** to meet the growing needs of its users. In order to minimize
the learning curve the concepts and the syntax of its implementation shall be
as close as possible to the [MySQL] and [PostgreSQL]. The goal is to allow for fast
transition of users which are already familiar with these features in those databases
to [ClickHouse].

## Terminology

* **RBAC** -
  role-based access control
* **quota** -
  setting that limits specific resource consumption

## Privilege Definitions

* **usage** -
  privilege to access a database or a table
* **select** -
  privilege to read data from a database or a table
* **insert**
  privilege to insert data into a database or a table
* **delete**
  privilege to delete a database or a table
* **alter**
  privilege to alter tables
* **create**
  privilege to create a database or a table
* **drop**
  privilege to drop a database or a table
* **all**
  privilege that includes **usage**, **select**,
  **insert**, **delete**, **alter**, **create**, and **drop**
* **grant option**
  privilege to grant the same privilege to other users or roles
* **admin option**
  privilege to perform administrative tasks are defined in the **system queries**

## Requirements

### Generic

#### RQ.SRS-006.RBAC
version: 1.0

[ClickHouse] SHALL support role based access control.

#### Login

##### RQ.SRS-006.RBAC.Login
version: 1.0

[ClickHouse] SHALL only allow access to the server for a given
user only when correct username and password are used during
the connection to the server.

##### RQ.SRS-006.RBAC.Login.DefaultUser
version: 1.0

[ClickHouse] SHALL use the **default user** when no username and password
are specified during the connection to the server.

#### User

##### RQ.SRS-006.RBAC.User
version: 1.0

[ClickHouse] SHALL support creation and manipulation of
one or more **user** accounts to which roles, privileges,
settings profile, quotas and row policies can be assigned.

##### RQ.SRS-006.RBAC.User.Roles
version: 1.0

[ClickHouse] SHALL support assigning one or more **roles**
to a **user**.

##### RQ.SRS-006.RBAC.User.Privileges
version: 1.0

[ClickHouse] SHALL support assigning one or more privileges to a **user**.

##### RQ.SRS-006.RBAC.User.Variables
version: 1.0

[ClickHouse] SHALL support assigning one or more variables to a **user**.

##### RQ.SRS-006.RBAC.User.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support assigning min, max and read-only constraints
for the variables that can be set and read by the **user**.

##### RQ.SRS-006.RBAC.User.SettingsProfile
version: 1.0

[ClickHouse] SHALL support assigning one or more **settings profiles**
to a **user**.

##### RQ.SRS-006.RBAC.User.Quotas
version: 1.0

[ClickHouse] SHALL support assigning one or more **quotas** to a **user**.

##### RQ.SRS-006.RBAC.User.RowPolicies
version: 1.0

[ClickHouse] SHALL support assigning one or more **row policies** to a **user**.

##### RQ.SRS-006.RBAC.User.AccountLock
version: 1.0

[ClickHouse] SHALL support locking and unlocking of **user** accounts.

##### RQ.SRS-006.RBAC.User.AccountLock.DenyAccess
version: 1.0

[ClickHouse] SHALL deny access to the user whose account is locked.

##### RQ.SRS-006.RBAC.User.DefaultRole
version: 1.0

[ClickHouse] SHALL support assigning a default role to a **user**.

##### RQ.SRS-006.RBAC.User.RoleSelection
version: 1.0

[ClickHouse] SHALL support selection of one or more **roles** from the available roles
that are assigned to a **user**.

##### RQ.SRS-006.RBAC.User.ShowCreate
version: 1.0

[ClickHouse] SHALL support showing the command of how **user** account was created.

##### RQ.SRS-006.RBAC.User.ShowPrivileges
version: 1.0

[ClickHouse] SHALL support listing the privileges of the **user**.

#### Role

##### RQ.SRS-006.RBAC.Role
version: 1.0

[ClikHouse] SHALL support creation and manipulation of **roles**
to which privileges, settings profile, quotas and row policies can be
assigned.

##### RQ.SRS-006.RBAC.Role.Privileges
version: 1.0

[ClickHouse] SHALL support assigning one or more privileges to a **role**.

##### RQ.SRS-006.RBAC.Role.Variables
version: 1.0

[ClickHouse] SHALL support assigning one or more variables to a **role**.

##### RQ.SRS-006.RBAC.Role.SettingsProfile
version: 1.0

[ClickHouse] SHALL support assigning one or more **settings profiles**
to a **role**.

##### RQ.SRS-006.RBAC.Role.Quotas
version: 1.0

[ClickHouse] SHALL support assigning one or more **quotas** to a **role**.

##### RQ.SRS-006.RBAC.Role.RowPolicies
version: 1.0

[ClickHouse] SHALL support assigning one or more **row policies** to a **role**.

#### Partial Revokes

##### RQ.SRS-006.RBAC.PartialRevokes
version: 1.0

[ClickHouse] SHALL support partial revoking of privileges granted
to a **user** or a **role**.

#### Settings Profile

##### RQ.SRS-006.RBAC.SettingsProfile
version: 1.0

[ClickHouse] SHALL support creation and manipulation of **settings profiles**
that can include value definition for one or more variables and can
can be assigned to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.SettingsProfile.Constraints
version: 1.0

[ClickHouse] SHALL support assigning min, max and read-only constraints
for the variables specified in the **settings profile**.

##### RQ.SRS-006.RBAC.SettingsProfile.ShowCreate
version: 1.0

[ClickHouse] SHALL support showing the command of how **setting profile** was created.

#### Quotas

##### RQ.SRS-006.RBAC.Quotas
version: 1.0

[ClickHouse] SHALL support creation and manipulation of **quotas**
that can be used to limit resource usage by a **user** or a **role**
over a period of time.

##### RQ.SRS-006.RBAC.Quotas.Keyed
version: 1.0

[ClickHouse] SHALL support creating **quotas** that are keyed
so that a quota is tracked separately for each key value.

##### RQ.SRS-006.RBAC.Quotas.Queries
version: 1.0

[ClickHouse] SHALL support setting **queries** quota to limit the total number of requests.

##### RQ.SRS-006.RBAC.Quotas.Errors
version: 1.0

[ClickHouse] SHALL support setting **errors** quota to limit the number of queries that threw an exception.

##### RQ.SRS-006.RBAC.Quotas.ResultRows
version: 1.0

[ClickHouse] SHALL support setting **result rows** quota to limit the
the total number of rows given as the result.

##### RQ.SRS-006.RBAC.Quotas.ReadRows
version: 1.0

[ClickHouse] SHALL support setting **read rows** quota to limit the total
number of source rows read from tables for running the query on all remote servers.

##### RQ.SRS-006.RBAC.Quotas.ResultBytes
version: 1.0

[ClickHouse] SHALL support setting **result bytes** quota to limit the total number
of bytes that can be returned as the result.

##### RQ.SRS-006.RBAC.Quotas.ReadBytes
version: 1.0

[ClickHouse] SHALL support setting **read bytes** quota to limit the total number
of source bytes read from tables for running the query on all remote servers.

##### RQ.SRS-006.RBAC.Quotas.ExecutionTime
version: 1.0

[ClickHouse] SHALL support setting **execution time** quota to limit the maximum
query execution time.

##### RQ.SRS-006.RBAC.Quotas.ShowCreate
version: 1.0

[ClickHouse] SHALL support showing the command of how **quota** was created.

#### Row Policy

##### RQ.SRS-006.RBAC.RowPolicy
version: 1.0

[ClickHouse] SHALL support creation and manipulation of table **row policies**
that can be used to limit access to the table contents for a **user** or a **role**
using a specified **condition**.

##### RQ.SRS-006.RBAC.RowPolicy.Condition
version: 1.0

[ClickHouse] SHALL support row policy **conditions** that can be any SQL
expression that returns a boolean.

##### RQ.SRS-006.RBAC.RowPolicy.ShowCreate
version: 1.0

[ClickHouse] SHALL support showing the command of how **row policy** was created.

### Specific

##### RQ.SRS-006.RBAC.User.Use.DefaultRole
version: 1.0

[ClickHouse] SHALL by default use default role or roles assigned
to the user if specified.

##### RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole
version: 1.0

[ClickHouse] SHALL by default use all the roles assigned to the user
if no default role or roles are specified for the user.

##### RQ.SRS-006.RBAC.User.Create
version: 1.0

[ClickHouse] SHALL support creating **user** accounts using `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.IfNotExists
version: 1.0

[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE USER` statement
to skip raising an exception if a user with the same **name** already exists.
If the `IF NOT EXISTS` clause is not specified then an exception SHALL be
raised if a user with the same **name** already exists.

##### RQ.SRS-006.RBAC.User.Create.Replace
version: 1.0

[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE USER` statement
to replace existing user account if already exists.

##### RQ.SRS-006.RBAC.User.Create.Password.NoPassword
version: 1.0

[ClickHouse] SHALL support specifying no password when creating
user account using `IDENTIFIED WITH NO_PASSWORD` clause .

##### RQ.SRS-006.RBAC.User.Create.Password.NoPassword.Login
version: 1.0

[ClickHouse] SHALL use no password for the user when connecting to the server
when an account was created with `IDENTIFIED WITH NO_PASSWORD` clause.

##### RQ.SRS-006.RBAC.User.Create.Password.PlainText
version: 1.0

[ClickHouse] SHALL support specifying plaintext password when creating
user account using `IDENTIFIED WITH PLAINTEXT_PASSWORD BY` clause.

##### RQ.SRS-006.RBAC.User.Create.Password.PlainText.Login
version: 1.0

[ClickHouse] SHALL use the plaintext password passed by the user when connecting to the server
when an account was created with `IDENTIFIED WITH PLAINTEXT_PASSWORD` clause
and compare the password with the one used in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Password.Sha256Password
version: 1.0

[ClickHouse] SHALL support specifying the result of applying SHA256
to some password when creating user account using `IDENTIFIED WITH SHA256_PASSWORD BY` or `IDENTIFIED BY`
clause.

##### RQ.SRS-006.RBAC.User.Create.Password.Sha256Password.Login
version: 1.0

[ClickHouse] SHALL calculate `SHA256` of the password passed by the user when connecting to the server
when an account was created with `IDENTIFIED WITH SHA256_PASSWORD` or with 'IDENTIFIED BY' clause
and compare the calculated hash to the one used in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash
version: 1.0

[ClickHouse] SHALL support specifying the result of applying SHA256
to some already calculated hash when creating user account using `IDENTIFIED WITH SHA256_HASH`
clause.

##### RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash.Login
version: 1.0

[ClickHouse] SHALL calculate `SHA256` of the already calculated hash passed by
the user when connecting to the server
when an account was created with `IDENTIFIED WITH SHA256_HASH` clause
and compare the calculated hash to the one used in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password
version: 1.0

[ClickHouse] SHALL support specifying the result of applying SHA1 two times
to a password when creating user account using `IDENTIFIED WITH DOUBLE_SHA1_PASSWORD`
clause.

##### RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password.Login
version: 1.0

[ClickHouse] SHALL calculate `SHA1` two times over the password passed by
the user when connecting to the server
when an account was created with `IDENTIFIED WITH DOUBLE_SHA1_PASSWORD` clause
and compare the calculated value to the one used in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash
version: 1.0

[ClickHouse] SHALL support specifying the result of applying SHA1 two times
to a hash when creating user account using `IDENTIFIED WITH DOUBLE_SHA1_HASH`
clause.

##### RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash.Login
version: 1.0

[ClickHouse] SHALL calculate `SHA1` two times over the hash passed by
the user when connecting to the server
when an account was created with `IDENTIFIED WITH DOUBLE_SHA1_HASH` clause
and compare the calculated value to the one used in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Name
version: 1.0

[ClickHouse] SHALL support specifying one or more hostnames from
which user can access the server using the `HOST NAME` clause
in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Regexp
version: 1.0

[ClickHouse] SHALL support specifying one or more regular expressions
to match hostnames from which user can access the server
using the `HOST REGEXP` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.IP
version: 1.0

[ClickHouse] SHALL support specifying one or more IP address or subnet from
which user can access the server using the `HOST IP` clause in the
`CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Any
version: 1.0

[ClickHouse] SHALL support specifying `HOST ANY` clause in the `CREATE USER` statement
to indicate that user can access the server from any host.

##### RQ.SRS-006.RBAC.User.Create.Host.None
version: 1.0

[ClickHouse] SHALL support fobidding access from any host using `HOST NONE` clause in the
`CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Local
version: 1.0

[ClickHouse] SHALL support limiting user access to local only using `HOST LOCAL` clause in the
`CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Like
version: 1.0

[ClickHouse] SHALL support specifying host using `LIKE` command syntax using the
`HOST LIKE` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Host.Default
version: 1.0

[ClickHouse] SHALL support user access to server from any host
if no `HOST` clause is specified in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.DefaultRole
version: 1.0

[ClickHouse] SHALL support specifying one or more default roles
using `DEFAULT ROLE` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.DefaultRole.None
version: 1.0

[ClickHouse] SHALL support specifying no default roles
using `DEFAULT ROLE NONE` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.DefaultRole.All
version: 1.0

[ClickHouse] SHALL support specifying all roles to be used as default
using `DEFAULT ROLE ALL` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Settings
version: 1.0

[ClickHouse] SHALL support specifying settings and profile
using `SETTINGS` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which the user
will be created using `ON CLUSTER` clause in the `CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.Create.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `CREATE USER` statement.

```sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | NAME REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

##### RQ.SRS-006.RBAC.User.Alter
version: 1.0

[ClickHouse] SHALL support altering **user** accounts using `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.OrderOfEvaluation
version: 1.0

[ClickHouse] SHALL support evaluating `ALTER USER` statement from left to right
where things defined on the right override anything that was previously defined on
the left.

##### RQ.SRS-006.RBAC.User.Alter.IfExists
version: 1.0

[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER USER` statement
to skip raising an exception (producing a warning instead) if a user with the specified **name** does not exist. If the `IF EXISTS` clause is not specified then an exception SHALL be raised if a user with the **name** does not exist.

##### RQ.SRS-006.RBAC.User.Alter.Cluster
version: 1.0

[ClickHouse] SHALL support specifying the cluster the user is on
when altering user account using `ON CLUSTER` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Rename
version: 1.0

[ClickHouse] SHALL support specifying a new name for the user when
altering user account using `RENAME` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Password.PlainText
version: 1.0

[ClickHouse] SHALL support specifying plaintext password when altering
user account using `IDENTIFIED WITH PLAINTEXT_PASSWORD BY` or
using shorthand `IDENTIFIED BY` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Password.Sha256Password
version: 1.0

[ClickHouse] SHALL support specifying the result of applying SHA256
to some password as identification when altering user account using
`IDENTIFIED WITH SHA256_PASSWORD` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Password.DoubleSha1Password
version: 1.0

[ClickHouse] SHALL support specifying the result of applying Double SHA1
to some password as identification when altering user account using
`IDENTIFIED WITH DOUBLE_SHA1_PASSWORD` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.AddDrop
version: 1.0

[ClickHouse] SHALL support altering user by adding and dropping access to hosts with the `ADD HOST` or the `DROP HOST`in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.Local
version: 1.0

[ClickHouse] SHALL support limiting user access to local only using `HOST LOCAL` clause in the
`ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.Name
version: 1.0

[ClickHouse] SHALL support specifying one or more hostnames from
which user can access the server using the `HOST NAME` clause
in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.Regexp
version: 1.0

[ClickHouse] SHALL support specifying one or more regular expressions
to match hostnames from which user can access the server
using the `HOST REGEXP` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.IP
version: 1.0

[ClickHouse] SHALL support specifying one or more IP address or subnet from
which user can access the server using the `HOST IP` clause in the
`ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.Like
version: 1.0

[ClickHouse] SHALL support specifying sone or more similar hosts using `LIKE` command syntax using the `HOST LIKE` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Host.Any
version: 1.0

[ClickHouse] SHALL support specifying `HOST ANY` clause in the `ALTER USER` statement
to indicate that user can access the server from any host.

##### RQ.SRS-006.RBAC.User.Alter.Host.None
version: 1.0

[ClickHouse] SHALL support fobidding access from any host using `HOST NONE` clause in the
`ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.DefaultRole
version: 1.0

[ClickHouse] SHALL support specifying one or more default roles
using `DEFAULT ROLE` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.DefaultRole.All
version: 1.0

[ClickHouse] SHALL support specifying all roles to be used as default
using `DEFAULT ROLE ALL` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.DefaultRole.AllExcept
version: 1.0

[ClickHouse] SHALL support specifying one or more roles which will not be used as default
using `DEFAULT ROLE ALL EXCEPT` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Settings
version: 1.0

[ClickHouse] SHALL support specifying one or more variables
using `SETTINGS` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Settings.Min
version: 1.0

[ClickHouse] SHALL support specifying a minimum value for the variable specifed using `SETTINGS` with `MIN` clause in the `ALTER USER` statement.


##### RQ.SRS-006.RBAC.User.Alter.Settings.Max
version: 1.0

[ClickHouse] SHALL support specifying a maximum value for the variable specifed using `SETTINGS` with `MAX` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Settings.Profile
version: 1.0

[ClickHouse] SHALL support specifying the name of a profile for the variable specifed using `SETTINGS` with `PROFILE` clause in the `ALTER USER` statement.

##### RQ.SRS-006.RBAC.User.Alter.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `ALTER USER` statement.

```sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

##### RQ.SRS-006.RBAC.SetDefaultRole
version: 1.0

[ClickHouse] SHALL support setting or changing granted roles to default for one or more
users using `SET DEFAULT ROLE` statement which
SHALL permanently change the default roles for the user or users if successful.

##### RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser
version: 1.0

[ClickHouse] SHALL support setting or changing granted roles to default for
the current user using `CURRENT_USER` clause in the `SET DEFAULT ROLE` statement.

##### RQ.SRS-006.RBAC.SetDefaultRole.All
version: 1.0

[ClickHouse] SHALL support setting or changing all granted roles to default
for one or more users using `ALL` clause in the `SET DEFAULT ROLE` statement.

##### RQ.SRS-006.RBAC.SetDefaultRole.AllExcept
version: 1.0

[ClickHouse] SHALL support setting or changing all granted roles except those specified
to default for one or more users using `ALL EXCEPT` clause in the `SET DEFAULT ROLE` statement.

##### RQ.SRS-006.RBAC.SetDefaultRole.None
version: 1.0

[ClickHouse] SHALL support removing all granted roles from default
for one or more users using `NONE` clause in the `SET DEFAULT ROLE` statement.

##### RQ.SRS-006.RBAC.SetDefaultRole.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `SET DEFAULT ROLE` statement.

```sql
SET DEFAULT ROLE
    {NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
    TO {user|CURRENT_USER} [,...]

```

##### RQ.SRS-006.RBAC.SetRole
version: 1.0

[ClickHouse] SHALL support activating role or roles for the current user
using `SET ROLE` statement.

##### RQ.SRS-006.RBAC.SetRole.Default
version: 1.0

[ClickHouse] SHALL support activating default roles for the current user
using `DEFAULT` clause in the `SET ROLE` statement.

##### RQ.SRS-006.RBAC.SetRole.None
version: 1.0

[ClickHouse] SHALL support activating no roles for the current user
using `NONE` clause in the `SET ROLE` statement.

##### RQ.SRS-006.RBAC.SetRole.All
version: 1.0

[ClickHouse] SHALL support activating all roles for the current user
using `ALL` clause in the `SET ROLE` statement.

##### RQ.SRS-006.RBAC.SetRole.AllExcept
version: 1.0

[ClickHouse] SHALL support activating all roles except those specified
for the current user using `ALL EXCEPT` clause in the `SET ROLE` statement.

##### RQ.SRS-006.RBAC.SetRole.Syntax
version: 1.0

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

##### RQ.SRS-006.RBAC.User.ShowCreateUser
version: 1.0

[ClickHouse] SHALL support showing the `CREATE USER` statement used to create the current user object
using the `SHOW CREATE USER` statement with `CURRENT_USER` or no argument.

##### RQ.SRS-006.RBAC.User.ShowCreateUser.For
version: 1.0

[ClickHouse] SHALL support showing the `CREATE USER` statement used to create the specified user object
using the `FOR` clause in the `SHOW CREATE USER` statement.

##### RQ.SRS-006.RBAC.User.ShowCreateUser.Syntax
version: 1.0

[ClickHouse] SHALL support showing the following syntax for `SHOW CREATE USER` statement.

```sql
SHOW CREATE USER [name | CURRENT_USER]
```

##### RQ.SRS-006.RBAC.User.Drop
version: 1.0

[ClickHouse] SHALL support removing a user account using `DROP USER` statement.

##### RQ.SRS-006.RBAC.User.Drop.IfExists
version: 1.0

[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP USER` statement
to skip raising an exception if the user account does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if a user does not exist.

##### RQ.SRS-006.RBAC.User.Drop.OnCluster
version: 1.0

[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP USER` statement
to specify the name of the cluster the user should be dropped from.

##### RQ.SRS-006.RBAC.User.Drop.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `DROP USER` statement

```sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

##### RQ.SRS-006.RBAC.Role.Create
version: 1.0

[ClickHouse] SHALL support creating a **role** using `CREATE ROLE` statement.

##### RQ.SRS-006.RBAC.Role.Create.IfNotExists
version: 1.0

[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE ROLE` statement
to raising an exception if a role with the same **name** already exists.
If the `IF NOT EXISTS` clause is not specified then an exception SHALL be
raised if a role with the same **name** already exists.

##### RQ.SRS-006.RBAC.Role.Create.Replace
version: 1.0

[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE ROLE` statement
to replace existing role if it already exists.

##### RQ.SRS-006.RBAC.Role.Create.Settings
version: 1.0

[ClickHouse] SHALL support specifying settings and profile using `SETTINGS`
clause in the `CREATE ROLE` statement.

##### RQ.SRS-006.RBAC.Role.Create.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `CREATE ROLE` statement

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

##### RQ.SRS-006.RBAC.Role.Alter
version: 1.0

[ClickHouse] SHALL support altering one **role** using `ALTER ROLE` statement.

##### RQ.SRS-006.RBAC.Role.Alter.IfExists
version: 1.0

[ClickHouse] SHALL support altering one **role** using `ALTER ROLE IF EXISTS` statement, where no exception
will be thrown if the role does not exist.

##### RQ.SRS-006.RBAC.Role.Alter.Cluster
version: 1.0

[ClickHouse] SHALL support altering one **role** using `ALTER ROLE role ON CLUSTER` statement to specify the
cluster location of the specified role.

##### RQ.SRS-006.RBAC.Role.Alter.Rename
version: 1.0

[ClickHouse] SHALL support altering one **role** using `ALTER ROLE role RENAME TO` statement which renames the
role to a specified new name. If the new name already exists, that an exception SHALL be raised unless the
`IF EXISTS` clause is specified, by which no exception will be raised and nothing will change.

##### RQ.SRS-006.RBAC.Role.Alter.Settings
version: 1.0

[ClickHouse] SHALL support altering the settings of one **role** using `ALTER ROLE role SETTINGS ...` statement.
Altering variable values, creating max and min values, specifying readonly or writable, and specifying the
profiles for which this alter change shall be applied to, are all supported, using the following syntax.

```sql
[SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

One or more variables and profiles may be specified as shown above.

##### RQ.SRS-006.RBAC.Role.Alter.Syntax
version: 1.0

```sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

##### RQ.SRS-006.RBAC.Role.Drop
version: 1.0

[ClickHouse] SHALL support removing one or more roles using `DROP ROLE` statement.

##### RQ.SRS-006.RBAC.Role.Drop.IfExists
version: 1.0

[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP ROLE` statement
to skip raising an exception if the role does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if a role does not exist.

##### RQ.SRS-006.RBAC.Role.Drop.Cluster
version: 1.0

[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP ROLE` statement to specify the cluster from which to drop the specified role.

##### RQ.SRS-006.RBAC.Role.Drop.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `DROP ROLE` statement

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

##### RQ.SRS-006.RBAC.Role.ShowCreate
version: 1.0

[ClickHouse] SHALL support viewing the settings for a role upon creation with the `SHOW CREATE ROLE`
statement.

##### RQ.SRS-006.RBAC.Role.ShowCreate.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `SHOW CREATE ROLE` command.

```sql
SHOW CREATE ROLE name
```

##### RQ.SRS-006.RBAC.Grant.Privilege.To
version: 1.0

[ClickHouse] SHALL support granting privileges to one or more users or roles using `TO` clause
in the `GRANT PRIVILEGE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser
version: 1.0

[ClickHouse] SHALL support granting privileges to current user using `TO CURRENT_USER` clause
in the `GRANT PRIVILEGE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Select
version: 1.0

[ClickHouse] SHALL support granting the **select** privilege to one or more users or roles
for a database or a table using the `GRANT SELECT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support granting the **insert** privilege to one or more users or roles
for a database or a table using the `GRANT INSERT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support granting the **alter** privilege to one or more users or roles
for a database or a table using the `GRANT ALTER` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Create
version: 1.0

[ClickHouse] SHALL support granting the **create** privilege to one or more users or roles
for a database or a table using the `GRANT CREATE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support granting the **drop** privilege to one or more users or roles
for a database or a table using the `GRANT DROP` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support granting the **truncate** privilege to one or more users or roles
for a database or a table using `GRANT TRUNCATE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support granting the **optimize** privilege to one or more users or roles
for a database or a table using `GRANT OPTIMIZE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Show
version: 1.0

[ClickHouse] SHALL support granting the **show** privilege to one or more users or roles
for a database or a table using `GRANT SHOW` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support granting the **kill query** privilege to one or more users or roles
for a database or a table using `GRANT KILL QUERY` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support granting the **access management** privileges to one or more users or roles
for a database or a table using `GRANT ACCESS MANAGEMENT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.System
version: 1.0

[ClickHouse] SHALL support granting the **system** privileges to one or more users or roles
for a database or a table using `GRANT SYSTEM` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support granting the **introspection** privileges to one or more users or roles
for a database or a table using `GRANT INTROSPECTION` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support granting the **sources** privileges to one or more users or roles
for a database or a table using `GRANT SOURCES` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support granting the **dictGet** privilege to one or more users or roles
for a database or a table using `GRANT dictGet` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.None
version: 1.0

[ClickHouse] SHALL support granting no privileges to one or more users or roles
for a database or a table using `GRANT NONE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.All
version: 1.0

[ClickHouse] SHALL support granting the **all** privileges to one or more users or roles
for a database or a table using the `GRANT ALL` or `GRANT ALL PRIVILEGES` statements.

##### RQ.SRS-006.RBAC.Grant.Privilege.GrantOption
version: 1.0

[ClickHouse] SHALL support granting the **grant option** privilege to one or more users or roles
for a database or a table using the `WITH GRANT OPTION` clause in the `GRANT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.On
version: 1.0

[ClickHouse] SHALL support the `ON` clause in the `GRANT` privilege statement
which SHALL allow to specify one or more tables to which the privilege SHALL
be granted using the following patterns

* `*.*` any table in any database
* `database.*` any table in the specified database
* `database.table` specific table in the specified database
* `*` any table in the current database
* `table` specific table in the current database

##### RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns
version: 1.0

[ClickHouse] SHALL support granting the privilege **some_privilege** to one or more users or roles
for a database or a table using the `GRANT some_privilege(column)` statement for one column.
Multiple columns will be supported with `GRANT some_privilege(column1, column2...)` statement.
The privileges will be granted for only the specified columns.

##### RQ.SRS-006.RBAC.Grant.Privilege.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which to grant privileges using the `ON CLUSTER`
clause in the `GRANT PRIVILEGE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `GRANT` statement that
grants explicit privileges to a user or a role.

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...]
    ON {db.table|db.*|*.*|table|*}
    TO {user | role | CURRENT_USER} [,...]
    [WITH GRANT OPTION]
```

##### RQ.SRS-006.RBAC.Revoke.Privilege.Cluster
version: 1.0

[ClickHouse] SHALL support revoking privileges to one or more users or roles
for a database or a table on some specific cluster using the `REVOKE ON CLUSTER cluster_name` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Any
version: 1.0

[ClickHouse] SHALL support revoking ANY privilege to one or more users or roles
for a database or a table using the `REVOKE some_privilege` statement.
**some_privilege** refers to any Clickhouse defined privilege, whose hierarchy includes
SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,
SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Select
version: 1.0

[ClickHouse] SHALL support revoking the **select** privilege to one or more users or roles
for a database or a table using the `REVOKE SELECT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support revoking the **insert** privilege to one or more users or roles
for a database or a table using the `REVOKE INSERT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support revoking the **alter** privilege to one or more users or roles
for a database or a table using the `REVOKE ALTER` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Create
version: 1.0

[ClickHouse] SHALL support revoking the **create** privilege to one or more users or roles
for a database or a table using the `REVOKE CREATE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support revoking the **drop** privilege to one or more users or roles
for a database or a table using the `REVOKE DROP` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support revoking the **truncate** privilege to one or more users or roles
for a database or a table using the `REVOKE TRUNCATE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support revoking the **optimize** privilege to one or more users or roles
for a database or a table using the `REVOKE OPTIMIZE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Show
version: 1.0

[ClickHouse] SHALL support revoking the **show** privilege to one or more users or roles
for a database or a table using the `REVOKE SHOW` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support revoking the **kill query** privilege to one or more users or roles
for a database or a table using the `REVOKE KILL QUERY` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support revoking the **access management** privilege to one or more users or roles
for a database or a table using the `REVOKE ACCESS MANAGEMENT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.System
version: 1.0

[ClickHouse] SHALL support revoking the **system** privilege to one or more users or roles
for a database or a table using the `REVOKE SYSTEM` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support revoking the **introspection** privilege to one or more users or roles
for a database or a table using the `REVOKE INTROSPECTION` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support revoking the **sources** privilege to one or more users or roles
for a database or a table using the `REVOKE SOURCES` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support revoking the **dictGet** privilege to one or more users or roles
for a database or a table using the `REVOKE dictGet` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns
version: 1.0

[ClickHouse] SHALL support revoking the privilege **some_privilege** to one or more users or roles
for a database or a table using the `REVOKE some_privilege(column)` statement for one column.
Multiple columns will be supported with `REVOKE some_privilege(column1, column2...)` statement.
The privileges will be revoked for only the specified columns.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Multiple
version: 1.0

[ClickHouse] SHALL support revoking MULTIPLE **privileges** to one or more users or roles
for a database or a table using the `REVOKE privilege1, privilege2...` statement.
**privileges** refers to any set of Clickhouse defined privilege, whose hierarchy includes
SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,
SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.

##### RQ.SRS-006.RBAC.Revoke.Privilege.All
version: 1.0

[ClickHouse] SHALL support revoking **all** privileges to one or more users or roles
for a database or a table using the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statements.

##### RQ.SRS-006.RBAC.Revoke.Privilege.None
version: 1.0

[ClickHouse] SHALL support revoking **no** privileges to one or more users or roles
for a database or a table using the `REVOKE NONE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.On
version: 1.0

[ClickHouse] SHALL support the `ON` clause in the `REVOKE` privilege statement
which SHALL allow to specify one or more tables to which the privilege SHALL
be revoked using the following patterns

* `db.table` specific table in the specified database
* `db.*` any table in the specified database
* `*.*` any table in any database
* `table` specific table in the current database
* `*` any table in the current database

##### RQ.SRS-006.RBAC.Revoke.Privilege.From
version: 1.0

[ClickHouse] SHALL support the `FROM` clause in the `REVOKE` privilege statement
which SHALL allow to specify one or more users to which the privilege SHALL
be revoked using the following patterns

* `{user | CURRENT_USER} [,...]` some combination of users by name, which may include the current user
* `ALL` all users
* `ALL EXCEPT {user | CURRENT_USER} [,...]` the logical reverse of the first pattern

##### RQ.SRS-006.RBAC.Revoke.Privilege.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `REVOKE` statement that
revokes explicit privileges of a user or a role.

```sql
REVOKE [ON CLUSTER cluster_name] privilege
    [(column_name [,...])] [,...]
    ON {db.table|db.*|*.*|table|*}
    FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

##### RQ.SRS-006.RBAC.PartialRevoke.Syntax
version: 1.0

[ClickHouse] SHALL support partial revokes by using `partial_revokes` variable
that can be set or unset using the following syntax.

To disable partial revokes the `partial_revokes` variable SHALL be set to `0`

```sql
SET partial_revokes = 0
```

To enable partial revokes the `partial revokes` variable SHALL be set to `1`

```sql
SET partial_revokes = 1
```

##### RQ.SRS-006.RBAC.Grant.Role
version: 1.0

[ClickHouse] SHALL support granting one or more roles to
one or more users or roles using the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.CurrentUser
version: 1.0

[ClickHouse] SHALL support granting one or more roles to current user using
`TO CURRENT_USER` clause in the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.AdminOption
version: 1.0

[ClickHouse] SHALL support granting `admin option` privilege
to one or more users or roles using the `WITH ADMIN OPTION` clause
in the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which the user is to be granted one or more roles
using `ON CLUSTER` clause in the `GRANT` statement.

##### RQ.SRS-006.RBAC.Grant.Role.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `GRANT` role statement

``` sql
GRANT
    ON CLUSTER cluster_name
    role [, role ...]
    TO {user | role | CURRENT_USER} [,...]
    [WITH ADMIN OPTION]
```

##### RQ.SRS-006.RBAC.Revoke.Role
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
one or more users or roles using the `REVOKE` role statement.

##### RQ.SRS-006.RBAC.Revoke.Role.Keywords
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
special groupings of one or more users or roles with the `ALL`, `ALL EXCEPT`,
and `CURRENT_USER` keywords.

##### RQ.SRS-006.RBAC.Revoke.Role.Cluster
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
one or more users or roles from one or more clusters
using the `REVOKE ON CLUSTER` role statement.

##### RQ.SRS-006.RBAC.Revoke.AdminOption
version: 1.0

[ClickHouse] SHALL support revoking `admin option` privilege
in one or more users or roles using the `ADMIN OPTION FOR` clause
in the `REVOKE` role statement.

##### RQ.SRS-006.RBAC.Revoke.Role.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `REVOKE` role statement

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR]
    role [,...]
    FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

##### RQ.SRS-006.RBAC.Show.Grants
version: 1.0

[ClickHouse] SHALL support listing all the privileges granted to current user and role
using the `SHOW GRANTS` statement.

##### RQ.SRS-006.RBAC.Show.Grants.For
version: 1.0

[ClickHouse] SHALL support listing all the privileges granted to a user or a role
using the `FOR` clause in the `SHOW GRANTS` statement.

##### RQ.SRS-006.RBAC.Show.Grants.Syntax
version: 1.0

[Clickhouse] SHALL use the following syntax for the `SHOW GRANTS` statement

``` sql
SHOW GRANTS [FOR user_or_role]
```

##### RQ.SRS-006.RBAC.SettingsProfile.Create
version: 1.0

[ClickHouse] SHALL support creating settings profile using the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.IfNotExists
version: 1.0

[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE SETTINGS PROFILE` statement
to skip raising an exception if a settings profile with the same **name** already exists.
If `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if
a settings profile with the same **name** already exists.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Replace
version: 1.0

[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE SETTINGS PROFILE` statement
to replace existing settings profile if it already exists.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables
version: 1.0

[ClickHouse] SHALL support assigning values and constraints to one or more
variables in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value
version: 1.0

[ClickHouse] SHALL support assigning variable value in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support setting `MIN`, `MAX`, `READONLY`, and `WRITABLE`
constraints for the variables in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment
version: 1.0

[ClickHouse] SHALL support assigning settings profile to one or more users
or roles in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.None
version: 1.0

[ClickHouse] SHALL support assigning settings profile to no users or roles using
`TO NONE` clause in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.All
version: 1.0

[ClickHouse] SHALL support assigning settings profile to all current users and roles
using `TO ALL` clause in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.AllExcept
version: 1.0

[ClickHouse] SHALL support excluding assignment to one or more users or roles using
the `ALL EXCEPT` clause in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Inherit
version: 1.0

[ClickHouse] SHALL support inheriting profile settings from indicated profile using
the `INHERIT` clause in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying what cluster to create settings profile on
using `ON CLUSTER` clause in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `CREATE SETTINGS PROFILE` statement.

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name
    [ON CLUSTER cluster_name]
    [SET varname [= value] [MIN min] [MAX max] [READONLY|WRITABLE] | [INHERIT 'profile_name'] [,...]]
    [TO {user_or_role [,...] | NONE | ALL | ALL EXCEPT user_or_role [,...]}]
```

##### RQ.SRS-006.RBAC.SettingsProfile.Alter
version: 1.0

[ClickHouse] SHALL support altering settings profile using the `ALTER STETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.IfExists
version: 1.0

[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER SETTINGS PROFILE` statement
to not raise exception if a settings profile does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if a settings profile does not exist.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Rename
version: 1.0

[ClickHouse] SHALL support renaming settings profile using the `RANAME TO` clause
in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables
version: 1.0

[ClickHouse] SHALL support altering values and constraints of one or more
variables in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value
version: 1.0

[ClickHouse] SHALL support altering value of the variable in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support altering `MIN`, `MAX`, `READONLY`, and `WRITABLE`
constraints for the variables in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment
version: 1.0

[ClickHouse] SHALL support reassigning settings profile to one or more users
or roles using the `TO` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.None
version: 1.0

[ClickHouse] SHALL support reassigning settings profile to no users or roles using the
`TO NONE` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.All
version: 1.0

[ClickHouse] SHALL support reassigning settings profile to all current users and roles
using the `TO ALL` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.AllExcept
version: 1.0

[ClickHouse] SHALL support excluding assignment to one or more users or roles using
the `TO ALL EXCEPT` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Inherit
version: 1.0

[ClickHouse] SHALL support altering the settings profile by inheriting settings from
specified profile using `INHERIT` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.OnCluster
version: 1.0

[ClickHouse] SHALL support altering the settings profile on a specified cluster using
`ON CLUSTER` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `ALTER SETTINGS PROFILE` statement.

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name
    [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
    [TO {user_or_role [,...] | NONE | ALL | ALL EXCEPT user_or_role [,...]]}
```

##### RQ.SRS-006.RBAC.SettingsProfile.Drop
version: 1.0

[ClickHouse] SHALL support removing one or more settings profiles using the `DROP SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Drop.IfExists
version: 1.0

[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP SETTINGS PROFILE` statement
to skip raising an exception if the settings profile does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if a settings profile does not exist.

##### RQ.SRS-006.RBAC.SettingsProfile.Drop.OnCluster
version: 1.0

[ClickHouse] SHALL support dropping one or more settings profiles on specified cluster using
`ON CLUSTER` clause in the `DROP SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Drop.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `DROP SETTINGS PROFILE` statement

``` sql
DROP SETTINGS PROFILE [IF EXISTS] name [,name,...]
```

##### RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile
version: 1.0

[ClickHouse] SHALL support showing the `CREATE SETTINGS PROFILE` statement used to create the settings profile
using the `SHOW CREATE SETTINGS PROFILE` statement with the following syntax

``` sql
SHOW CREATE SETTINGS PROFILE name
```

##### RQ.SRS-006.RBAC.Quota.Create
version: 1.0

[ClickHouse] SHALL support creating quotas using the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.IfNotExists
version: 1.0

[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE QUOTA` statement
to skip raising an exception if a quota with the same **name** already exists.
If `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if
a quota with the same **name** already exists.

##### RQ.SRS-006.RBAC.Quota.Create.Replace
version: 1.0

[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE QUOTA` statement
to replace existing quota if it already exists.

##### RQ.SRS-006.RBAC.Quota.Create.Cluster
version: 1.0

[ClickHouse] SHALL support creating quotas on a specific cluster with the
`ON CLUSTER` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Interval
version: 1.0

[ClickHouse] SHALL support defining the quota interval that specifies
a period of time over for which the quota SHALL apply using the
`FOR INTERVAL` clause in the `CREATE QUOTA` statement.

This statement SHALL also support a number and a time period which will be one
of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:

`FOR INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some real number
to define the interval.


##### RQ.SRS-006.RBAC.Quota.Create.Interval.Randomized
version: 1.0

[ClickHouse] SHALL support defining the quota randomized interval that specifies
a period of time over for which the quota SHALL apply using the
`FOR RANDOMIZED INTERVAL` clause in the `CREATE QUOTA` statement.

This statement SHALL also support a number and a time period which will be one
of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:

`FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some
real number to define the interval.

##### RQ.SRS-006.RBAC.Quota.Create.Queries
version: 1.0

[ClickHouse] SHALL support limiting number of requests over a period of time
using the `QUERIES` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Errors
version: 1.0

[ClickHouse] SHALL support limiting number of queries that threw an exception
using the `ERRORS` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.ResultRows
version: 1.0

[ClickHouse] SHALL support limiting the total number of rows given as the result
using the `RESULT ROWS` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.ReadRows
version: 1.0

[ClickHouse] SHALL support limiting the total number of source rows read from tables
for running the query on all remote servers
using the `READ ROWS` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.ResultBytes
version: 1.0

[ClickHouse] SHALL support limiting the total number of bytes that can be returned as the result
using the `RESULT BYTES` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.ReadBytes
version: 1.0

[ClickHouse] SHALL support limiting the total number of source bytes read from tables
for running the query on all remote servers
using the `READ BYTES` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.ExecutionTime
version: 1.0

[ClickHouse] SHALL support limiting the maximum query execution time
using the `EXECUTION TIME` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.NoLimits
version: 1.0

[ClickHouse] SHALL support limiting the maximum query execution time
using the `NO LIMITS` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.TrackingOnly
version: 1.0

[ClickHouse] SHALL support limiting the maximum query execution time
using the `TRACKING ONLY` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.KeyedBy
version: 1.0

[ClickHouse] SHALL support to track quota for some key
following the `KEYED BY` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.KeyedByOptions
version: 1.0

[ClickHouse] SHALL support to track quota separately for some parameter
using the `KEYED BY 'parameter'` clause in the `CREATE QUOTA` statement.

'parameter' can be one of:
`{'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}`

##### RQ.SRS-006.RBAC.Quota.Create.Assignment
version: 1.0

[ClickHouse] SHALL support assigning quota to one or more users
or roles using the `TO` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Assignment.None
version: 1.0

[ClickHouse] SHALL support assigning quota to no users or roles using
`TO NONE` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Assignment.All
version: 1.0

[ClickHouse] SHALL support assigning quota to all current users and roles
using `TO ALL` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Assignment.Except
version: 1.0

[ClickHouse] SHALL support excluding assignment of quota to one or more users or roles using
the `EXCEPT` clause in the `CREATE QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Create.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `CREATE QUOTA` statement

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

##### RQ.SRS-006.RBAC.Quota.Alter
version: 1.0

[ClickHouse] SHALL support altering quotas using the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.IfExists
version: 1.0

[ClickHouse] SHALL support `IF EXISTS` clause in the `ALTER QUOTA` statement
to skip raising an exception if a quota does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be raised if
a quota does not exist.

##### RQ.SRS-006.RBAC.Quota.Alter.Rename
version: 1.0

[ClickHouse] SHALL support `RENAME TO` clause in the `ALTER QUOTA` statement
to rename the quota to the specified name.

##### RQ.SRS-006.RBAC.Quota.Alter.Cluster
version: 1.0

[ClickHouse] SHALL support altering quotas on a specific cluster with the
`ON CLUSTER` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Interval
version: 1.0

[ClickHouse] SHALL support redefining the quota interval that specifies
a period of time over for which the quota SHALL apply using the
`FOR INTERVAL` clause in the `ALTER QUOTA` statement.

This statement SHALL also support a number and a time period which will be one
of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:

`FOR INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some real number
to define the interval.

##### RQ.SRS-006.RBAC.Quota.Alter.Interval.Randomized
version: 1.0

[ClickHouse] SHALL support redefining the quota randomized interval that specifies
a period of time over for which the quota SHALL apply using the
`FOR RANDOMIZED INTERVAL` clause in the `ALTER QUOTA` statement.

This statement SHALL also support a number and a time period which will be one
of `{SECOND | MINUTE | HOUR | DAY | MONTH}`. Thus, the complete syntax SHALL be:

`FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}` where number is some
real number to define the interval.

##### RQ.SRS-006.RBAC.Quota.Alter.Queries
version: 1.0

[ClickHouse] SHALL support altering the limit of number of requests over a period of time
using the `QUERIES` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Errors
version: 1.0

[ClickHouse] SHALL support altering the limit of number of queries that threw an exception
using the `ERRORS` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.ResultRows
version: 1.0

[ClickHouse] SHALL support altering the limit of the total number of rows given as the result
using the `RESULT ROWS` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.ReadRows
version: 1.0

[ClickHouse] SHALL support altering the limit of the total number of source rows read from tables
for running the query on all remote servers
using the `READ ROWS` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.ALter.ResultBytes
version: 1.0

[ClickHouse] SHALL support altering the limit of the total number of bytes that can be returned as the result
using the `RESULT BYTES` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.ReadBytes
version: 1.0

[ClickHouse] SHALL support altering the limit of the total number of source bytes read from tables
for running the query on all remote servers
using the `READ BYTES` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.ExecutionTime
version: 1.0

[ClickHouse] SHALL support altering the limit of the maximum query execution time
using the `EXECUTION TIME` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.NoLimits
version: 1.0

[ClickHouse] SHALL support limiting the maximum query execution time
using the `NO LIMITS` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.TrackingOnly
version: 1.0

[ClickHouse] SHALL support limiting the maximum query execution time
using the `TRACKING ONLY` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.KeyedBy
version: 1.0

[ClickHouse] SHALL support altering quota to track quota separately for some key
following the `KEYED BY` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.KeyedByOptions
version: 1.0

[ClickHouse] SHALL support altering quota to track quota separately for some parameter
using the `KEYED BY 'parameter'` clause in the `ALTER QUOTA` statement.

'parameter' can be one of:
`{'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}`

##### RQ.SRS-006.RBAC.Quota.Alter.Assignment
version: 1.0

[ClickHouse] SHALL support reassigning quota to one or more users
or roles using the `TO` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Assignment.None
version: 1.0

[ClickHouse] SHALL support reassigning quota to no users or roles using
`TO NONE` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Assignment.All
version: 1.0

[ClickHouse] SHALL support reassigning quota to all current users and roles
using `TO ALL` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Assignment.Except
version: 1.0

[ClickHouse] SHALL support excluding assignment of quota to one or more users or roles using
the `EXCEPT` clause in the `ALTER QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Alter.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `ALTER QUOTA` statement

``` sql
ALTER QUOTA [IF EXIST] name
    {{{QUERIES | ERRORS | RESULT ROWS | READ ROWS | RESULT BYTES | READ BYTES | EXECUTION TIME} number} [, ...] FOR INTERVAL number time_unit} [, ...]
    [KEYED BY USERNAME | KEYED BY IP | NOT KEYED] [ALLOW CUSTOM KEY | DISALLOW CUSTOM KEY]
    [TO {user_or_role [,...] | NONE | ALL} [EXCEPT user_or_role [,...]]]
```

##### RQ.SRS-006.RBAC.Quota.Drop
version: 1.0

[ClickHouse] SHALL support removing one or more quotas using the `DROP QUOTA` statement.

##### RQ.SRS-006.RBAC.Quota.Drop.IfExists
version: 1.0

[ClickHouse] SHALL support using `IF EXISTS` clause in the `DROP QUOTA` statement
to skip raising an exception when the quota does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if the quota does not exist.

##### RQ.SRS-006.RBAC.Quota.Drop.Cluster
version: 1.0

[ClickHouse] SHALL support using `ON CLUSTER` clause in the `DROP QUOTA` statement
to indicate the cluster the quota to be dropped is located on.

##### RQ.SRS-006.RBAC.Quota.Drop.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `DROP QUOTA` statement

``` sql
DROP QUOTA [IF EXISTS] name [,name...]
```

##### RQ.SRS-006.RBAC.Quota.ShowQuotas
version: 1.0

[ClickHouse] SHALL support showing all of the current quotas
using the `SHOW QUOTAS` statement with the following syntax

##### RQ.SRS-006.RBAC.Quota.ShowQuotas.IntoOutfile
version: 1.0

[ClickHouse] SHALL support the `INTO OUTFILE` clause in the `SHOW QUOTAS` statement to define an outfile by some given string literal.

##### RQ.SRS-006.RBAC.Quota.ShowQuotas.Format
version: 1.0

[ClickHouse] SHALL support the `FORMAT` clause in the `SHOW QUOTAS` statement to define a format for the output quota list.

The types of valid formats are many, listed in output column:
https://clickhouse.tech/docs/en/interfaces/formats/

##### RQ.SRS-006.RBAC.Quota.ShowQuotas.Settings
version: 1.0

[ClickHouse] SHALL support the `SETTINGS` clause in the `SHOW QUOTAS` statement to define settings in the showing of all quotas.


##### RQ.SRS-006.RBAC.Quota.ShowQuotas.Syntax
version: 1.0

[ClickHouse] SHALL support using the `SHOW QUOTAS` statement
with the following syntax
``` sql
SHOW QUOTAS
```
##### RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Name
version: 1.0

[ClickHouse] SHALL support showing the `CREATE QUOTA` statement used to create the quota with some given name
using the `SHOW CREATE QUOTA` statement with the following syntax

``` sql
SHOW CREATE QUOTA name
```

##### RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Current
version: 1.0

[ClickHouse] SHALL support showing the `CREATE QUOTA` statement used to create the CURRENT quota
using the `SHOW CREATE QUOTA CURRENT` statement or the shorthand form
`SHOW CREATE QUOTA`

##### RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax when
using the `SHOW CREATE QUOTA` statement.

```sql
SHOW CREATE QUOTA [name | CURRENT]
```

##### RQ.SRS-006.RBAC.RowPolicy.Create
version: 1.0

[ClickHouse] SHALL support creating row policy using the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.IfNotExists
version: 1.0

[ClickHouse] SHALL support `IF NOT EXISTS` clause in the `CREATE ROW POLICY` statement
to skip raising an exception if a row policy with the same **name** already exists.
If the `IF NOT EXISTS` clause is not specified then an exception SHALL be raised if
a row policy with the same **name** already exists.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Replace
version: 1.0

[ClickHouse] SHALL support `OR REPLACE` clause in the `CREATE ROW POLICY` statement
to replace existing row policy if it already exists.

##### RQ.SRS-006.RBAC.RowPolicy.Create.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which to create the role policy
using the `ON CLUSTER` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.On
version: 1.0

[ClickHouse] SHALL support specifying table on which to create the role policy
using the `ON` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Access
version: 1.0

[ClickHouse] SHALL support allowing or restricting access to rows using the
`AS` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Access.Permissive
version: 1.0

[ClickHouse] SHALL support allowing access to rows using the
`AS PERMISSIVE` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Access.Restrictive
version: 1.0

[ClickHouse] SHALL support restricting access to rows using the
`AS RESTRICTIVE` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.ForSelect
version: 1.0

[ClickHouse] SHALL support specifying which rows are affected
using the `FOR SELECT` clause in the `CREATE ROW POLICY` statement.
REQUIRES CONFIRMATION

##### RQ.SRS-006.RBAC.RowPolicy.Create.Condition
version: 1.0

[ClickHouse] SHALL support specifying a condition that
that can be any SQL expression which returns a boolean using the `USING`
clause in the `CREATE ROW POLOCY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Assignment
version: 1.0

[ClickHouse] SHALL support assigning row policy to one or more users
or roles using the `TO` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.None
version: 1.0

[ClickHouse] SHALL support assigning row policy to no users or roles using
the `TO NONE` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.All
version: 1.0

[ClickHouse] SHALL support assigning row policy to all current users and roles
using `TO ALL` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.AllExcept
version: 1.0

[ClickHouse] SHALL support excluding assignment of row policy to one or more users or roles using
the `ALL EXCEPT` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `CRETE ROW POLICY` statement

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

##### RQ.SRS-006.RBAC.RowPolicy.Alter
version: 1.0

[ClickHouse] SHALL support altering row policy using the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.IfExists
version: 1.0

[ClickHouse] SHALL support the `IF EXISTS` clause in the `ALTER ROW POLICY` statement
to skip raising an exception if a row policy does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be raised if
a row policy does not exist.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.ForSelect
version: 1.0

[ClickHouse] SHALL support modifying rows on which to apply the row policy
using the `FOR SELECT` clause in the `ALTER ROW POLICY` statement.
REQUIRES FUNCTION CONFIRMATION.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which to alter the row policy
using the `ON CLUSTER` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.On
version: 1.0

[ClickHouse] SHALL support specifying table on which to alter the row policy
using the `ON` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Rename
version: 1.0

[ClickHouse] SHALL support renaming the row policy using the `RENAME` clause
in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Access
version: 1.0

[ClickHouse] SHALL support altering access to rows using the
`AS` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive
version: 1.0

[ClickHouse] SHALL support permitting access to rows using the
`AS PERMISSIVE` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive
version: 1.0

[ClickHouse] SHALL support restricting access to rows using the
`AS RESTRICTIVE` clause in the `CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Condition
version: 1.0

[ClickHouse] SHALL support re-specifying the row policy condition
using the `USING` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.None
version: 1.0

[ClickHouse] SHALL support removing the row policy condition
using the `USING NONE` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment
version: 1.0

[ClickHouse] SHALL support reassigning row policy to one or more users
or roles using the `TO` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.None
version: 1.0

[ClickHouse] SHALL support reassigning row policy to no users or roles using
the `TO NONE` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.All
version: 1.0

[ClickHouse] SHALL support reassigning row policy to all current users and roles
using the `TO ALL` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.AllExcept
version: 1.0

[ClickHouse] SHALL support excluding assignment of row policy to one or more users or roles using
the `ALL EXCEPT` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `ALTER ROW POLICY` statement

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

##### RQ.SRS-006.RBAC.RowPolicy.Drop
version: 1.0

[ClickHouse] SHALL support removing one or more row policies using the `DROP ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Drop.IfExists
version: 1.0

[ClickHouse] SHALL support using the `IF EXISTS` clause in the `DROP ROW POLICY` statement
to skip raising an exception when the row policy does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be
raised if the row policy does not exist.

##### RQ.SRS-006.RBAC.RowPolicy.Drop.On
version: 1.0

[ClickHouse] SHALL support removing row policy from one or more specified tables
using the `ON` clause in the `DROP ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Drop.OnCluster
version: 1.0

[ClickHouse] SHALL support removing row policy from specified cluster
using the `ON CLUSTER` clause in the `DROP ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Drop.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `DROP ROW POLICY` statement.

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

##### RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy
version: 1.0

[ClickHouse] SHALL support showing the `CREATE ROW POLICY` statement used to create the row policy
using the `SHOW CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.On
version: 1.0

[ClickHouse] SHALL support showing statement used to create row policy on specific table
using the `ON` in the `SHOW CREATE ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `SHOW CREATE ROW POLICY`.

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

##### RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies
version: 1.0

[ClickHouse] SHALL support showing row policies using the `SHOW ROW POLICIES` statement.

##### RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.On
version: 1.0

[ClickHouse] SHALL support showing row policies on a specific table
using the `ON` clause in the `SHOW ROW POLICIES` statement.

##### RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `SHOW ROW POLICIES`.

```sql
SHOW [ROW] POLICIES [ON [database.]table]
```

#### Table Privileges

##### RQ.SRS-006.RBAC.Table.PublicTables
version: 1.0

[ClickHouse] SHALL support that a user without any privileges will be able to access the following tables

* system.one
* system.numbers
* system.contributors
* system.functions

##### RQ.SRS-006.RBAC.Table.ShowTables
version: 1.0

[ClickHouse] SHALL add a table to the list of tables accessible through `SHOW TABLES` by a user if and only if
that user has any privilege on that table, either directly or through a role.

##### Distributed Tables

###### RQ.SRS-006.RBAC.Table.DistributedTable.Create
version: 1.0

[ClickHouse] SHALL successfully `CREATE` a distributed table if and only if
the user has **create table** privilege on the table and **remote** privilege on *.*.

###### RQ.SRS-006.RBAC.Table.DistributedTable.Select
version: 1.0

[ClickHouse] SHALL successfully `SELECT` from a distributed table if and only if
the user has **select** privilege on the table and on the remote table specified in the `CREATE` query of the distributed table.

Does not require **select** privilege for the remote table if the remote table does not exist on the same server as the user.

###### RQ.SRS-006.RBAC.Table.DistributedTable.Insert
version: 1.0

[ClickHouse] SHALL successfully `INSERT` into a distributed table if and only if
the user has **insert** privilege on the table and on the remote table specified in the `CREATE` query of the distributed table.

Does not require **insert** privilege for the remote table if the remote table does not exist on the same server as the user,
insert executes into the remote table on a different server.

###### RQ.SRS-006.RBAC.Table.DistributedTable.SpecialTables
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table that uses one of the special tables if and only if
the user has the necessary privileges to interact with that special table, either granted directly or through a role.
Special tables include:
* materialized view
* distributed table
* source table of a materialized view

###### RQ.SRS-006.RBAC.Table.DistributedTable.LocalUser
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table from
a user present locally, but not remotely.

###### RQ.SRS-006.RBAC.Table.DistributedTable.SameUserDifferentNodesDifferentPrivileges
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table by a user that exists on multiple nodes
if and only if the user has the required privileges on the node the query is being executed from.

#### Views

##### View

###### RQ.SRS-006.RBAC.View
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select** and **drop**
privileges for a view for users or roles.

###### RQ.SRS-006.RBAC.View.Create
version: 1.0

[ClickHouse] SHALL only successfully execute a `CREATE VIEW` command if and only if
the user has **create view** privilege either explicitly or through roles.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE VIEW view AS SELECT * FROM source_table
CREATE VIEW view AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE VIEW view AS SELECT * FROM table0 JOIN table1 USING column
CREATE VIEW view AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE VIEW view AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE VIEW view0 AS SELECT column FROM view1 UNION ALL SELECT column FROM view2
```

###### RQ.SRS-006.RBAC.View.Select
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from a view if and only if
the user has **select** privilege for that view either explicitly or through a role.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE VIEW view AS SELECT * FROM source_table
CREATE VIEW view AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE VIEW view AS SELECT * FROM table0 JOIN table1 USING column
CREATE VIEW view AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE VIEW view AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE VIEW view0 AS SELECT column FROM view1 UNION ALL SELECT column FROM view2

SELECT * FROM view
```

###### RQ.SRS-006.RBAC.View.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

##### Materialized View

###### RQ.SRS-006.RBAC.MaterializedView
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select**, **alter** and **drop**
privileges for a materialized view for users or roles.

###### RQ.SRS-006.RBAC.MaterializedView.Create
version: 1.0

[ClickHouse] SHALL only successfully execute a `CREATE MATERIALIZED VIEW` command if and only if
the user has **create view** privilege either explicitly or through roles.

If `POPULATE` is specified, the user must have `INSERT` privilege on the view,
either explicitly or through roles.
For example,
```sql
CREATE MATERIALIZED VIEW view ENGINE = Memory POPULATE AS SELECT * FROM source_table
```

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM source_table
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 JOIN table1 USING column
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE MATERIALIZED VIEW view0 ENGINE = Memory AS SELECT column FROM view1 UNION ALL SELECT column FROM view2
```

If the materialized view has a target table explicitly declared in the `TO` clause, the user must have
**insert** and **select** privilege on the target table.
For example,
```sql
CREATE MATERIALIZED VIEW view TO target_table AS SELECT * FROM source_table
```

###### RQ.SRS-006.RBAC.MaterializedView.Select
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from a materialized view if and only if
the user has **select** privilege for that view either explicitly or through a role.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM source_table
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 JOIN table1 USING column
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE MATERIALIZED VIEW view ENGINE = Memory AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE MATERIALIZED VIEW view0 ENGINE = Memory AS SELECT column FROM view1 UNION ALL SELECT column FROM view2

SELECT * FROM view
```

###### RQ.SRS-006.RBAC.MaterializedView.Select.TargetTable
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from the target table, implicit or explicit, of a materialized view if and only if
the user has `SELECT` privilege for the table, either explicitly or through a role.

###### RQ.SRS-006.RBAC.MaterializedView.Select.SourceTable
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from the source table of a materialized view if and only if
the user has `SELECT` privilege for the table, either explicitly or through a role.

###### RQ.SRS-006.RBAC.MaterializedView.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

###### RQ.SRS-006.RBAC.MaterializedView.ModifyQuery
version: 1.0

[ClickHouse] SHALL only successfully execute a `MODIFY QUERY` command if and only if
the user has **modify query** privilege on that view either explicitly or through a role.

If the new query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
ALTER TABLE view MODIFY QUERY SELECT * FROM source_table
```

###### RQ.SRS-006.RBAC.MaterializedView.Insert
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a materialized view if and only if
the user has `INSERT` privilege on the view, either explicitly or through a role.

###### RQ.SRS-006.RBAC.MaterializedView.Insert.SourceTable
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a source table of a materialized view if and only if
the user has `INSERT` privilege on the source table, either explicitly or through a role.

###### RQ.SRS-006.RBAC.MaterializedView.Insert.TargetTable
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a target table of a materialized view if and only if
the user has `INSERT` privelege on the target table, either explicitly or through a role.

##### Live View

###### RQ.SRS-006.RBAC.LiveView
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select**, **alter** and **drop**
privileges for a live view for users or roles.

###### RQ.SRS-006.RBAC.LiveView.Create
version: 1.0

[ClickHouse] SHALL only successfully execute a `CREATE LIVE VIEW` command if and only if
the user has **create view** privilege either explicitly or through roles.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE LIVE VIEW view AS SELECT * FROM source_table
CREATE LIVE VIEW view AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE LIVE VIEW view AS SELECT * FROM table0 JOIN table1 USING column
CREATE LIVE VIEW view AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE LIVE VIEW view AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE LIVE VIEW view0 AS SELECT column FROM view1 UNION ALL SELECT column FROM view2
```

###### RQ.SRS-006.RBAC.LiveView.Select
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from a live view if and only if
the user has **select** privilege for that view either explicitly or through a role.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
CREATE LIVE VIEW view AS SELECT * FROM source_table
CREATE LIVE VIEW view AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE LIVE VIEW view AS SELECT * FROM table0 JOIN table1 USING column
CREATE LIVE VIEW view AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE LIVE VIEW view AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE LIVE VIEW view0 AS SELECT column FROM view1 UNION ALL SELECT column FROM view2

SELECT * FROM view
```

###### RQ.SRS-006.RBAC.LiveView.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

###### RQ.SRS-006.RBAC.LiveView.Refresh
version: 1.0

[ClickHouse] SHALL only successfully execute an `ALTER LIVE VIEW REFRESH` command if and only if
the user has **refresh** privilege on that view either explicitly or through a role.

#### Privileges

##### RQ.SRS-006.RBAC.Privileges.Usage
version: 1.0

[ClickHouse] SHALL support granting or revoking **usage** privilege
for a database or a specific table to one or more **users** or **roles**.

##### Select

###### RQ.SRS-006.RBAC.Privileges.Select
version: 1.0

[ClickHouse] SHALL support controlling access to the **select** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `SELECT INTO` statements SHALL not to be executed, unless the user
has the **select** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.Select.Grant
version: 1.0

[ClickHouse] SHALL support granting **select** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.Select.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **select** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.Select.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **select** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `SELECT INTO` statements SHALL not to be executed, unless the user
has the **select** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.Select.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **select** privilege
on a specified cluster to one or more **users** or **roles**.
Any `SELECT INTO` statements SHALL succeed only on nodes where
the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.Select.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **select** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Insert

###### RQ.SRS-006.RBAC.Privileges.Insert
version: 1.0

[ClickHouse] SHALL support controlling access to the **insert** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `INSERT INTO` statements SHALL not to be executed, unless the user
has the **insert** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.Insert.Grant
version: 1.0

[ClickHouse] SHALL support granting **insert** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.Insert.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **insert** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.Insert.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **insert** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `INSERT INTO` statements SHALL not to be executed, unless the user
has the **insert** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.Insert.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **insert** privilege
on a specified cluster to one or more **users** or **roles**.
Any `INSERT INTO` statements SHALL succeed only on nodes where
the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.Insert.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **insert** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### AlterColumn

###### RQ.SRS-006.RBAC.Privileges.AlterColumn
version: 1.0

[ClickHouse] SHALL support controlling access to the **AlterColumn** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN` statements SHALL
return an error, unless the user has the **alter column** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.AlterColumn.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter column** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterColumn.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter column** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.AlterColumn.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter column** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN` statements SHALL return an error,
 unless the user has the **alter column** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.AlterColumn.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter column** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.AlterColumn.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter column** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### AlterIndex

###### RQ.SRS-006.RBAC.Privileges.AlterIndex
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter index** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ORDER BY | ADD|DROP|MATERIALIZE|CLEAR INDEX` statements SHALL
return an error, unless the user has the **alter index** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.AlterIndex.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter index** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterIndex.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter index** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.AlterIndex.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter index** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ORDER BY | ADD|DROP|MATERIALIZE|CLEAR INDEX`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.AlterIndex.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter index** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### AlterConstraint

###### RQ.SRS-006.RBAC.Privileges.AlterConstraint
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP CONSTRAINT` statements SHALL
return an error, unless the user has the **alter constraint** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter constraint** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP CONSTRAINT`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.AlterConstraint.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter constraint** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### AlterTTL

###### RQ.SRS-006.RBAC.Privileges.AlterTTL
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ALTER TTL | ALTER MATERIALIZE TTL` statements SHALL
return an error, unless the user has the **alter ttl** or **alter materialize ttl** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

###### RQ.SRS-006.RBAC.Privileges.AlterTTL.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterTTL.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.AlterTTL.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter ttl** or **alter materialize ttl** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ALTER TTL | ALTER MATERIALIZE TTL`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.AlterTTL.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter ttl** or **alter materialize ttl** privilege
on tables created using the following engines

* MergeTree

##### AlterSettings

###### RQ.SRS-006.RBAC.Privileges.AlterSettings
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... MODIFY SETTING setting` statements SHALL
return an error, unless the user has the **alter settings** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user. The **alter settings** privilege allows
modifying table engine settings. It doesn’t affect settings or server configuration parameters.

###### RQ.SRS-006.RBAC.Privileges.AlterSettings.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterSettings.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**

###### RQ.SRS-006.RBAC.Privileges.AlterSettings.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter settings** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... MODIFY SETTING setting`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

###### RQ.SRS-006.RBAC.Privileges.AlterSettings.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter settings** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Alter Update

###### RQ.SRS-006.RBAC.Privileges.AlterUpdate
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER UPDATE` statement if and only if the user has **alter update** privilege for that column,
either directly or through a role.

###### RQ.SRS-006.RBAC.Privileges.AlterUpdate.Access
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter update** privilege on a column level
to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterUpdate.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter update** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Alter Delete

###### RQ.SRS-006.RBAC.Privileges.AlterDelete
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER DELETE` statement if and only if the user has **alter delete** privilege for that table,
either directly or through a role.

###### RQ.SRS-006.RBAC.Privileges.AlterDelete.Access
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter delete** privilege to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterDelete.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter delete** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Alter Freeze Partition

###### RQ.SRS-006.RBAC.Privileges.AlterFreeze
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER FREEZE` statement if and only if the user has **alter freeze** privilege for that table,
either directly or through a role.

###### RQ.SRS-006.RBAC.Privileges.AlterFreeze.Access
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter freeze** privilege to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterFreeze.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter freeze** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Alter Fetch Partition

###### RQ.SRS-006.RBAC.Privileges.AlterFetch
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER FETCH` statement if and only if the user has **alter fetch** privilege for that table,
either directly or through a role.

###### RQ.SRS-006.RBAC.Privileges.AlterFetch.Access
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter fetch** privilege to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterFetch.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter fetch** privilege
on tables created using the following engines

* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Alter Move Partition

###### RQ.SRS-006.RBAC.Privileges.AlterMove
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER MOVE` statement if and only if the user has **alter move**, **select**, and **alter delete** privilege on the source table
and **insert** privilege on the target table, either directly or through a role.
For example,
```sql
ALTER TABLE source_table MOVE PARTITION 1 TO target_table
```

###### RQ.SRS-006.RBAC.Privileges.AlterMove.Access
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter move** privilege to one or more **users** or **roles**.

###### RQ.SRS-006.RBAC.Privileges.AlterMove.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter move** privilege
on tables created using the following engines

* MergeTree
* ReplacingMergeTree
* SummingMergeTree
* AggregatingMergeTree
* CollapsingMergeTree
* VersionedCollapsingMergeTree
* GraphiteMergeTree
* ReplicatedMergeTree
* ReplicatedSummingMergeTree
* ReplicatedReplacingMergeTree
* ReplicatedAggregatingMergeTree
* ReplicatedCollapsingMergeTree
* ReplicatedVersionedCollapsingMergeTree
* ReplicatedGraphiteMergeTree

##### Grant Option

###### RQ.SRS-006.RBAC.Privileges.GrantOption
version: 1.0

[ClickHouse] SHALL successfully execute `GRANT` or `REVOKE` privilege statements by a user if and only if
the user has that privilege with `GRANT OPTION`, either directly or through a role.

`GRANT OPTION` is supported by the following privileges

* `ALTER MOVE PARTITION`
* `ALTER FETCH PARTITION`
* `ALTER FREEZE PARTITION`
* `ALTER DELETE`
* `ALTER UPDATE`
* `ALTER SETTINGS`
* `ALTER TTL`
* `ALTER CONSTRAINT`
* `ALTER COLUMN`
* `ALTER INDEX`
* `INSERT`
* `SELECT`

##### RQ.SRS-006.RBAC.Privileges.Delete
version: 1.0

[ClickHouse] SHALL support granting or revoking **delete** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.Alter
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.Create
version: 1.0

[ClickHouse] SHALL support granting or revoking **create** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.Drop
version: 1.0

[ClickHouse] SHALL support granting or revoking **drop** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.All
version: 1.0

[ClickHouse] SHALL include in the **all** privilege the same rights
as provided by **usage**, **select**, **select columns**,
**insert**, **delete**, **alter**, **create**, and **drop** privileges.

##### RQ.SRS-006.RBAC.Privileges.All.GrantRevoke
version: 1.0

[ClickHouse] SHALL support granting or revoking **all** privileges
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AdminOption
version: 1.0

[ClickHouse] SHALL support granting or revoking **admin option** privilege
to one or more **users** or **roles**.

#### Required Privileges

##### RQ.SRS-006.RBAC.RequiredPrivileges.Create
version: 1.0

[ClickHouse] SHALL not allow any `CREATE` statements
to be executed unless the user has the **create** privilege for the destination database
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Alter
version: 1.0

[ClickHouse] SHALL not allow any `ALTER` statements
to be executed unless the user has the **alter** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Drop
version: 1.0

[ClickHouse] SHALL not allow any `DROP` statements
to be executed unless the user has the **drop** privilege for the destination database
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Drop.Table
version: 1.0

[ClickHouse] SHALL not allow any `DROP TABLE` statements
to be executed unless the user has the **drop** privilege for the destination database or the table
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.GrantRevoke
version: 1.0

[ClickHouse] SHALL not allow any `GRANT` or `REVOKE` statements
to be executed unless the user has the **grant option** privilege
for the privilege of the destination table
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Use
version: 1.0

[ClickHouse] SHALL not allow the `USE` statement to be executed
unless the user has at least one of the privileges for the database
or the table inside that database
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Admin
version: 1.0

[ClickHouse] SHALL not allow any of the following statements

* `SYSTEM`
* `SHOW`
* `ATTACH`
* `CHECK TABLE`
* `DESCRIBE TABLE`
* `DETACH`
* `EXISTS`
* `KILL QUERY`
* `KILL MUTATION`
* `OPTIMIZE`
* `RENAME`
* `TRUNCATE`

to be executed unless the user has the **admin option** privilege
through one of the roles with **admin option** privilege assigned to the user.

## References

* **ClickHouse:** https://clickhouse.tech
* **GitHub repository:** https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/rbac/requirements/requirements.md
* **Revision history:** https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/rbac/requirements/requirements.md
* **Git:** https://git-scm.com/
* **MySQL:** https://dev.mysql.com/doc/refman/8.0/en/account-management-statements.html
* **PostgreSQL:** https://www.postgresql.org/docs/12/user-manag.html

[ClickHouse]: https://clickhouse.tech
[GitHub repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/rbac/requirements/requirements.md
[Revision history]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/rbac/requirements/requirements.md
[Git]: https://git-scm.com/
[MySQL]: https://dev.mysql.com/doc/refman/8.0/en/account-management-statements.html
[PostgreSQL]: https://www.postgresql.org/docs/12/user-manag.html
