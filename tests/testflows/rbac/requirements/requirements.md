# SRS-006 ClickHouse Role Based Access Control<br>Software Requirements Specification

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
    * 5.1.5 [Privileges](#privileges)
      * 5.1.5.1 [RQ.SRS-006.RBAC.Privileges.Usage](#rqsrs-006rbacprivilegesusage)
      * 5.1.5.2 [RQ.SRS-006.RBAC.Privileges.Select](#rqsrs-006rbacprivilegesselect)
      * 5.1.5.3 [RQ.SRS-006.RBAC.Privileges.SelectColumns](#rqsrs-006rbacprivilegesselectcolumns)
      * 5.1.5.4 [RQ.SRS-006.RBAC.Privileges.Insert](#rqsrs-006rbacprivilegesinsert)
      * 5.1.5.5 [RQ.SRS-006.RBAC.Privileges.Delete](#rqsrs-006rbacprivilegesdelete)
      * 5.1.5.6 [RQ.SRS-006.RBAC.Privileges.Alter](#rqsrs-006rbacprivilegesalter)
      * 5.1.5.7 [RQ.SRS-006.RBAC.Privileges.Create](#rqsrs-006rbacprivilegescreate)
      * 5.1.5.8 [RQ.SRS-006.RBAC.Privileges.Drop](#rqsrs-006rbacprivilegesdrop)
      * 5.1.5.9 [RQ.SRS-006.RBAC.Privileges.All](#rqsrs-006rbacprivilegesall)
      * 5.1.5.10 [RQ.SRS-006.RBAC.Privileges.All.GrantRevoke](#rqsrs-006rbacprivilegesallgrantrevoke)
      * 5.1.5.11 [RQ.SRS-006.RBAC.Privileges.GrantOption](#rqsrs-006rbacprivilegesgrantoption)
      * 5.1.5.12 [RQ.SRS-006.RBAC.Privileges.AdminOption](#rqsrs-006rbacprivilegesadminoption)
    * 5.1.6 [Required Privileges](#required-privileges)
      * 5.1.6.1 [RQ.SRS-006.RBAC.RequiredPrivileges.Insert](#rqsrs-006rbacrequiredprivilegesinsert)
      * 5.1.6.2 [RQ.SRS-006.RBAC.RequiredPrivileges.Select](#rqsrs-006rbacrequiredprivilegesselect)
      * 5.1.6.3 [RQ.SRS-006.RBAC.RequiredPrivileges.Create](#rqsrs-006rbacrequiredprivilegescreate)
      * 5.1.6.4 [RQ.SRS-006.RBAC.RequiredPrivileges.Alter](#rqsrs-006rbacrequiredprivilegesalter)
      * 5.1.6.5 [RQ.SRS-006.RBAC.RequiredPrivileges.Drop](#rqsrs-006rbacrequiredprivilegesdrop)
      * 5.1.6.6 [RQ.SRS-006.RBAC.RequiredPrivileges.Drop.Table](#rqsrs-006rbacrequiredprivilegesdroptable)
      * 5.1.6.7 [RQ.SRS-006.RBAC.RequiredPrivileges.GrantRevoke](#rqsrs-006rbacrequiredprivilegesgrantrevoke)
      * 5.1.6.8 [RQ.SRS-006.RBAC.RequiredPrivileges.Use](#rqsrs-006rbacrequiredprivilegesuse)
      * 5.1.6.9 [RQ.SRS-006.RBAC.RequiredPrivileges.Admin](#rqsrs-006rbacrequiredprivilegesadmin)
    * 5.1.7 [Partial Revokes](#partial-revokes)
      * 5.1.7.1 [RQ.SRS-006.RBAC.PartialRevokes](#rqsrs-006rbacpartialrevokes)
    * 5.1.8 [Settings Profile](#settings-profile)
      * 5.1.8.1 [RQ.SRS-006.RBAC.SettingsProfile](#rqsrs-006rbacsettingsprofile)
      * 5.1.8.2 [RQ.SRS-006.RBAC.SettingsProfile.Constraints](#rqsrs-006rbacsettingsprofileconstraints)
      * 5.1.8.3 [RQ.SRS-006.RBAC.SettingsProfile.ShowCreate](#rqsrs-006rbacsettingsprofileshowcreate)
    * 5.1.9 [Quotas](#quotas)
      * 5.1.9.1 [RQ.SRS-006.RBAC.Quotas](#rqsrs-006rbacquotas)
      * 5.1.9.2 [RQ.SRS-006.RBAC.Quotas.Keyed](#rqsrs-006rbacquotaskeyed)
      * 5.1.9.3 [RQ.SRS-006.RBAC.Quotas.Queries](#rqsrs-006rbacquotasqueries)
      * 5.1.9.4 [RQ.SRS-006.RBAC.Quotas.Errors](#rqsrs-006rbacquotaserrors)
      * 5.1.9.5 [RQ.SRS-006.RBAC.Quotas.ResultRows](#rqsrs-006rbacquotasresultrows)
      * 5.1.9.6 [RQ.SRS-006.RBAC.Quotas.ReadRows](#rqsrs-006rbacquotasreadrows)
      * 5.1.9.7 [RQ.SRS-006.RBAC.Quotas.ResultBytes](#rqsrs-006rbacquotasresultbytes)
      * 5.1.9.8 [RQ.SRS-006.RBAC.Quotas.ReadBytes](#rqsrs-006rbacquotasreadbytes)
      * 5.1.9.9 [RQ.SRS-006.RBAC.Quotas.ExecutionTime](#rqsrs-006rbacquotasexecutiontime)
      * 5.1.9.10 [RQ.SRS-006.RBAC.Quotas.ShowCreate](#rqsrs-006rbacquotasshowcreate)
    * 5.1.10 [Row Policy](#row-policy)
      * 5.1.10.1 [RQ.SRS-006.RBAC.RowPolicy](#rqsrs-006rbacrowpolicy)
      * 5.1.10.2 [RQ.SRS-006.RBAC.RowPolicy.Condition](#rqsrs-006rbacrowpolicycondition)
      * 5.1.10.3 [RQ.SRS-006.RBAC.RowPolicy.ShowCreate](#rqsrs-006rbacrowpolicyshowcreate)
  * 5.2 [Specific](#specific)
      * 5.2.10.1 [RQ.SRS-006.RBAC.User.Use.DefaultRole](#rqsrs-006rbacuserusedefaultrole)
      * 5.2.10.2 [RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole](#rqsrs-006rbacuseruseallroleswhennodefaultrole)
      * 5.2.10.3 [RQ.SRS-006.RBAC.User.Create](#rqsrs-006rbacusercreate)
      * 5.2.10.4 [RQ.SRS-006.RBAC.User.Create.IfNotExists](#rqsrs-006rbacusercreateifnotexists)
      * 5.2.10.5 [RQ.SRS-006.RBAC.User.Create.Replace](#rqsrs-006rbacusercreatereplace)
      * 5.2.10.6 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword](#rqsrs-006rbacusercreatepasswordnopassword)
      * 5.2.10.7 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword.Login](#rqsrs-006rbacusercreatepasswordnopasswordlogin)
      * 5.2.10.8 [RQ.SRS-006.RBAC.User.Create.Password.PlainText](#rqsrs-006rbacusercreatepasswordplaintext)
      * 5.2.10.9 [RQ.SRS-006.RBAC.User.Create.Password.PlainText.Login](#rqsrs-006rbacusercreatepasswordplaintextlogin)
      * 5.2.10.10 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password](#rqsrs-006rbacusercreatepasswordsha256password)
      * 5.2.10.11 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password.Login](#rqsrs-006rbacusercreatepasswordsha256passwordlogin)
      * 5.2.10.12 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash](#rqsrs-006rbacusercreatepasswordsha256hash)
      * 5.2.10.13 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash.Login](#rqsrs-006rbacusercreatepasswordsha256hashlogin)
      * 5.2.10.14 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password](#rqsrs-006rbacusercreatepassworddoublesha1password)
      * 5.2.10.15 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password.Login](#rqsrs-006rbacusercreatepassworddoublesha1passwordlogin)
      * 5.2.10.16 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash](#rqsrs-006rbacusercreatepassworddoublesha1hash)
      * 5.2.10.17 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash.Login](#rqsrs-006rbacusercreatepassworddoublesha1hashlogin)
      * 5.2.10.18 [RQ.SRS-006.RBAC.User.Create.Host.Name](#rqsrs-006rbacusercreatehostname)
      * 5.2.10.19 [RQ.SRS-006.RBAC.User.Create.Host.Regexp](#rqsrs-006rbacusercreatehostregexp)
      * 5.2.10.20 [RQ.SRS-006.RBAC.User.Create.Host.IP](#rqsrs-006rbacusercreatehostip)
      * 5.2.10.21 [RQ.SRS-006.RBAC.User.Create.Host.Any](#rqsrs-006rbacusercreatehostany)
      * 5.2.10.22 [RQ.SRS-006.RBAC.User.Create.Host.None](#rqsrs-006rbacusercreatehostnone)
      * 5.2.10.23 [RQ.SRS-006.RBAC.User.Create.Host.Local](#rqsrs-006rbacusercreatehostlocal)
      * 5.2.10.24 [RQ.SRS-006.RBAC.User.Create.Host.Like](#rqsrs-006rbacusercreatehostlike)
      * 5.2.10.25 [RQ.SRS-006.RBAC.User.Create.Host.Default](#rqsrs-006rbacusercreatehostdefault)
      * 5.2.10.26 [RQ.SRS-006.RBAC.User.Create.DefaultRole](#rqsrs-006rbacusercreatedefaultrole)
      * 5.2.10.27 [RQ.SRS-006.RBAC.User.Create.DefaultRole.None](#rqsrs-006rbacusercreatedefaultrolenone)
      * 5.2.10.28 [RQ.SRS-006.RBAC.User.Create.DefaultRole.All](#rqsrs-006rbacusercreatedefaultroleall)
      * 5.2.10.29 [RQ.SRS-006.RBAC.User.Create.Settings](#rqsrs-006rbacusercreatesettings)
      * 5.2.10.30 [RQ.SRS-006.RBAC.User.Create.OnCluster](#rqsrs-006rbacusercreateoncluster)
      * 5.2.10.31 [RQ.SRS-006.RBAC.User.Create.Syntax](#rqsrs-006rbacusercreatesyntax)
      * 5.2.10.32 [RQ.SRS-006.RBAC.User.Alter](#rqsrs-006rbacuseralter)
      * 5.2.10.33 [RQ.SRS-006.RBAC.User.Alter.OrderOfEvaluation](#rqsrs-006rbacuseralterorderofevaluation)
      * 5.2.10.34 [RQ.SRS-006.RBAC.User.Alter.IfExists](#rqsrs-006rbacuseralterifexists)
      * 5.2.10.35 [RQ.SRS-006.RBAC.User.Alter.Cluster](#rqsrs-006rbacuseraltercluster)
      * 5.2.10.36 [RQ.SRS-006.RBAC.User.Alter.Rename](#rqsrs-006rbacuseralterrename)
      * 5.2.10.37 [RQ.SRS-006.RBAC.User.Alter.Password.PlainText](#rqsrs-006rbacuseralterpasswordplaintext)
      * 5.2.10.38 [RQ.SRS-006.RBAC.User.Alter.Password.Sha256Password](#rqsrs-006rbacuseralterpasswordsha256password)
      * 5.2.10.39 [RQ.SRS-006.RBAC.User.Alter.Password.DoubleSha1Password](#rqsrs-006rbacuseralterpassworddoublesha1password)
      * 5.2.10.40 [RQ.SRS-006.RBAC.User.Alter.Host.AddDrop](#rqsrs-006rbacuseralterhostadddrop)
      * 5.2.10.41 [RQ.SRS-006.RBAC.User.Alter.Host.Local](#rqsrs-006rbacuseralterhostlocal)
      * 5.2.10.42 [RQ.SRS-006.RBAC.User.Alter.Host.Name](#rqsrs-006rbacuseralterhostname)
      * 5.2.10.43 [RQ.SRS-006.RBAC.User.Alter.Host.Regexp](#rqsrs-006rbacuseralterhostregexp)
      * 5.2.10.44 [RQ.SRS-006.RBAC.User.Alter.Host.IP](#rqsrs-006rbacuseralterhostip)
      * 5.2.10.45 [RQ.SRS-006.RBAC.User.Alter.Host.Like](#rqsrs-006rbacuseralterhostlike)
      * 5.2.10.46 [RQ.SRS-006.RBAC.User.Alter.Host.Any](#rqsrs-006rbacuseralterhostany)
      * 5.2.10.47 [RQ.SRS-006.RBAC.User.Alter.Host.None](#rqsrs-006rbacuseralterhostnone)
      * 5.2.10.48 [RQ.SRS-006.RBAC.User.Alter.DefaultRole](#rqsrs-006rbacuseralterdefaultrole)
      * 5.2.10.49 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.All](#rqsrs-006rbacuseralterdefaultroleall)
      * 5.2.10.50 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.AllExcept](#rqsrs-006rbacuseralterdefaultroleallexcept)
      * 5.2.10.51 [RQ.SRS-006.RBAC.User.Alter.Settings](#rqsrs-006rbacuseraltersettings)
      * 5.2.10.52 [RQ.SRS-006.RBAC.User.Alter.Settings.Min](#rqsrs-006rbacuseraltersettingsmin)
      * 5.2.10.53 [RQ.SRS-006.RBAC.User.Alter.Settings.Max](#rqsrs-006rbacuseraltersettingsmax)
      * 5.2.10.54 [RQ.SRS-006.RBAC.User.Alter.Settings.Profile](#rqsrs-006rbacuseraltersettingsprofile)
      * 5.2.10.55 [RQ.SRS-006.RBAC.User.Alter.Syntax](#rqsrs-006rbacuseraltersyntax)
      * 5.2.10.56 [RQ.SRS-006.RBAC.SetDefaultRole](#rqsrs-006rbacsetdefaultrole)
      * 5.2.10.57 [RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser](#rqsrs-006rbacsetdefaultrolecurrentuser)
      * 5.2.10.58 [RQ.SRS-006.RBAC.SetDefaultRole.All](#rqsrs-006rbacsetdefaultroleall)
      * 5.2.10.59 [RQ.SRS-006.RBAC.SetDefaultRole.AllExcept](#rqsrs-006rbacsetdefaultroleallexcept)
      * 5.2.10.60 [RQ.SRS-006.RBAC.SetDefaultRole.None](#rqsrs-006rbacsetdefaultrolenone)
      * 5.2.10.61 [RQ.SRS-006.RBAC.SetDefaultRole.Syntax](#rqsrs-006rbacsetdefaultrolesyntax)
      * 5.2.10.62 [RQ.SRS-006.RBAC.SetRole](#rqsrs-006rbacsetrole)
      * 5.2.10.63 [RQ.SRS-006.RBAC.SetRole.Default](#rqsrs-006rbacsetroledefault)
      * 5.2.10.64 [RQ.SRS-006.RBAC.SetRole.None](#rqsrs-006rbacsetrolenone)
      * 5.2.10.65 [RQ.SRS-006.RBAC.SetRole.All](#rqsrs-006rbacsetroleall)
      * 5.2.10.66 [RQ.SRS-006.RBAC.SetRole.AllExcept](#rqsrs-006rbacsetroleallexcept)
      * 5.2.10.67 [RQ.SRS-006.RBAC.SetRole.Syntax](#rqsrs-006rbacsetrolesyntax)
      * 5.2.10.68 [RQ.SRS-006.RBAC.User.ShowCreateUser](#rqsrs-006rbacusershowcreateuser)
      * 5.2.10.69 [RQ.SRS-006.RBAC.User.ShowCreateUser.For](#rqsrs-006rbacusershowcreateuserfor)
      * 5.2.10.70 [RQ.SRS-006.RBAC.User.ShowCreateUser.Syntax](#rqsrs-006rbacusershowcreateusersyntax)
      * 5.2.10.71 [RQ.SRS-006.RBAC.User.Drop](#rqsrs-006rbacuserdrop)
      * 5.2.10.72 [RQ.SRS-006.RBAC.User.Drop.IfExists](#rqsrs-006rbacuserdropifexists)
      * 5.2.10.73 [RQ.SRS-006.RBAC.User.Drop.OnCluster](#rqsrs-006rbacuserdroponcluster)
      * 5.2.10.74 [RQ.SRS-006.RBAC.User.Drop.Syntax](#rqsrs-006rbacuserdropsyntax)
      * 5.2.10.75 [RQ.SRS-006.RBAC.Role.Create](#rqsrs-006rbacrolecreate)
      * 5.2.10.76 [RQ.SRS-006.RBAC.Role.Create.IfNotExists](#rqsrs-006rbacrolecreateifnotexists)
      * 5.2.10.77 [RQ.SRS-006.RBAC.Role.Create.Replace](#rqsrs-006rbacrolecreatereplace)
      * 5.2.10.78 [RQ.SRS-006.RBAC.Role.Create.Settings](#rqsrs-006rbacrolecreatesettings)
      * 5.2.10.79 [RQ.SRS-006.RBAC.Role.Create.Syntax](#rqsrs-006rbacrolecreatesyntax)
      * 5.2.10.80 [RQ.SRS-006.RBAC.Role.Create.Effect](#rqsrs-006rbacrolecreateeffect)
      * 5.2.10.81 [RQ.SRS-006.RBAC.Role.Alter](#rqsrs-006rbacrolealter)
      * 5.2.10.82 [RQ.SRS-006.RBAC.Role.Alter.IfExists](#rqsrs-006rbacrolealterifexists)
      * 5.2.10.83 [RQ.SRS-006.RBAC.Role.Alter.Cluster](#rqsrs-006rbacrolealtercluster)
      * 5.2.10.84 [RQ.SRS-006.RBAC.Role.Alter.Rename](#rqsrs-006rbacrolealterrename)
      * 5.2.10.85 [RQ.SRS-006.RBAC.Role.Alter.Settings](#rqsrs-006rbacrolealtersettings)
      * 5.2.10.86 [RQ.SRS-006.RBAC.Role.Alter.Effect](#rqsrs-006rbacrolealtereffect)
      * 5.2.10.87 [RQ.SRS-006.RBAC.Role.Alter.Syntax](#rqsrs-006rbacrolealtersyntax)
      * 5.2.10.88 [RQ.SRS-006.RBAC.Role.Drop](#rqsrs-006rbacroledrop)
      * 5.2.10.89 [RQ.SRS-006.RBAC.Role.Drop.IfExists](#rqsrs-006rbacroledropifexists)
      * 5.2.10.90 [RQ.SRS-006.RBAC.Role.Drop.Cluster](#rqsrs-006rbacroledropcluster)
      * 5.2.10.91 [RQ.SRS-006.RBAC.Role.Drop.Effect](#rqsrs-006rbacroledropeffect)
      * 5.2.10.92 [RQ.SRS-006.RBAC.Role.Drop.Syntax](#rqsrs-006rbacroledropsyntax)
      * 5.2.10.93 [RQ.SRS-006.RBAC.Role.ShowCreate](#rqsrs-006rbacroleshowcreate)
      * 5.2.10.94 [RQ.SRS-006.RBAC.Role.ShowCreate.Syntax](#rqsrs-006rbacroleshowcreatesyntax)
      * 5.2.10.95 [RQ.SRS-006.RBAC.Grant.Privilege.To](#rqsrs-006rbacgrantprivilegeto)
      * 5.2.10.96 [RQ.SRS-006.RBAC.Grant.Privilege.To.Effect](#rqsrs-006rbacgrantprivilegetoeffect)
      * 5.2.10.97 [RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser](#rqsrs-006rbacgrantprivilegetocurrentuser)
      * 5.2.10.98 [RQ.SRS-006.RBAC.Grant.Privilege.Select](#rqsrs-006rbacgrantprivilegeselect)
      * 5.2.10.99 [RQ.SRS-006.RBAC.Grant.Privilege.Select.Effect](#rqsrs-006rbacgrantprivilegeselecteffect)
      * 5.2.10.100 [RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns](#rqsrs-006rbacgrantprivilegeselectcolumns)
      * 5.2.10.101 [RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns.Effect](#rqsrs-006rbacgrantprivilegeselectcolumnseffect)
      * 5.2.10.102 [RQ.SRS-006.RBAC.Grant.Privilege.Insert](#rqsrs-006rbacgrantprivilegeinsert)
      * 5.2.10.103 [RQ.SRS-006.RBAC.Grant.Privilege.Insert.Effect](#rqsrs-006rbacgrantprivilegeinserteffect)
      * 5.2.10.104 [RQ.SRS-006.RBAC.Grant.Privilege.Alter](#rqsrs-006rbacgrantprivilegealter)
      * 5.2.10.105 [RQ.SRS-006.RBAC.Grant.Privilege.Alter.Effect](#rqsrs-006rbacgrantprivilegealtereffect)
      * 5.2.10.106 [RQ.SRS-006.RBAC.Grant.Privilege.Create](#rqsrs-006rbacgrantprivilegecreate)
      * 5.2.10.107 [RQ.SRS-006.RBAC.Grant.Privilege.Create.Effect](#rqsrs-006rbacgrantprivilegecreateeffect)
      * 5.2.10.108 [RQ.SRS-006.RBAC.Grant.Privilege.Drop](#rqsrs-006rbacgrantprivilegedrop)
      * 5.2.10.109 [RQ.SRS-006.RBAC.Grant.Privilege.Drop.Effect](#rqsrs-006rbacgrantprivilegedropeffect)
      * 5.2.10.110 [RQ.SRS-006.RBAC.Grant.Privilege.Truncate](#rqsrs-006rbacgrantprivilegetruncate)
      * 5.2.10.111 [RQ.SRS-006.RBAC.Grant.Privilege.Truncate.Effect](#rqsrs-006rbacgrantprivilegetruncateeffect)
      * 5.2.10.112 [RQ.SRS-006.RBAC.Grant.Privilege.Optimize](#rqsrs-006rbacgrantprivilegeoptimize)
      * 5.2.10.113 [RQ.SRS-006.RBAC.Grant.Privilege.Optimize.Effect](#rqsrs-006rbacgrantprivilegeoptimizeeffect)
      * 5.2.10.114 [RQ.SRS-006.RBAC.Grant.Privilege.Show](#rqsrs-006rbacgrantprivilegeshow)
      * 5.2.10.115 [RQ.SRS-006.RBAC.Grant.Privilege.Show.Effect](#rqsrs-006rbacgrantprivilegeshoweffect)
      * 5.2.10.116 [RQ.SRS-006.RBAC.Grant.Privilege.KillQuery](#rqsrs-006rbacgrantprivilegekillquery)
      * 5.2.10.117 [RQ.SRS-006.RBAC.Grant.Privilege.KillQuery.Effect](#rqsrs-006rbacgrantprivilegekillqueryeffect)
      * 5.2.10.118 [RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement](#rqsrs-006rbacgrantprivilegeaccessmanagement)
      * 5.2.10.119 [RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement.Effect](#rqsrs-006rbacgrantprivilegeaccessmanagementeffect)
      * 5.2.10.120 [RQ.SRS-006.RBAC.Grant.Privilege.System](#rqsrs-006rbacgrantprivilegesystem)
      * 5.2.10.121 [RQ.SRS-006.RBAC.Grant.Privilege.System.Effect](#rqsrs-006rbacgrantprivilegesystemeffect)
      * 5.2.10.122 [RQ.SRS-006.RBAC.Grant.Privilege.Introspection](#rqsrs-006rbacgrantprivilegeintrospection)
      * 5.2.10.123 [RQ.SRS-006.RBAC.Grant.Privilege.Introspection.Effect](#rqsrs-006rbacgrantprivilegeintrospectioneffect)
      * 5.2.10.124 [RQ.SRS-006.RBAC.Grant.Privilege.Sources](#rqsrs-006rbacgrantprivilegesources)
      * 5.2.10.125 [RQ.SRS-006.RBAC.Grant.Privilege.Sources.Effect](#rqsrs-006rbacgrantprivilegesourceseffect)
      * 5.2.10.126 [RQ.SRS-006.RBAC.Grant.Privilege.DictGet](#rqsrs-006rbacgrantprivilegedictget)
      * 5.2.10.127 [RQ.SRS-006.RBAC.Grant.Privilege.DictGet.Effect](#rqsrs-006rbacgrantprivilegedictgeteffect)
      * 5.2.10.128 [RQ.SRS-006.RBAC.Grant.Privilege.None](#rqsrs-006rbacgrantprivilegenone)
      * 5.2.10.129 [RQ.SRS-006.RBAC.Grant.Privilege.None.Effect](#rqsrs-006rbacgrantprivilegenoneeffect)
      * 5.2.10.130 [RQ.SRS-006.RBAC.Grant.Privilege.All](#rqsrs-006rbacgrantprivilegeall)
      * 5.2.10.131 [RQ.SRS-006.RBAC.Grant.Privilege.All.Effect](#rqsrs-006rbacgrantprivilegealleffect)
      * 5.2.10.132 [RQ.SRS-006.RBAC.Grant.Privilege.GrantOption](#rqsrs-006rbacgrantprivilegegrantoption)
      * 5.2.10.133 [RQ.SRS-006.RBAC.Grant.Privilege.GrantOption.Effect](#rqsrs-006rbacgrantprivilegegrantoptioneffect)
      * 5.2.10.134 [RQ.SRS-006.RBAC.Grant.Privilege.On](#rqsrs-006rbacgrantprivilegeon)
      * 5.2.10.135 [RQ.SRS-006.RBAC.Grant.Privilege.On.Effect](#rqsrs-006rbacgrantprivilegeoneffect)
      * 5.2.10.136 [RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns](#rqsrs-006rbacgrantprivilegeprivilegecolumns)
      * 5.2.10.137 [RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns.Effect](#rqsrs-006rbacgrantprivilegeprivilegecolumnseffect)
      * 5.2.10.138 [RQ.SRS-006.RBAC.Grant.Privilege.OnCluster](#rqsrs-006rbacgrantprivilegeoncluster)
      * 5.2.10.139 [RQ.SRS-006.RBAC.Grant.Privilege.Syntax](#rqsrs-006rbacgrantprivilegesyntax)
      * 5.2.10.140 [RQ.SRS-006.RBAC.Revoke.Privilege.Cluster](#rqsrs-006rbacrevokeprivilegecluster)
      * 5.2.10.141 [RQ.SRS-006.RBAC.Revoke.Privilege.Cluster.Effect](#rqsrs-006rbacrevokeprivilegeclustereffect)
      * 5.2.10.142 [RQ.SRS-006.RBAC.Revoke.Privilege.Any](#rqsrs-006rbacrevokeprivilegeany)
      * 5.2.10.143 [RQ.SRS-006.RBAC.Revoke.Privilege.Any.Effect](#rqsrs-006rbacrevokeprivilegeanyeffect)
      * 5.2.10.144 [RQ.SRS-006.RBAC.Revoke.Privilege.Select](#rqsrs-006rbacrevokeprivilegeselect)
      * 5.2.10.145 [RQ.SRS-006.RBAC.Revoke.Privilege.Select.Effect](#rqsrs-006rbacrevokeprivilegeselecteffect)
      * 5.2.10.146 [RQ.SRS-006.RBAC.Revoke.Privilege.Insert](#rqsrs-006rbacrevokeprivilegeinsert)
      * 5.2.10.147 [RQ.SRS-006.RBAC.Revoke.Privilege.Insert.Effect](#rqsrs-006rbacrevokeprivilegeinserteffect)
      * 5.2.10.148 [RQ.SRS-006.RBAC.Revoke.Privilege.Alter](#rqsrs-006rbacrevokeprivilegealter)
      * 5.2.10.149 [RQ.SRS-006.RBAC.Revoke.Privilege.Alter.Effect](#rqsrs-006rbacrevokeprivilegealtereffect)
      * 5.2.10.150 [RQ.SRS-006.RBAC.Revoke.Privilege.Create](#rqsrs-006rbacrevokeprivilegecreate)
      * 5.2.10.151 [RQ.SRS-006.RBAC.Revoke.Privilege.Create.Effect](#rqsrs-006rbacrevokeprivilegecreateeffect)
      * 5.2.10.152 [RQ.SRS-006.RBAC.Revoke.Privilege.Drop](#rqsrs-006rbacrevokeprivilegedrop)
      * 5.2.10.153 [RQ.SRS-006.RBAC.Revoke.Privilege.Drop.Effect](#rqsrs-006rbacrevokeprivilegedropeffect)
      * 5.2.10.154 [RQ.SRS-006.RBAC.Revoke.Privilege.Truncate](#rqsrs-006rbacrevokeprivilegetruncate)
      * 5.2.10.155 [RQ.SRS-006.RBAC.Revoke.Privilege.Truncate.Effect](#rqsrs-006rbacrevokeprivilegetruncateeffect)
      * 5.2.10.156 [RQ.SRS-006.RBAC.Revoke.Privilege.Optimize](#rqsrs-006rbacrevokeprivilegeoptimize)
      * 5.2.10.157 [RQ.SRS-006.RBAC.Revoke.Privilege.Optimize.Effect](#rqsrs-006rbacrevokeprivilegeoptimizeeffect)
      * 5.2.10.158 [RQ.SRS-006.RBAC.Revoke.Privilege.Show](#rqsrs-006rbacrevokeprivilegeshow)
      * 5.2.10.159 [RQ.SRS-006.RBAC.Revoke.Privilege.Show.Effect](#rqsrs-006rbacrevokeprivilegeshoweffect)
      * 5.2.10.160 [RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery](#rqsrs-006rbacrevokeprivilegekillquery)
      * 5.2.10.161 [RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery.Effect](#rqsrs-006rbacrevokeprivilegekillqueryeffect)
      * 5.2.10.162 [RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement](#rqsrs-006rbacrevokeprivilegeaccessmanagement)
      * 5.2.10.163 [RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement.Effect](#rqsrs-006rbacrevokeprivilegeaccessmanagementeffect)
      * 5.2.10.164 [RQ.SRS-006.RBAC.Revoke.Privilege.System](#rqsrs-006rbacrevokeprivilegesystem)
      * 5.2.10.165 [RQ.SRS-006.RBAC.Revoke.Privilege.System.Effect](#rqsrs-006rbacrevokeprivilegesystemeffect)
      * 5.2.10.166 [RQ.SRS-006.RBAC.Revoke.Privilege.Introspection](#rqsrs-006rbacrevokeprivilegeintrospection)
      * 5.2.10.167 [RQ.SRS-006.RBAC.Revoke.Privilege.Introspection.Effect](#rqsrs-006rbacrevokeprivilegeintrospectioneffect)
      * 5.2.10.168 [RQ.SRS-006.RBAC.Revoke.Privilege.Sources](#rqsrs-006rbacrevokeprivilegesources)
      * 5.2.10.169 [RQ.SRS-006.RBAC.Revoke.Privilege.Sources.Effect](#rqsrs-006rbacrevokeprivilegesourceseffect)
      * 5.2.10.170 [RQ.SRS-006.RBAC.Revoke.Privilege.DictGet](#rqsrs-006rbacrevokeprivilegedictget)
      * 5.2.10.171 [RQ.SRS-006.RBAC.Revoke.Privilege.DictGet.Effect](#rqsrs-006rbacrevokeprivilegedictgeteffect)
      * 5.2.10.172 [RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns](#rqsrs-006rbacrevokeprivilegeprivelegecolumns)
      * 5.2.10.173 [RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns.Effect](#rqsrs-006rbacrevokeprivilegeprivelegecolumnseffect)
      * 5.2.10.174 [RQ.SRS-006.RBAC.Revoke.Privilege.Multiple](#rqsrs-006rbacrevokeprivilegemultiple)
      * 5.2.10.175 [RQ.SRS-006.RBAC.Revoke.Privilege.Multiple.Effect](#rqsrs-006rbacrevokeprivilegemultipleeffect)
      * 5.2.10.176 [RQ.SRS-006.RBAC.Revoke.Privilege.All](#rqsrs-006rbacrevokeprivilegeall)
      * 5.2.10.177 [RQ.SRS-006.RBAC.Revoke.Privilege.All.Effect](#rqsrs-006rbacrevokeprivilegealleffect)
      * 5.2.10.178 [RQ.SRS-006.RBAC.Revoke.Privilege.None](#rqsrs-006rbacrevokeprivilegenone)
      * 5.2.10.179 [RQ.SRS-006.RBAC.Revoke.Privilege.None.Effect](#rqsrs-006rbacrevokeprivilegenoneeffect)
      * 5.2.10.180 [RQ.SRS-006.RBAC.Revoke.Privilege.On](#rqsrs-006rbacrevokeprivilegeon)
      * 5.2.10.181 [RQ.SRS-006.RBAC.Revoke.Privilege.On.Effect](#rqsrs-006rbacrevokeprivilegeoneffect)
      * 5.2.10.182 [RQ.SRS-006.RBAC.Revoke.Privilege.From](#rqsrs-006rbacrevokeprivilegefrom)
      * 5.2.10.183 [RQ.SRS-006.RBAC.Revoke.Privilege.From.Effect](#rqsrs-006rbacrevokeprivilegefromeffect)
      * 5.2.10.184 [RQ.SRS-006.RBAC.Revoke.Privilege.Syntax](#rqsrs-006rbacrevokeprivilegesyntax)
      * 5.2.10.185 [RQ.SRS-006.RBAC.PartialRevoke.Syntax](#rqsrs-006rbacpartialrevokesyntax)
      * 5.2.10.186 [RQ.SRS-006.RBAC.PartialRevoke.Effect](#rqsrs-006rbacpartialrevokeeffect)
      * 5.2.10.187 [RQ.SRS-006.RBAC.Grant.Role](#rqsrs-006rbacgrantrole)
      * 5.2.10.188 [RQ.SRS-006.RBAC.Grant.Role.Effect](#rqsrs-006rbacgrantroleeffect)
      * 5.2.10.189 [RQ.SRS-006.RBAC.Grant.Role.CurrentUser](#rqsrs-006rbacgrantrolecurrentuser)
      * 5.2.10.190 [RQ.SRS-006.RBAC.Grant.Role.CurrentUser.Effect](#rqsrs-006rbacgrantrolecurrentusereffect)
      * 5.2.10.191 [RQ.SRS-006.RBAC.Grant.Role.AdminOption](#rqsrs-006rbacgrantroleadminoption)
      * 5.2.10.192 [RQ.SRS-006.RBAC.Grant.Role.AdminOption.Effect](#rqsrs-006rbacgrantroleadminoptioneffect)
      * 5.2.10.193 [RQ.SRS-006.RBAC.Grant.Role.OnCluster](#rqsrs-006rbacgrantroleoncluster)
      * 5.2.10.194 [RQ.SRS-006.RBAC.Grant.Role.Syntax](#rqsrs-006rbacgrantrolesyntax)
      * 5.2.10.195 [RQ.SRS-006.RBAC.Revoke.Role](#rqsrs-006rbacrevokerole)
      * 5.2.10.196 [RQ.SRS-006.RBAC.Revoke.Role.Effect](#rqsrs-006rbacrevokeroleeffect)
      * 5.2.10.197 [RQ.SRS-006.RBAC.Revoke.Role.Keywords](#rqsrs-006rbacrevokerolekeywords)
      * 5.2.10.198 [RQ.SRS-006.RBAC.Revoke.Role.Keywords.Effect](#rqsrs-006rbacrevokerolekeywordseffect)
      * 5.2.10.199 [RQ.SRS-006.RBAC.Revoke.Role.Cluster](#rqsrs-006rbacrevokerolecluster)
      * 5.2.10.200 [RQ.SRS-006.RBAC.Revoke.Role.Cluster.Effect](#rqsrs-006rbacrevokeroleclustereffect)
      * 5.2.10.201 [RQ.SRS-006.RBAC.Revoke.AdminOption](#rqsrs-006rbacrevokeadminoption)
      * 5.2.10.202 [RQ.SRS-006.RBAC.Revoke.AdminOption.Effect](#rqsrs-006rbacrevokeadminoptioneffect)
      * 5.2.10.203 [RQ.SRS-006.RBAC.Revoke.Role.Syntax](#rqsrs-006rbacrevokerolesyntax)
      * 5.2.10.204 [RQ.SRS-006.RBAC.Show.Grants](#rqsrs-006rbacshowgrants)
      * 5.2.10.205 [RQ.SRS-006.RBAC.Show.Grants.For](#rqsrs-006rbacshowgrantsfor)
      * 5.2.10.206 [RQ.SRS-006.RBAC.Show.Grants.Syntax](#rqsrs-006rbacshowgrantssyntax)
      * 5.2.10.207 [RQ.SRS-006.RBAC.SettingsProfile.Create](#rqsrs-006rbacsettingsprofilecreate)
      * 5.2.10.208 [RQ.SRS-006.RBAC.SettingsProfile.Create.Effect](#rqsrs-006rbacsettingsprofilecreateeffect)
      * 5.2.10.209 [RQ.SRS-006.RBAC.SettingsProfile.Create.IfNotExists](#rqsrs-006rbacsettingsprofilecreateifnotexists)
      * 5.2.10.210 [RQ.SRS-006.RBAC.SettingsProfile.Create.Replace](#rqsrs-006rbacsettingsprofilecreatereplace)
      * 5.2.10.211 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables](#rqsrs-006rbacsettingsprofilecreatevariables)
      * 5.2.10.212 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value](#rqsrs-006rbacsettingsprofilecreatevariablesvalue)
      * 5.2.10.213 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value.Effect](#rqsrs-006rbacsettingsprofilecreatevariablesvalueeffect)
      * 5.2.10.214 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints](#rqsrs-006rbacsettingsprofilecreatevariablesconstraints)
      * 5.2.10.215 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints.Effect](#rqsrs-006rbacsettingsprofilecreatevariablesconstraintseffect)
      * 5.2.10.216 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment](#rqsrs-006rbacsettingsprofilecreateassignment)
      * 5.2.10.217 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.None](#rqsrs-006rbacsettingsprofilecreateassignmentnone)
      * 5.2.10.218 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.All](#rqsrs-006rbacsettingsprofilecreateassignmentall)
      * 5.2.10.219 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilecreateassignmentallexcept)
      * 5.2.10.220 [RQ.SRS-006.RBAC.SettingsProfile.Create.Inherit](#rqsrs-006rbacsettingsprofilecreateinherit)
      * 5.2.10.221 [RQ.SRS-006.RBAC.SettingsProfile.Create.OnCluster](#rqsrs-006rbacsettingsprofilecreateoncluster)
      * 5.2.10.222 [RQ.SRS-006.RBAC.SettingsProfile.Create.Syntax](#rqsrs-006rbacsettingsprofilecreatesyntax)
      * 5.2.10.223 [RQ.SRS-006.RBAC.SettingsProfile.Alter](#rqsrs-006rbacsettingsprofilealter)
      * 5.2.10.224 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Effect](#rqsrs-006rbacsettingsprofilealtereffect)
      * 5.2.10.225 [RQ.SRS-006.RBAC.SettingsProfile.Alter.IfExists](#rqsrs-006rbacsettingsprofilealterifexists)
      * 5.2.10.226 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Rename](#rqsrs-006rbacsettingsprofilealterrename)
      * 5.2.10.227 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables](#rqsrs-006rbacsettingsprofilealtervariables)
      * 5.2.10.228 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value](#rqsrs-006rbacsettingsprofilealtervariablesvalue)
      * 5.2.10.229 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value.Effect](#rqsrs-006rbacsettingsprofilealtervariablesvalueeffect)
      * 5.2.10.230 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints](#rqsrs-006rbacsettingsprofilealtervariablesconstraints)
      * 5.2.10.231 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints.Effect](#rqsrs-006rbacsettingsprofilealtervariablesconstraintseffect)
      * 5.2.10.232 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment](#rqsrs-006rbacsettingsprofilealterassignment)
      * 5.2.10.233 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Effect](#rqsrs-006rbacsettingsprofilealterassignmenteffect)
      * 5.2.10.234 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.None](#rqsrs-006rbacsettingsprofilealterassignmentnone)
      * 5.2.10.235 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.All](#rqsrs-006rbacsettingsprofilealterassignmentall)
      * 5.2.10.236 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilealterassignmentallexcept)
      * 5.2.10.237 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Inherit](#rqsrs-006rbacsettingsprofilealterassignmentinherit)
      * 5.2.10.238 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.OnCluster](#rqsrs-006rbacsettingsprofilealterassignmentoncluster)
      * 5.2.10.239 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Syntax](#rqsrs-006rbacsettingsprofilealtersyntax)
      * 5.2.10.240 [RQ.SRS-006.RBAC.SettingsProfile.Drop](#rqsrs-006rbacsettingsprofiledrop)
      * 5.2.10.241 [RQ.SRS-006.RBAC.SettingsProfile.Drop.Effect](#rqsrs-006rbacsettingsprofiledropeffect)
      * 5.2.10.242 [RQ.SRS-006.RBAC.SettingsProfile.Drop.IfExists](#rqsrs-006rbacsettingsprofiledropifexists)
      * 5.2.10.243 [RQ.SRS-006.RBAC.SettingsProfile.Drop.OnCluster](#rqsrs-006rbacsettingsprofiledroponcluster)
      * 5.2.10.244 [RQ.SRS-006.RBAC.SettingsProfile.Drop.Syntax](#rqsrs-006rbacsettingsprofiledropsyntax)
      * 5.2.10.245 [RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile](#rqsrs-006rbacsettingsprofileshowcreatesettingsprofile)
      * 5.2.10.246 [RQ.SRS-006.RBAC.Quota.Create](#rqsrs-006rbacquotacreate)
      * 5.2.10.247 [RQ.SRS-006.RBAC.Quota.Create.Effect](#rqsrs-006rbacquotacreateeffect)
      * 5.2.10.248 [RQ.SRS-006.RBAC.Quota.Create.IfNotExists](#rqsrs-006rbacquotacreateifnotexists)
      * 5.2.10.249 [RQ.SRS-006.RBAC.Quota.Create.Replace](#rqsrs-006rbacquotacreatereplace)
      * 5.2.10.250 [RQ.SRS-006.RBAC.Quota.Create.Cluster](#rqsrs-006rbacquotacreatecluster)
      * 5.2.10.251 [RQ.SRS-006.RBAC.Quota.Create.Interval](#rqsrs-006rbacquotacreateinterval)
      * 5.2.10.252 [RQ.SRS-006.RBAC.Quota.Create.Interval.Randomized](#rqsrs-006rbacquotacreateintervalrandomized)
      * 5.2.10.253 [RQ.SRS-006.RBAC.Quota.Create.Queries](#rqsrs-006rbacquotacreatequeries)
      * 5.2.10.254 [RQ.SRS-006.RBAC.Quota.Create.Errors](#rqsrs-006rbacquotacreateerrors)
      * 5.2.10.255 [RQ.SRS-006.RBAC.Quota.Create.ResultRows](#rqsrs-006rbacquotacreateresultrows)
      * 5.2.10.256 [RQ.SRS-006.RBAC.Quota.Create.ReadRows](#rqsrs-006rbacquotacreatereadrows)
      * 5.2.10.257 [RQ.SRS-006.RBAC.Quota.Create.ResultBytes](#rqsrs-006rbacquotacreateresultbytes)
      * 5.2.10.258 [RQ.SRS-006.RBAC.Quota.Create.ReadBytes](#rqsrs-006rbacquotacreatereadbytes)
      * 5.2.10.259 [RQ.SRS-006.RBAC.Quota.Create.ExecutionTime](#rqsrs-006rbacquotacreateexecutiontime)
      * 5.2.10.260 [RQ.SRS-006.RBAC.Quota.Create.NoLimits](#rqsrs-006rbacquotacreatenolimits)
      * 5.2.10.261 [RQ.SRS-006.RBAC.Quota.Create.TrackingOnly](#rqsrs-006rbacquotacreatetrackingonly)
      * 5.2.10.262 [RQ.SRS-006.RBAC.Quota.Create.KeyedBy](#rqsrs-006rbacquotacreatekeyedby)
      * 5.2.10.263 [RQ.SRS-006.RBAC.Quota.Create.KeyedByOptions](#rqsrs-006rbacquotacreatekeyedbyoptions)
      * 5.2.10.264 [RQ.SRS-006.RBAC.Quota.Create.Assignment](#rqsrs-006rbacquotacreateassignment)
      * 5.2.10.265 [RQ.SRS-006.RBAC.Quota.Create.Assignment.None](#rqsrs-006rbacquotacreateassignmentnone)
      * 5.2.10.266 [RQ.SRS-006.RBAC.Quota.Create.Assignment.All](#rqsrs-006rbacquotacreateassignmentall)
      * 5.2.10.267 [RQ.SRS-006.RBAC.Quota.Create.Assignment.Except](#rqsrs-006rbacquotacreateassignmentexcept)
      * 5.2.10.268 [RQ.SRS-006.RBAC.Quota.Create.Syntax](#rqsrs-006rbacquotacreatesyntax)
      * 5.2.10.269 [RQ.SRS-006.RBAC.Quota.Alter](#rqsrs-006rbacquotaalter)
      * 5.2.10.270 [RQ.SRS-006.RBAC.Quota.Alter.Effect](#rqsrs-006rbacquotaaltereffect)
      * 5.2.10.271 [RQ.SRS-006.RBAC.Quota.Alter.IfExists](#rqsrs-006rbacquotaalterifexists)
      * 5.2.10.272 [RQ.SRS-006.RBAC.Quota.Alter.Rename](#rqsrs-006rbacquotaalterrename)
      * 5.2.10.273 [RQ.SRS-006.RBAC.Quota.Alter.Cluster](#rqsrs-006rbacquotaaltercluster)
      * 5.2.10.274 [RQ.SRS-006.RBAC.Quota.Alter.Interval](#rqsrs-006rbacquotaalterinterval)
      * 5.2.10.275 [RQ.SRS-006.RBAC.Quota.Alter.Interval.Randomized](#rqsrs-006rbacquotaalterintervalrandomized)
      * 5.2.10.276 [RQ.SRS-006.RBAC.Quota.Alter.Queries](#rqsrs-006rbacquotaalterqueries)
      * 5.2.10.277 [RQ.SRS-006.RBAC.Quota.Alter.Errors](#rqsrs-006rbacquotaaltererrors)
      * 5.2.10.278 [RQ.SRS-006.RBAC.Quota.Alter.ResultRows](#rqsrs-006rbacquotaalterresultrows)
      * 5.2.10.279 [RQ.SRS-006.RBAC.Quota.Alter.ReadRows](#rqsrs-006rbacquotaalterreadrows)
      * 5.2.10.280 [RQ.SRS-006.RBAC.Quota.ALter.ResultBytes](#rqsrs-006rbacquotaalterresultbytes)
      * 5.2.10.281 [RQ.SRS-006.RBAC.Quota.Alter.ReadBytes](#rqsrs-006rbacquotaalterreadbytes)
      * 5.2.10.282 [RQ.SRS-006.RBAC.Quota.Alter.ExecutionTime](#rqsrs-006rbacquotaalterexecutiontime)
      * 5.2.10.283 [RQ.SRS-006.RBAC.Quota.Alter.NoLimits](#rqsrs-006rbacquotaalternolimits)
      * 5.2.10.284 [RQ.SRS-006.RBAC.Quota.Alter.TrackingOnly](#rqsrs-006rbacquotaaltertrackingonly)
      * 5.2.10.285 [RQ.SRS-006.RBAC.Quota.Alter.KeyedBy](#rqsrs-006rbacquotaalterkeyedby)
      * 5.2.10.286 [RQ.SRS-006.RBAC.Quota.Alter.KeyedByOptions](#rqsrs-006rbacquotaalterkeyedbyoptions)
      * 5.2.10.287 [RQ.SRS-006.RBAC.Quota.Alter.Assignment](#rqsrs-006rbacquotaalterassignment)
      * 5.2.10.288 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.None](#rqsrs-006rbacquotaalterassignmentnone)
      * 5.2.10.289 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.All](#rqsrs-006rbacquotaalterassignmentall)
      * 5.2.10.290 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.Except](#rqsrs-006rbacquotaalterassignmentexcept)
      * 5.2.10.291 [RQ.SRS-006.RBAC.Quota.Alter.Syntax](#rqsrs-006rbacquotaaltersyntax)
      * 5.2.10.292 [RQ.SRS-006.RBAC.Quota.Drop](#rqsrs-006rbacquotadrop)
      * 5.2.10.293 [RQ.SRS-006.RBAC.Quota.Drop.Effect](#rqsrs-006rbacquotadropeffect)
      * 5.2.10.294 [RQ.SRS-006.RBAC.Quota.Drop.IfExists](#rqsrs-006rbacquotadropifexists)
      * 5.2.10.295 [RQ.SRS-006.RBAC.Quota.Drop.Cluster](#rqsrs-006rbacquotadropcluster)
      * 5.2.10.296 [RQ.SRS-006.RBAC.Quota.Drop.Syntax](#rqsrs-006rbacquotadropsyntax)
      * 5.2.10.297 [RQ.SRS-006.RBAC.Quota.ShowQuotas](#rqsrs-006rbacquotashowquotas)
      * 5.2.10.298 [RQ.SRS-006.RBAC.Quota.ShowQuotas.IntoOutfile](#rqsrs-006rbacquotashowquotasintooutfile)
      * 5.2.10.299 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Format](#rqsrs-006rbacquotashowquotasformat)
      * 5.2.10.300 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Settings](#rqsrs-006rbacquotashowquotassettings)
      * 5.2.10.301 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Syntax](#rqsrs-006rbacquotashowquotassyntax)
      * 5.2.10.302 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Name](#rqsrs-006rbacquotashowcreatequotaname)
      * 5.2.10.303 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Current](#rqsrs-006rbacquotashowcreatequotacurrent)
      * 5.2.10.304 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Syntax](#rqsrs-006rbacquotashowcreatequotasyntax)
      * 5.2.10.305 [RQ.SRS-006.RBAC.RowPolicy.Create](#rqsrs-006rbacrowpolicycreate)
      * 5.2.10.306 [RQ.SRS-006.RBAC.RowPolicy.Create.Effect](#rqsrs-006rbacrowpolicycreateeffect)
      * 5.2.10.307 [RQ.SRS-006.RBAC.RowPolicy.Create.IfNotExists](#rqsrs-006rbacrowpolicycreateifnotexists)
      * 5.2.10.308 [RQ.SRS-006.RBAC.RowPolicy.Create.Replace](#rqsrs-006rbacrowpolicycreatereplace)
      * 5.2.10.309 [RQ.SRS-006.RBAC.RowPolicy.Create.OnCluster](#rqsrs-006rbacrowpolicycreateoncluster)
      * 5.2.10.310 [RQ.SRS-006.RBAC.RowPolicy.Create.On](#rqsrs-006rbacrowpolicycreateon)
      * 5.2.10.311 [RQ.SRS-006.RBAC.RowPolicy.Create.Access](#rqsrs-006rbacrowpolicycreateaccess)
      * 5.2.10.312 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Permissive](#rqsrs-006rbacrowpolicycreateaccesspermissive)
      * 5.2.10.313 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Restrictive](#rqsrs-006rbacrowpolicycreateaccessrestrictive)
      * 5.2.10.314 [RQ.SRS-006.RBAC.RowPolicy.Create.ForSelect](#rqsrs-006rbacrowpolicycreateforselect)
      * 5.2.10.315 [RQ.SRS-006.RBAC.RowPolicy.Create.Condition](#rqsrs-006rbacrowpolicycreatecondition)
      * 5.2.10.316 [RQ.SRS-006.RBAC.RowPolicy.Create.Condition.Effect](#rqsrs-006rbacrowpolicycreateconditioneffect)
      * 5.2.10.317 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment](#rqsrs-006rbacrowpolicycreateassignment)
      * 5.2.10.318 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.None](#rqsrs-006rbacrowpolicycreateassignmentnone)
      * 5.2.10.319 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.All](#rqsrs-006rbacrowpolicycreateassignmentall)
      * 5.2.10.320 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.AllExcept](#rqsrs-006rbacrowpolicycreateassignmentallexcept)
      * 5.2.10.321 [RQ.SRS-006.RBAC.RowPolicy.Create.Syntax](#rqsrs-006rbacrowpolicycreatesyntax)
      * 5.2.10.322 [RQ.SRS-006.RBAC.RowPolicy.Alter](#rqsrs-006rbacrowpolicyalter)
      * 5.2.10.323 [RQ.SRS-006.RBAC.RowPolicy.Alter.Effect](#rqsrs-006rbacrowpolicyaltereffect)
      * 5.2.10.324 [RQ.SRS-006.RBAC.RowPolicy.Alter.IfExists](#rqsrs-006rbacrowpolicyalterifexists)
      * 5.2.10.325 [RQ.SRS-006.RBAC.RowPolicy.Alter.ForSelect](#rqsrs-006rbacrowpolicyalterforselect)
      * 5.2.10.326 [RQ.SRS-006.RBAC.RowPolicy.Alter.OnCluster](#rqsrs-006rbacrowpolicyalteroncluster)
      * 5.2.10.327 [RQ.SRS-006.RBAC.RowPolicy.Alter.On](#rqsrs-006rbacrowpolicyalteron)
      * 5.2.10.328 [RQ.SRS-006.RBAC.RowPolicy.Alter.Rename](#rqsrs-006rbacrowpolicyalterrename)
      * 5.2.10.329 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access](#rqsrs-006rbacrowpolicyalteraccess)
      * 5.2.10.330 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive](#rqsrs-006rbacrowpolicyalteraccesspermissive)
      * 5.2.10.331 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive](#rqsrs-006rbacrowpolicyalteraccessrestrictive)
      * 5.2.10.332 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition](#rqsrs-006rbacrowpolicyaltercondition)
      * 5.2.10.333 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.Effect](#rqsrs-006rbacrowpolicyalterconditioneffect)
      * 5.2.10.334 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.None](#rqsrs-006rbacrowpolicyalterconditionnone)
      * 5.2.10.335 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment](#rqsrs-006rbacrowpolicyalterassignment)
      * 5.2.10.336 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.None](#rqsrs-006rbacrowpolicyalterassignmentnone)
      * 5.2.10.337 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.All](#rqsrs-006rbacrowpolicyalterassignmentall)
      * 5.2.10.338 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.AllExcept](#rqsrs-006rbacrowpolicyalterassignmentallexcept)
      * 5.2.10.339 [RQ.SRS-006.RBAC.RowPolicy.Alter.Syntax](#rqsrs-006rbacrowpolicyaltersyntax)
      * 5.2.10.340 [RQ.SRS-006.RBAC.RowPolicy.Drop](#rqsrs-006rbacrowpolicydrop)
      * 5.2.10.341 [RQ.SRS-006.RBAC.RowPolicy.Drop.Effect](#rqsrs-006rbacrowpolicydropeffect)
      * 5.2.10.342 [RQ.SRS-006.RBAC.RowPolicy.Drop.IfExists](#rqsrs-006rbacrowpolicydropifexists)
      * 5.2.10.343 [RQ.SRS-006.RBAC.RowPolicy.Drop.On](#rqsrs-006rbacrowpolicydropon)
      * 5.2.10.344 [RQ.SRS-006.RBAC.RowPolicy.Drop.OnCluster](#rqsrs-006rbacrowpolicydroponcluster)
      * 5.2.10.345 [RQ.SRS-006.RBAC.RowPolicy.Drop.Syntax](#rqsrs-006rbacrowpolicydropsyntax)
      * 5.2.10.346 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy](#rqsrs-006rbacrowpolicyshowcreaterowpolicy)
      * 5.2.10.347 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.On](#rqsrs-006rbacrowpolicyshowcreaterowpolicyon)
      * 5.2.10.348 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.Syntax](#rqsrs-006rbacrowpolicyshowcreaterowpolicysyntax)
      * 5.2.10.349 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies](#rqsrs-006rbacrowpolicyshowrowpolicies)
      * 5.2.10.350 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.On](#rqsrs-006rbacrowpolicyshowrowpolicieson)
      * 5.2.10.351 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.Syntax](#rqsrs-006rbacrowpolicyshowrowpoliciessyntax)
* 6 [References](#references)

## Revision History

This document is stored in an electronic form using [Git] source control management software
hosted in a Gitlab repository.

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
* **select columns** -
  privilege to read specific columns from a table
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
  privilege that includes **usage**, **select**, **select columns**,
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

#### Privileges

##### RQ.SRS-006.RBAC.Privileges.Usage
version: 1.0

[ClickHouse] SHALL support granting or revoking **usage** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.Select
version: 1.0

[ClickHouse] SHALL support granting or revoking **select** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.SelectColumns
version: 1.0

[ClickHouse] SHALL support granting or revoking **select columns** privilege
for a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.Insert
version: 1.0

[ClickHouse] SHALL support granting or revoking **insert** privilege
for a database or a specific table to one or more **users** or **roles**.

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

##### RQ.SRS-006.RBAC.Privileges.GrantOption
version: 1.0

[ClickHouse] SHALL support granting or revoking **grant option** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AdminOption
version: 1.0

[ClickHouse] SHALL support granting or revoking **admin option** privilege
to one or more **users** or **roles**.

#### Required Privileges

##### RQ.SRS-006.RBAC.RequiredPrivileges.Insert
version: 1.0

[ClickHouse] SHALL not allow any `INSERT INTO` statements
to be executed unless the user has the **insert** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.RequiredPrivileges.Select
version: 1.0

[ClickHouse] SHALL not allow any `SELECT` statements
to be executed unless the user has the **select** or **select columns** privilege
for the destination table either because of the explicit grant
or through one of the roles assigned to the user.
If the the user only has the **select columns**
privilege then only the specified columns SHALL be available for reading.

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

##### RQ.SRS-006.RBAC.Role.Create.Effect
version: 1.0

[ClickHouse] SHALL make the role available to be linked with users, privileges, quotas and
settings profiles after the successful execution of the `CREATE ROLE` statement.

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

##### RQ.SRS-006.RBAC.Role.Alter.Effect
version: 1.0

[ClickHouse] SHALL alter the abilities granted by the role
from all the users to which the role was assigned after the successful execution
of the `ALTER ROLE` statement. Operations in progress SHALL be allowed to complete as is, but any new operation that requires the privileges that not otherwise granted to the user SHALL fail.

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

##### RQ.SRS-006.RBAC.Role.Drop.Effect
version: 1.0

[ClickHouse] SHALL remove the abilities granted by the role
from all the users to which the role was assigned after the successful execution
of the `DROP ROLE` statement. Operations in progress SHALL be allowed to complete
but any new operation that requires the privileges that not otherwise granted to
the user SHALL fail.

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

##### RQ.SRS-006.RBAC.Grant.Privilege.To.Effect
version: 1.0

[ClickHouse] SHALL grant privileges to any set of users and/or roles specified in the `TO` clause of the grant statement.
Any new operation by one of the specified users or roles with the granted privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser
version: 1.0

[ClickHouse] SHALL support granting privileges to current user using `TO CURRENT_USER` clause
in the `GRANT PRIVILEGE` statement.


##### RQ.SRS-006.RBAC.Grant.Privilege.Select
version: 1.0

[ClickHouse] SHALL support granting the **select** privilege to one or more users or roles
for a database or a table using the `GRANT SELECT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Select.Effect
version: 1.0

[ClickHouse] SHALL add the **select** privilege to the specified users or roles
after the successful execution of the `GRANT SELECT` statement.
Any new operation by a user or a user that has the specified role
which requires the **select** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns
version: 1.0

[ClickHouse] SHALL support granting the **select columns** privilege to one or more users or roles
for a database or a table using the `GRANT SELECT(columns)` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.SelectColumns.Effect
version: 1.0

[ClickHouse] SHALL add the **select columns** privilege to the specified users or roles
after the successful execution of the `GRANT SELECT(columns)` statement.
Any new operation by a user or a user that has the specified role
which requires the **select columns** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support granting the **insert** privilege to one or more users or roles
for a database or a table using the `GRANT INSERT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Insert.Effect
version: 1.0

[ClickHouse] SHALL add the **insert** privilege to the specified users or roles
after the successful execution of the `GRANT INSERT` statement.
Any new operation by a user or a user that has the specified role
which requires the **insert** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support granting the **alter** privilege to one or more users or roles
for a database or a table using the `GRANT ALTER` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Alter.Effect
version: 1.0

[ClickHouse] SHALL add the **alter** privilege to the specified users or roles
after the successful execution of the `GRANT ALTER` statement.
Any new operation by a user or a user that has the specified role
which requires the **alter** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Create
version: 1.0

[ClickHouse] SHALL support granting the **create** privilege to one or more users or roles
for a database or a table using the `GRANT CREATE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Create.Effect
version: 1.0

[ClickHouse] SHALL add the **create** privilege to the specified users or roles
after the successful execution of the `GRANT CREATE` statement.
Any new operation by a user or a user that has the specified role
which requires the **create** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support granting the **drop** privilege to one or more users or roles
for a database or a table using the `GRANT DROP` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Drop.Effect
version: 1.0

[ClickHouse] SHALL add the **drop** privilege to the specified users or roles
after the successful execution of the `GRANT DROP` statement.
Any new operation by a user or a user that has the specified role
which requires the **drop** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support granting the **truncate** privilege to one or more users or roles
for a database or a table using `GRANT TRUNCATE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Truncate.Effect
version: 1.0

[ClickHouse] SHALL add the **truncate** privilege to the specified users or roles
after the successful execution of the `GRANT TRUNCATE` statement.
Any new operation by a user or a user that has the specified role
which requires the **truncate** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support granting the **optimize** privilege to one or more users or roles
for a database or a table using `GRANT OPTIMIZE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Optimize.Effect
version: 1.0

[ClickHouse] SHALL add the **optimize** privilege to the specified users or roles
after the successful execution of the `GRANT OPTIMIZE` statement.
Any new operation by a user or a user that has the specified role
which requires the **optimize** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Show
version: 1.0

[ClickHouse] SHALL support granting the **show** privilege to one or more users or roles
for a database or a table using `GRANT SHOW` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Show.Effect
version: 1.0

[ClickHouse] SHALL add the **show** privilege to the specified users or roles
after the successful execution of the `GRANT SHOW` statement.
Any new operation by a user or a user that has the specified role
which requires the **show** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support granting the **kill query** privilege to one or more users or roles
for a database or a table using `GRANT KILL QUERY` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.KillQuery.Effect
version: 1.0

[ClickHouse] SHALL add the **kill query** privilege to the specified users or roles
after the successful execution of the `GRANT KILL QUERY` statement.
Any new operation by a user or a user that has the specified role
which requires the **kill query** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support granting the **access management** privileges to one or more users or roles
for a database or a table using `GRANT ACCESS MANAGEMENT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement.Effect
version: 1.0

[ClickHouse] SHALL add the **access management** privileges to the specified users or roles
after the successful execution of the `GRANT ACCESS MANAGEMENT` statement.
Any new operation by a user or a user that has the specified role
which requires the **access management** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.System
version: 1.0

[ClickHouse] SHALL support granting the **system** privileges to one or more users or roles
for a database or a table using `GRANT SYSTEM` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.System.Effect
version: 1.0

[ClickHouse] SHALL add the **system** privileges to the specified users or roles
after the successful execution of the `GRANT SYSTEM` statement.
Any new operation by a user or a user that has the specified role
which requires the **system** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support granting the **introspection** privileges to one or more users or roles
for a database or a table using `GRANT INTROSPECTION` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Introspection.Effect
version: 1.0

[ClickHouse] SHALL add the **introspection** privileges to the specified users or roles
after the successful execution of the `GRANT INTROSPECTION` statement.
Any new operation by a user or a user that has the specified role
which requires the **introspection** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support granting the **sources** privileges to one or more users or roles
for a database or a table using `GRANT SOURCES` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Sources.Effect
version: 1.0

[ClickHouse] SHALL add the **sources** privileges to the specified users or roles
after the successful execution of the `GRANT SOURCES` statement.
Any new operation by a user or a user that has the specified role
which requires the **sources** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support granting the **dictGet** privilege to one or more users or roles
for a database or a table using `GRANT dictGet` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.DictGet.Effect
version: 1.0

[ClickHouse] SHALL add the **dictGet** privileges to the specified users or roles
after the successful execution of the `GRANT dictGet` statement.
Any new operation by a user or a user that has the specified role
which requires the **dictGet** privilege SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.None
version: 1.0

[ClickHouse] SHALL support granting no privileges to one or more users or roles
for a database or a table using `GRANT NONE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.None.Effect
version: 1.0

[ClickHouse] SHALL add no privileges to the specified users or roles
after the successful execution of the `GRANT NONE` statement.
Any new operation by a user or a user that has the specified role
which requires no privileges SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.All
version: 1.0

[ClickHouse] SHALL support granting the **all** privileges to one or more users or roles
for a database or a table using the `GRANT ALL` or `GRANT ALL PRIVILEGES` statements.

##### RQ.SRS-006.RBAC.Grant.Privilege.All.Effect
version: 1.0

[ClickHouse] SHALL add the **all** privileges to the specified users or roles
after the successful execution of the `GRANT ALL` or `GRANT ALL PRIVILEGES` statement.
Any new operation by a user or a user that has the specified role
which requires one or more privileges that are part of the **all**
privileges SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.GrantOption
version: 1.0

[ClickHouse] SHALL support granting the **grant option** privilege to one or more users or roles
for a database or a table using the `WITH GRANT OPTION` clause in the `GRANT` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.GrantOption.Effect
version: 1.0

[ClickHouse] SHALL add the **grant option** privilege to the specified users or roles
after the successful execution of the `GRANT` statement with the `WITH GRANT OPTION` clause
for the privilege that was specified in the statement.
Any new `GRANT` statements executed by a user or a user that has the specified role
which requires **grant option** for the privilege SHALL succeed.

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

##### RQ.SRS-006.RBAC.Grant.Privilege.On.Effect
version: 1.0

[ClickHouse] SHALL grant privilege on a table specified in the `ON` clause.
Any new operation by user or role with privilege on the granted table SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns
version: 1.0

[ClickHouse] SHALL support granting the privilege **some_privilege** to one or more users or roles
for a database or a table using the `GRANT some_privilege(column)` statement for one column.
Multiple columns will be supported with `GRANT some_privilege(column1, column2...)` statement.
The privileges will be granted for only the specified columns.

##### RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns.Effect
version: 1.0

[ClickHouse] SHALL grant the privilege **some_privilege** to the specified users or roles
after the successful execution of the `GRANT some_privilege(column)` statement for the specified column.
Granting of the privilege **some_privilege** over multiple columns SHALL happen after the successful
execution of the `GRANT some_privilege(column1, column2...)` statement.
Any new operation by a user or a user that had the specified role
which requires the privilege **some_privilege** over specified columns SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Privilege.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which to grant privileges using the `ON CLUSTER`
clause in the `GRANT PRIVILEGE` statement.

##### RQ.SRS-006.RBAC.Grant.Privilege.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `GRANT` statement that
grants explicit privileges to a user or a role.

```sql
GRANT [ON CLUSTER cluster_name]
    privilege {SELECT | SELECT(columns) | INSERT | ALTER | CREATE | DROP | TRUNCATE | OPTIMIZE | SHOW | KILL QUERY | ACCESS MANAGEMENT | SYSTEM | INTROSPECTION | SOURCES | dictGet | NONE |ALL 	[PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    TO {user | role | CURRENT_USER} [,...]
    [WITH GRANT OPTION]
```

##### RQ.SRS-006.RBAC.Revoke.Privilege.Cluster
version: 1.0

[ClickHouse] SHALL support revoking privileges to one or more users or roles
for a database or a table on some specific cluster using the `REVOKE ON CLUSTER cluster_name` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Cluster.Effect
version: 1.0

[ClickHouse] SHALL remove some privilege from the specified users or roles
on cluster **cluster_name** after the successful execution of the
`REVOKE ON CLUSTER cluster_name some_privilege` statement. Any new operation by a user or a user
that had the specified role which requires that privilege on cluster **cluster_name** SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Any
version: 1.0

[ClickHouse] SHALL support revoking ANY privilege to one or more users or roles
for a database or a table using the `REVOKE some_privilege` statement.
**some_privilege** refers to any Clickhouse defined privilege, whose hierarchy includes
SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,
SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Any.Effect
version: 1.0

[ClickHouse] SHALL remove the **some_privilege** privilege from the specified users or roles
after the successful execution of the `REVOKE some_privilege` statement.
Any new operation by a user or a user that had the specified role
which requires the privilege **some_privilege** SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Select
version: 1.0

[ClickHouse] SHALL support revoking the **select** privilege to one or more users or roles
for a database or a table using the `REVOKE SELECT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Select.Effect
version: 1.0

[ClickHouse] SHALL remove the **select** privilege from the specified users or roles
after the successful execution of the `REVOKE SELECT` statement.
Any new operation by a user or a user that had the specified role
which requires the **select** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support revoking the **insert** privilege to one or more users or roles
for a database or a table using the `REVOKE INSERT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Insert.Effect
version: 1.0

[ClickHouse] SHALL remove the **insert** privilege from the specified users or roles
after the successful execution of the `REVOKE INSERT` statement.
Any new operation by a user or a user that had the specified role
which requires the **insert** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support revoking the **alter** privilege to one or more users or roles
for a database or a table using the `REVOKE ALTER` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Alter.Effect
version: 1.0

[ClickHouse] SHALL remove the **alter** privilege from the specified users or roles
after the successful execution of the `REVOKE ALTER` statement.
Any new operation by a user or a user that had the specified role
which requires the **alter** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Create
version: 1.0

[ClickHouse] SHALL support revoking the **create** privilege to one or more users or roles
for a database or a table using the `REVOKE CREATE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Create.Effect
version: 1.0

[ClickHouse] SHALL remove the **create** privilege from the specified users or roles
after the successful execution of the `REVOKE CREATE` statement.
Any new operation by a user or a user that had the specified role
which requires the **create** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support revoking the **drop** privilege to one or more users or roles
for a database or a table using the `REVOKE DROP` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Drop.Effect
version: 1.0

[ClickHouse] SHALL remove the **drop** privilege from the specified users or roles
after the successful execution of the `REVOKE DROP` statement.
Any new operation by a user or a user that had the specified role
which requires the **drop** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support revoking the **truncate** privilege to one or more users or roles
for a database or a table using the `REVOKE TRUNCATE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Truncate.Effect
version: 1.0

[ClickHouse] SHALL remove the **truncate** privilege from the specified users or roles
after the successful execution of the `REVOKE TRUNCATE` statement.
Any new operation by a user or a user that had the specified role
which requires the **truncate** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support revoking the **optimize** privilege to one or more users or roles
for a database or a table using the `REVOKE OPTIMIZE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Optimize.Effect
version: 1.0

[ClickHouse] SHALL remove the **optimize** privilege from the specified users or roles
after the successful execution of the `REVOKE OPTMIZE` statement.
Any new operation by a user or a user that had the specified role
which requires the **optimize** privilege SHALL fail if user does not have it otherwise.



##### RQ.SRS-006.RBAC.Revoke.Privilege.Show
version: 1.0

[ClickHouse] SHALL support revoking the **show** privilege to one or more users or roles
for a database or a table using the `REVOKE SHOW` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Show.Effect
version: 1.0

[ClickHouse] SHALL remove the **show** privilege from the specified users or roles
after the successful execution of the `REVOKE SHOW` statement.
Any new operation by a user or a user that had the specified role
which requires the **show** privilege SHALL fail if user does not have it otherwise.



##### RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support revoking the **kill query** privilege to one or more users or roles
for a database or a table using the `REVOKE KILL QUERY` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery.Effect
version: 1.0

[ClickHouse] SHALL remove the **kill query** privilege from the specified users or roles
after the successful execution of the `REVOKE KILL QUERY` statement.
Any new operation by a user or a user that had the specified role
which requires the **kill query** privilege SHALL fail if user does not have it otherwise.


##### RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support revoking the **access management** privilege to one or more users or roles
for a database or a table using the `REVOKE ACCESS MANAGEMENT` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement.Effect
version: 1.0

[ClickHouse] SHALL remove the **access management** privilege from the specified users or roles
after the successful execution of the `REVOKE ACCESS MANAGEMENT` statement.
Any new operation by a user or a user that had the specified role
which requires the **access management** privilege SHALL fail if user does not have it otherwise.


##### RQ.SRS-006.RBAC.Revoke.Privilege.System
version: 1.0

[ClickHouse] SHALL support revoking the **system** privilege to one or more users or roles
for a database or a table using the `REVOKE SYSTEM` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.System.Effect
version: 1.0

[ClickHouse] SHALL remove the **system** privilege from the specified users or roles
after the successful execution of the `REVOKE SYSTEM` statement.
Any new operation by a user or a user that had the specified role
which requires the **system** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support revoking the **introspection** privilege to one or more users or roles
for a database or a table using the `REVOKE INTROSPECTION` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Introspection.Effect
version: 1.0

[ClickHouse] SHALL remove the **introspection** privilege from the specified users or roles
after the successful execution of the `REVOKE INTROSPECTION` statement.
Any new operation by a user or a user that had the specified role
which requires the **introspection** privilege SHALL fail if user does not have it otherwise.


##### RQ.SRS-006.RBAC.Revoke.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support revoking the **sources** privilege to one or more users or roles
for a database or a table using the `REVOKE SOURCES` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Sources.Effect
version: 1.0

[ClickHouse] SHALL remove the **sources** privilege from the specified users or roles
after the successful execution of the `REVOKE SOURCES` statement.
Any new operation by a user or a user that had the specified role
which requires the **sources** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support revoking the **dictGet** privilege to one or more users or roles
for a database or a table using the `REVOKE dictGet` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.DictGet.Effect
version: 1.0

[ClickHouse] SHALL remove the **dictGet** privilege from the specified users or roles
after the successful execution of the `REVOKE dictGet` statement.
Any new operation by a user or a user that had the specified role
which requires the **dictGet** privilege SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns
version: 1.0

[ClickHouse] SHALL support revoking the privilege **some_privilege** to one or more users or roles
for a database or a table using the `REVOKE some_privilege(column)` statement for one column.
Multiple columns will be supported with `REVOKE some_privilege(column1, column2...)` statement.
The privileges will be revoked for only the specified columns.

##### RQ.SRS-006.RBAC.Revoke.Privilege.PrivelegeColumns.Effect
version: 1.0

[ClickHouse] SHALL remove the privilege **some_privilege** from the specified users or roles
after the successful execution of the `REVOKE some_privilege(column)` statement for the specified column.
Removal of the privilege **some_privilege** over multiple columns SHALL happen after the successful
execution of the `REVOKE some_privilege(column1, column2...)` statement.
Any new operation by a user or a user that had the specified role
which requires the privilege **some_privilege** over specified SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Multiple
version: 1.0

[ClickHouse] SHALL support revoking MULTIPLE **privileges** to one or more users or roles
for a database or a table using the `REVOKE privilege1, privilege2...` statement.
**privileges** refers to any set of Clickhouse defined privilege, whose hierarchy includes
SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,
SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.

##### RQ.SRS-006.RBAC.Revoke.Privilege.Multiple.Effect
version: 1.0

[ClickHouse] SHALL remove the **privileges** from the specified users or roles
after the successful execution of the `REVOKE privilege1, privilege2...` statement.
Any new operation by a user or a user that had the specified role
which requires any of the **privileges** SHALL fail if user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Privilege.All
version: 1.0

[ClickHouse] SHALL support revoking **all** privileges to one or more users or roles
for a database or a table using the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statements.

##### RQ.SRS-006.RBAC.Revoke.Privilege.All.Effect
version: 1.0

[ClickHouse] SHALL remove **all** privileges from the specified users or roles
after the successful execution of the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statement.
Any new operation by a user or a user that had the specified role
which requires one or more privileges that are part of **all**
privileges SHALL fail.

##### RQ.SRS-006.RBAC.Revoke.Privilege.None
version: 1.0

[ClickHouse] SHALL support revoking **no** privileges to one or more users or roles
for a database or a table using the `REVOKE NONE` statement.

##### RQ.SRS-006.RBAC.Revoke.Privilege.None.Effect
version: 1.0

[ClickHouse] SHALL remove **no** privileges from the specified users or roles
after the successful execution of the `REVOKE NONE` statement.
Any new operation by a user or a user that had the specified role
shall have the same effect after this command as it did before this command.

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

##### RQ.SRS-006.RBAC.Revoke.Privilege.On.Effect
version: 1.0

[ClickHouse] SHALL remove the specificed priviliges from the specified one or more tables
indicated with the `ON` clause in the `REVOKE` privilege statement.
The tables will be indicated using the following patterns

* `db.table` specific table in the specified database
* `db.*` any table in the specified database
* `*.*` any table in any database
* `table` specific table in the current database
* `*` any table in the current database

Any new operation by a user or a user that had the specified role
which requires one or more privileges on the revoked tables SHALL fail.

##### RQ.SRS-006.RBAC.Revoke.Privilege.From
version: 1.0

[ClickHouse] SHALL support the `FROM` clause in the `REVOKE` privilege statement
which SHALL allow to specify one or more users to which the privilege SHALL
be revoked using the following patterns

* `{user | CURRENT_USER} [,...]` some combination of users by name, which may include the current user
* `ALL` all users
* `ALL EXCEPT {user | CURRENT_USER} [,...]` the logical reverse of the first pattern

##### RQ.SRS-006.RBAC.Revoke.Privilege.From.Effect
version: 1.0

[ClickHouse] SHALL remove **priviliges** to any set of users specified in the `FROM` clause
in the `REVOKE` privilege statement. The details of the removed **privileges** will be specified
in the other clauses. Any new operation by one of the specified users whose **privileges** have been
revoked SHALL fail. The patterns that expand the `FROM` clause are listed below

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
<!-- old syntax, for reference -->
<!-- ```sql
REVOKE [GRANT OPTION FOR]
    {USAGE | SELECT | SELECT(columns) | INSERT | DELETE | ALTER | CREATE | DROP | ALL [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    FROM user_or_role [, user_or_role ...]
``` -->

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

##### RQ.SRS-006.RBAC.PartialRevoke.Effect
version: 1.0

FIXME: Need to be defined.

##### RQ.SRS-006.RBAC.Grant.Role
version: 1.0

[ClickHouse] SHALL support granting one or more roles to
one or more users or roles using the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.Effect
version: 1.0

[ClickHouse] SHALL add all the privileges that are assigned to the role
which is granted to the user or the role to which `GRANT` role statement is applied.
Any new operation that requires the privileges included in the role
SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Role.CurrentUser
version: 1.0

[ClickHouse] SHALL support granting one or more roles to current user using
`TO CURRENT_USER` clause in the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.CurrentUser.Effect
version: 1.0

[ClickHouse] SHALL add all the privileges that are assigned to the role
which is granted to the current user via the `GRANT` statement. Any new operation that
requires the privileges included in the role SHALL succeed.

##### RQ.SRS-006.RBAC.Grant.Role.AdminOption
version: 1.0

[ClickHouse] SHALL support granting `admin option` privilege
to one or more users or roles using the `WITH ADMIN OPTION` clause
in the `GRANT` role statement.

##### RQ.SRS-006.RBAC.Grant.Role.AdminOption.Effect
version: 1.0

[ClickHouse] SHALL add the **admin option** privilege to the specified users or roles
after the successful execution of the `GRANT` role statement with the `WITH ADMIN OPTION` clause.
Any new **system queries** statements executed by a user or a user that has the specified role
which requires the **admin option** privilege SHALL succeed.

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

##### RQ.SRS-006.RBAC.Revoke.Role.Effect
version: 1.0

[ClickHouse] SHALL remove all the privileges that are assigned to the role
that is being revoked from the user or the role to which the `REVOKE` role statement is applied.
Any new operation, by the user or users that have the role which included the role being revoked,
that requires the privileges included in the role SHALL fail if the user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Role.Keywords
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
special groupings of one or more users or roles with the `ALL`, `ALL EXCEPT`,
and `CURRENT_USER` keywords.

##### RQ.SRS-006.RBAC.Revoke.Role.Keywords.Effect
version: 1.0

[ClickHouse] SHALL remove all the privileges that are assigned to the role
that is being revoked from the user or the role to which the `REVOKE` role statement with the specified keywords is applied.
Any new operation, by the user or users that have the role which included the role being revoked,
that requires the privileges included in the role SHALL fail if the user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.Role.Cluster
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
one or more users or roles from one or more clusters
using the `REVOKE ON CLUSTER` role statement.

##### RQ.SRS-006.RBAC.Revoke.Role.Cluster.Effect
version: 1.0

[ClickHouse] SHALL remove all the privileges that are assigned to the role
that is being revoked from the user or the role from the cluster(s)
to which the `REVOKE ON CLUSTER` role statement is applied.
Any new operation, by the user or users that have the role which included the role being revoked,
that requires the privileges included in the role SHALL fail if the user does not have it otherwise.

##### RQ.SRS-006.RBAC.Revoke.AdminOption
version: 1.0

[ClickHouse] SHALL support revoking `admin option` privilege
in one or more users or roles using the `ADMIN OPTION FOR` clause
in the `REVOKE` role statement.

##### RQ.SRS-006.RBAC.Revoke.AdminOption.Effect
version: 1.0

[ClickHouse] SHALL remove the **admin option** privilege from the specified users or roles
after the successful execution of the `REVOKE` role statement with the `ADMIN OPTION FOR` clause.
Any new **system queries** statements executed by a user or a user that has the specified role
which requires the **admin option** privilege SHALL fail.

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

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Effect
version: 1.0

[ClickHouse] SHALL use new profile after the `CREATE SETTINGS PROFILE` statement
is successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned.

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

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value.Effect
version: 1.0

[ClickHouse] SHALL use new variable values after `CREATE SETTINGS PROFILE` statement is
successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support setting `MIN`, `MAX`, `READONLY`, and `WRITABLE`
constraints for the variables in the `CREATE SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints.Effect
version: 1.0

[ClickHouse] SHALL use new variable constraints after `CREATE SETTINGS PROFILE` statement is
successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned.

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

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Effect
version: 1.0

[ClickHouse] SHALL use the updated settings profile after `ALTER SETTINGS PROFILE`
is successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned or SHALL raise an exception if the settings profile does not exist.

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

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value.Effect
version: 1.0

[ClickHouse] SHALL use the new value of the variable after `ALTER SETTINGS PROFILE`
is successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support altering `MIN`, `MAX`, `READONLY`, and `WRITABLE`
constraints for the variables in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints.Effect
version: 1.0

[ClickHouse] SHALL use new constraints after `ALTER SETTINGS PROFILE`
is successfully executed for any new operations performed by all the users and roles to which
the settings profile is assigned.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment
version: 1.0

[ClickHouse] SHALL support reassigning settings profile to one or more users
or roles using the `TO` clause in the `ALTER SETTINGS PROFILE` statement.

##### RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Effect
version: 1.0

[ClickHouse] SHALL unset all the variables and constraints that were defined in the settings profile
in all users and roles to which the settings profile was previously assigned.

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

##### RQ.SRS-006.RBAC.SettingsProfile.Drop.Effect
version: 1.0

[ClickHouse] SHALL unset all the variables and constraints that were defined in the settings profile
in all the users and roles to which the settings profile was assigned.

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

##### RQ.SRS-006.RBAC.Quota.Create.Effect
version: 1.0

[ClickHouse] SHALL use new limits specified by the quota after the `CREATE QUOTA` statement
is successfully executed for any new operations performed by all the users and roles to which
the quota is assigned.

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

##### RQ.SRS-006.RBAC.Quota.Alter.Effect
version: 1.0

[ClickHouse] SHALL use new limits specified by the updated quota after the `ALTER QUOTA` statement
is successfully executed for any new operations performed by all the users and roles to which
the quota is assigned.

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

##### RQ.SRS-006.RBAC.Quota.Drop.Effect
version: 1.0

[ClickHouse] SHALL unset all the limits that were defined in the quota
in all the users and roles to which the quota was assigned.

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

##### RQ.SRS-006.RBAC.RowPolicy.Create.Effect
version: 1.0

[ClickHouse] SHALL use the new row policy to control access to the specified table
after the `CREATE ROW POLICY` statement is successfully executed
for any new operations on the table performed by all the users and roles to which
the row policy is assigned.

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

##### RQ.SRS-006.RBAC.RowPolicy.Create.Condition.Effect
version: 1.0

[ClickHouse] SHALL check the condition specified in the row policy using the
`USING` clause in the `CREATE ROW POLICY` statement. The users or roles
to which the row policy is assigned SHALL only see data for which
the condition evaluates to the boolean value of `true`.

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

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Effect
version: 1.0

[ClickHouse] SHALL use the updated row policy to control access to the specified table
after the `ALTER ROW POLICY` statement is successfully executed
for any new operations on the table performed by all the users and roles to which
the row policy is assigned.

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

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.Effect
version: 1.0

[ClickHouse] SHALL check the new condition specified for the row policy using the
`USING` clause in the `ALTER ROW POLICY` statement. The users or roles
to which the row policy is assigned SHALL only see data for which
the new condition evaluates to the boolean value of `true`.

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

##### RQ.SRS-006.RBAC.RowPolicy.Drop.Effect
version: 1.0

[ClickHouse] SHALL remove checking the condition defined in the row policy
in all the users and roles to which the row policy was assigned.

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
