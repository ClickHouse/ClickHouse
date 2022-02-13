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
  * 5.2 [Login](#login)
    * 5.2.1 [RQ.SRS-006.RBAC.Login](#rqsrs-006rbaclogin)
    * 5.2.2 [RQ.SRS-006.RBAC.Login.DefaultUser](#rqsrs-006rbaclogindefaultuser)
  * 5.3 [User](#user)
    * 5.3.1 [RQ.SRS-006.RBAC.User](#rqsrs-006rbacuser)
    * 5.3.2 [RQ.SRS-006.RBAC.User.Roles](#rqsrs-006rbacuserroles)
    * 5.3.3 [RQ.SRS-006.RBAC.User.Privileges](#rqsrs-006rbacuserprivileges)
    * 5.3.4 [RQ.SRS-006.RBAC.User.Variables](#rqsrs-006rbacuservariables)
    * 5.3.5 [RQ.SRS-006.RBAC.User.Variables.Constraints](#rqsrs-006rbacuservariablesconstraints)
    * 5.3.6 [RQ.SRS-006.RBAC.User.SettingsProfile](#rqsrs-006rbacusersettingsprofile)
    * 5.3.7 [RQ.SRS-006.RBAC.User.Quotas](#rqsrs-006rbacuserquotas)
    * 5.3.8 [RQ.SRS-006.RBAC.User.RowPolicies](#rqsrs-006rbacuserrowpolicies)
    * 5.3.9 [RQ.SRS-006.RBAC.User.DefaultRole](#rqsrs-006rbacuserdefaultrole)
    * 5.3.10 [RQ.SRS-006.RBAC.User.RoleSelection](#rqsrs-006rbacuserroleselection)
    * 5.3.11 [RQ.SRS-006.RBAC.User.ShowCreate](#rqsrs-006rbacusershowcreate)
    * 5.3.12 [RQ.SRS-006.RBAC.User.ShowPrivileges](#rqsrs-006rbacusershowprivileges)
    * 5.3.13 [RQ.SRS-006.RBAC.User.Use.DefaultRole](#rqsrs-006rbacuserusedefaultrole)
    * 5.3.14 [RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole](#rqsrs-006rbacuseruseallroleswhennodefaultrole)
    * 5.3.15 [Create User](#create-user)
      * 5.3.15.1 [RQ.SRS-006.RBAC.User.Create](#rqsrs-006rbacusercreate)
      * 5.3.15.2 [RQ.SRS-006.RBAC.User.Create.IfNotExists](#rqsrs-006rbacusercreateifnotexists)
      * 5.3.15.3 [RQ.SRS-006.RBAC.User.Create.Replace](#rqsrs-006rbacusercreatereplace)
      * 5.3.15.4 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword](#rqsrs-006rbacusercreatepasswordnopassword)
      * 5.3.15.5 [RQ.SRS-006.RBAC.User.Create.Password.NoPassword.Login](#rqsrs-006rbacusercreatepasswordnopasswordlogin)
      * 5.3.15.6 [RQ.SRS-006.RBAC.User.Create.Password.PlainText](#rqsrs-006rbacusercreatepasswordplaintext)
      * 5.3.15.7 [RQ.SRS-006.RBAC.User.Create.Password.PlainText.Login](#rqsrs-006rbacusercreatepasswordplaintextlogin)
      * 5.3.15.8 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password](#rqsrs-006rbacusercreatepasswordsha256password)
      * 5.3.15.9 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Password.Login](#rqsrs-006rbacusercreatepasswordsha256passwordlogin)
      * 5.3.15.10 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash](#rqsrs-006rbacusercreatepasswordsha256hash)
      * 5.3.15.11 [RQ.SRS-006.RBAC.User.Create.Password.Sha256Hash.Login](#rqsrs-006rbacusercreatepasswordsha256hashlogin)
      * 5.3.15.12 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password](#rqsrs-006rbacusercreatepassworddoublesha1password)
      * 5.3.15.13 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Password.Login](#rqsrs-006rbacusercreatepassworddoublesha1passwordlogin)
      * 5.3.15.14 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash](#rqsrs-006rbacusercreatepassworddoublesha1hash)
      * 5.3.15.15 [RQ.SRS-006.RBAC.User.Create.Password.DoubleSha1Hash.Login](#rqsrs-006rbacusercreatepassworddoublesha1hashlogin)
      * 5.3.15.16 [RQ.SRS-006.RBAC.User.Create.Host.Name](#rqsrs-006rbacusercreatehostname)
      * 5.3.15.17 [RQ.SRS-006.RBAC.User.Create.Host.Regexp](#rqsrs-006rbacusercreatehostregexp)
      * 5.3.15.18 [RQ.SRS-006.RBAC.User.Create.Host.IP](#rqsrs-006rbacusercreatehostip)
      * 5.3.15.19 [RQ.SRS-006.RBAC.User.Create.Host.Any](#rqsrs-006rbacusercreatehostany)
      * 5.3.15.20 [RQ.SRS-006.RBAC.User.Create.Host.None](#rqsrs-006rbacusercreatehostnone)
      * 5.3.15.21 [RQ.SRS-006.RBAC.User.Create.Host.Local](#rqsrs-006rbacusercreatehostlocal)
      * 5.3.15.22 [RQ.SRS-006.RBAC.User.Create.Host.Like](#rqsrs-006rbacusercreatehostlike)
      * 5.3.15.23 [RQ.SRS-006.RBAC.User.Create.Host.Default](#rqsrs-006rbacusercreatehostdefault)
      * 5.3.15.24 [RQ.SRS-006.RBAC.User.Create.DefaultRole](#rqsrs-006rbacusercreatedefaultrole)
      * 5.3.15.25 [RQ.SRS-006.RBAC.User.Create.DefaultRole.None](#rqsrs-006rbacusercreatedefaultrolenone)
      * 5.3.15.26 [RQ.SRS-006.RBAC.User.Create.DefaultRole.All](#rqsrs-006rbacusercreatedefaultroleall)
      * 5.3.15.27 [RQ.SRS-006.RBAC.User.Create.Settings](#rqsrs-006rbacusercreatesettings)
      * 5.3.15.28 [RQ.SRS-006.RBAC.User.Create.OnCluster](#rqsrs-006rbacusercreateoncluster)
      * 5.3.15.29 [RQ.SRS-006.RBAC.User.Create.Syntax](#rqsrs-006rbacusercreatesyntax)
    * 5.3.16 [Alter User](#alter-user)
      * 5.3.16.1 [RQ.SRS-006.RBAC.User.Alter](#rqsrs-006rbacuseralter)
      * 5.3.16.2 [RQ.SRS-006.RBAC.User.Alter.OrderOfEvaluation](#rqsrs-006rbacuseralterorderofevaluation)
      * 5.3.16.3 [RQ.SRS-006.RBAC.User.Alter.IfExists](#rqsrs-006rbacuseralterifexists)
      * 5.3.16.4 [RQ.SRS-006.RBAC.User.Alter.Cluster](#rqsrs-006rbacuseraltercluster)
      * 5.3.16.5 [RQ.SRS-006.RBAC.User.Alter.Rename](#rqsrs-006rbacuseralterrename)
      * 5.3.16.6 [RQ.SRS-006.RBAC.User.Alter.Password.PlainText](#rqsrs-006rbacuseralterpasswordplaintext)
      * 5.3.16.7 [RQ.SRS-006.RBAC.User.Alter.Password.Sha256Password](#rqsrs-006rbacuseralterpasswordsha256password)
      * 5.3.16.8 [RQ.SRS-006.RBAC.User.Alter.Password.DoubleSha1Password](#rqsrs-006rbacuseralterpassworddoublesha1password)
      * 5.3.16.9 [RQ.SRS-006.RBAC.User.Alter.Host.AddDrop](#rqsrs-006rbacuseralterhostadddrop)
      * 5.3.16.10 [RQ.SRS-006.RBAC.User.Alter.Host.Local](#rqsrs-006rbacuseralterhostlocal)
      * 5.3.16.11 [RQ.SRS-006.RBAC.User.Alter.Host.Name](#rqsrs-006rbacuseralterhostname)
      * 5.3.16.12 [RQ.SRS-006.RBAC.User.Alter.Host.Regexp](#rqsrs-006rbacuseralterhostregexp)
      * 5.3.16.13 [RQ.SRS-006.RBAC.User.Alter.Host.IP](#rqsrs-006rbacuseralterhostip)
      * 5.3.16.14 [RQ.SRS-006.RBAC.User.Alter.Host.Like](#rqsrs-006rbacuseralterhostlike)
      * 5.3.16.15 [RQ.SRS-006.RBAC.User.Alter.Host.Any](#rqsrs-006rbacuseralterhostany)
      * 5.3.16.16 [RQ.SRS-006.RBAC.User.Alter.Host.None](#rqsrs-006rbacuseralterhostnone)
      * 5.3.16.17 [RQ.SRS-006.RBAC.User.Alter.DefaultRole](#rqsrs-006rbacuseralterdefaultrole)
      * 5.3.16.18 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.All](#rqsrs-006rbacuseralterdefaultroleall)
      * 5.3.16.19 [RQ.SRS-006.RBAC.User.Alter.DefaultRole.AllExcept](#rqsrs-006rbacuseralterdefaultroleallexcept)
      * 5.3.16.20 [RQ.SRS-006.RBAC.User.Alter.Settings](#rqsrs-006rbacuseraltersettings)
      * 5.3.16.21 [RQ.SRS-006.RBAC.User.Alter.Settings.Min](#rqsrs-006rbacuseraltersettingsmin)
      * 5.3.16.22 [RQ.SRS-006.RBAC.User.Alter.Settings.Max](#rqsrs-006rbacuseraltersettingsmax)
      * 5.3.16.23 [RQ.SRS-006.RBAC.User.Alter.Settings.Profile](#rqsrs-006rbacuseraltersettingsprofile)
      * 5.3.16.24 [RQ.SRS-006.RBAC.User.Alter.Syntax](#rqsrs-006rbacuseraltersyntax)
    * 5.3.17 [Show Create User](#show-create-user)
      * 5.3.17.1 [RQ.SRS-006.RBAC.User.ShowCreateUser](#rqsrs-006rbacusershowcreateuser)
      * 5.3.17.2 [RQ.SRS-006.RBAC.User.ShowCreateUser.For](#rqsrs-006rbacusershowcreateuserfor)
      * 5.3.17.3 [RQ.SRS-006.RBAC.User.ShowCreateUser.Syntax](#rqsrs-006rbacusershowcreateusersyntax)
    * 5.3.18 [Drop User](#drop-user)
      * 5.3.18.1 [RQ.SRS-006.RBAC.User.Drop](#rqsrs-006rbacuserdrop)
      * 5.3.18.2 [RQ.SRS-006.RBAC.User.Drop.IfExists](#rqsrs-006rbacuserdropifexists)
      * 5.3.18.3 [RQ.SRS-006.RBAC.User.Drop.OnCluster](#rqsrs-006rbacuserdroponcluster)
      * 5.3.18.4 [RQ.SRS-006.RBAC.User.Drop.Syntax](#rqsrs-006rbacuserdropsyntax)
  * 5.4 [Role](#role)
    * 5.4.1 [RQ.SRS-006.RBAC.Role](#rqsrs-006rbacrole)
    * 5.4.2 [RQ.SRS-006.RBAC.Role.Privileges](#rqsrs-006rbacroleprivileges)
    * 5.4.3 [RQ.SRS-006.RBAC.Role.Variables](#rqsrs-006rbacrolevariables)
    * 5.4.4 [RQ.SRS-006.RBAC.Role.SettingsProfile](#rqsrs-006rbacrolesettingsprofile)
    * 5.4.5 [RQ.SRS-006.RBAC.Role.Quotas](#rqsrs-006rbacrolequotas)
    * 5.4.6 [RQ.SRS-006.RBAC.Role.RowPolicies](#rqsrs-006rbacrolerowpolicies)
    * 5.4.7 [Create Role](#create-role)
      * 5.4.7.1 [RQ.SRS-006.RBAC.Role.Create](#rqsrs-006rbacrolecreate)
      * 5.4.7.2 [RQ.SRS-006.RBAC.Role.Create.IfNotExists](#rqsrs-006rbacrolecreateifnotexists)
      * 5.4.7.3 [RQ.SRS-006.RBAC.Role.Create.Replace](#rqsrs-006rbacrolecreatereplace)
      * 5.4.7.4 [RQ.SRS-006.RBAC.Role.Create.Settings](#rqsrs-006rbacrolecreatesettings)
      * 5.4.7.5 [RQ.SRS-006.RBAC.Role.Create.Syntax](#rqsrs-006rbacrolecreatesyntax)
    * 5.4.8 [Alter Role](#alter-role)
      * 5.4.8.1 [RQ.SRS-006.RBAC.Role.Alter](#rqsrs-006rbacrolealter)
      * 5.4.8.2 [RQ.SRS-006.RBAC.Role.Alter.IfExists](#rqsrs-006rbacrolealterifexists)
      * 5.4.8.3 [RQ.SRS-006.RBAC.Role.Alter.Cluster](#rqsrs-006rbacrolealtercluster)
      * 5.4.8.4 [RQ.SRS-006.RBAC.Role.Alter.Rename](#rqsrs-006rbacrolealterrename)
      * 5.4.8.5 [RQ.SRS-006.RBAC.Role.Alter.Settings](#rqsrs-006rbacrolealtersettings)
      * 5.4.8.6 [RQ.SRS-006.RBAC.Role.Alter.Syntax](#rqsrs-006rbacrolealtersyntax)
    * 5.4.9 [Drop Role](#drop-role)
      * 5.4.9.1 [RQ.SRS-006.RBAC.Role.Drop](#rqsrs-006rbacroledrop)
      * 5.4.9.2 [RQ.SRS-006.RBAC.Role.Drop.IfExists](#rqsrs-006rbacroledropifexists)
      * 5.4.9.3 [RQ.SRS-006.RBAC.Role.Drop.Cluster](#rqsrs-006rbacroledropcluster)
      * 5.4.9.4 [RQ.SRS-006.RBAC.Role.Drop.Syntax](#rqsrs-006rbacroledropsyntax)
    * 5.4.10 [Show Create Role](#show-create-role)
      * 5.4.10.1 [RQ.SRS-006.RBAC.Role.ShowCreate](#rqsrs-006rbacroleshowcreate)
      * 5.4.10.2 [RQ.SRS-006.RBAC.Role.ShowCreate.Syntax](#rqsrs-006rbacroleshowcreatesyntax)
  * 5.5 [Partial Revokes](#partial-revokes)
    * 5.5.1 [RQ.SRS-006.RBAC.PartialRevokes](#rqsrs-006rbacpartialrevokes)
    * 5.5.2 [RQ.SRS-006.RBAC.PartialRevoke.Syntax](#rqsrs-006rbacpartialrevokesyntax)
  * 5.6 [Settings Profile](#settings-profile)
    * 5.6.1 [RQ.SRS-006.RBAC.SettingsProfile](#rqsrs-006rbacsettingsprofile)
    * 5.6.2 [RQ.SRS-006.RBAC.SettingsProfile.Constraints](#rqsrs-006rbacsettingsprofileconstraints)
    * 5.6.3 [Create Settings Profile](#create-settings-profile)
      * 5.6.3.1 [RQ.SRS-006.RBAC.SettingsProfile.Create](#rqsrs-006rbacsettingsprofilecreate)
      * 5.6.3.2 [RQ.SRS-006.RBAC.SettingsProfile.Create.IfNotExists](#rqsrs-006rbacsettingsprofilecreateifnotexists)
      * 5.6.3.3 [RQ.SRS-006.RBAC.SettingsProfile.Create.Replace](#rqsrs-006rbacsettingsprofilecreatereplace)
      * 5.6.3.4 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables](#rqsrs-006rbacsettingsprofilecreatevariables)
      * 5.6.3.5 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Value](#rqsrs-006rbacsettingsprofilecreatevariablesvalue)
      * 5.6.3.6 [RQ.SRS-006.RBAC.SettingsProfile.Create.Variables.Constraints](#rqsrs-006rbacsettingsprofilecreatevariablesconstraints)
      * 5.6.3.7 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment](#rqsrs-006rbacsettingsprofilecreateassignment)
      * 5.6.3.8 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.None](#rqsrs-006rbacsettingsprofilecreateassignmentnone)
      * 5.6.3.9 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.All](#rqsrs-006rbacsettingsprofilecreateassignmentall)
      * 5.6.3.10 [RQ.SRS-006.RBAC.SettingsProfile.Create.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilecreateassignmentallexcept)
      * 5.6.3.11 [RQ.SRS-006.RBAC.SettingsProfile.Create.Inherit](#rqsrs-006rbacsettingsprofilecreateinherit)
      * 5.6.3.12 [RQ.SRS-006.RBAC.SettingsProfile.Create.OnCluster](#rqsrs-006rbacsettingsprofilecreateoncluster)
      * 5.6.3.13 [RQ.SRS-006.RBAC.SettingsProfile.Create.Syntax](#rqsrs-006rbacsettingsprofilecreatesyntax)
    * 5.6.4 [Alter Settings Profile](#alter-settings-profile)
      * 5.6.4.1 [RQ.SRS-006.RBAC.SettingsProfile.Alter](#rqsrs-006rbacsettingsprofilealter)
      * 5.6.4.2 [RQ.SRS-006.RBAC.SettingsProfile.Alter.IfExists](#rqsrs-006rbacsettingsprofilealterifexists)
      * 5.6.4.3 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Rename](#rqsrs-006rbacsettingsprofilealterrename)
      * 5.6.4.4 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables](#rqsrs-006rbacsettingsprofilealtervariables)
      * 5.6.4.5 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Value](#rqsrs-006rbacsettingsprofilealtervariablesvalue)
      * 5.6.4.6 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Variables.Constraints](#rqsrs-006rbacsettingsprofilealtervariablesconstraints)
      * 5.6.4.7 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment](#rqsrs-006rbacsettingsprofilealterassignment)
      * 5.6.4.8 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.None](#rqsrs-006rbacsettingsprofilealterassignmentnone)
      * 5.6.4.9 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.All](#rqsrs-006rbacsettingsprofilealterassignmentall)
      * 5.6.4.10 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.AllExcept](#rqsrs-006rbacsettingsprofilealterassignmentallexcept)
      * 5.6.4.11 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.Inherit](#rqsrs-006rbacsettingsprofilealterassignmentinherit)
      * 5.6.4.12 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Assignment.OnCluster](#rqsrs-006rbacsettingsprofilealterassignmentoncluster)
      * 5.6.4.13 [RQ.SRS-006.RBAC.SettingsProfile.Alter.Syntax](#rqsrs-006rbacsettingsprofilealtersyntax)
    * 5.6.5 [Drop Settings Profile](#drop-settings-profile)
      * 5.6.5.1 [RQ.SRS-006.RBAC.SettingsProfile.Drop](#rqsrs-006rbacsettingsprofiledrop)
      * 5.6.5.2 [RQ.SRS-006.RBAC.SettingsProfile.Drop.IfExists](#rqsrs-006rbacsettingsprofiledropifexists)
      * 5.6.5.3 [RQ.SRS-006.RBAC.SettingsProfile.Drop.OnCluster](#rqsrs-006rbacsettingsprofiledroponcluster)
      * 5.6.5.4 [RQ.SRS-006.RBAC.SettingsProfile.Drop.Syntax](#rqsrs-006rbacsettingsprofiledropsyntax)
    * 5.6.6 [Show Create Settings Profile](#show-create-settings-profile)
      * 5.6.6.1 [RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile](#rqsrs-006rbacsettingsprofileshowcreatesettingsprofile)
  * 5.7 [Quotas](#quotas)
    * 5.7.1 [RQ.SRS-006.RBAC.Quotas](#rqsrs-006rbacquotas)
    * 5.7.2 [RQ.SRS-006.RBAC.Quotas.Keyed](#rqsrs-006rbacquotaskeyed)
    * 5.7.3 [RQ.SRS-006.RBAC.Quotas.Queries](#rqsrs-006rbacquotasqueries)
    * 5.7.4 [RQ.SRS-006.RBAC.Quotas.Errors](#rqsrs-006rbacquotaserrors)
    * 5.7.5 [RQ.SRS-006.RBAC.Quotas.ResultRows](#rqsrs-006rbacquotasresultrows)
    * 5.7.6 [RQ.SRS-006.RBAC.Quotas.ReadRows](#rqsrs-006rbacquotasreadrows)
    * 5.7.7 [RQ.SRS-006.RBAC.Quotas.ResultBytes](#rqsrs-006rbacquotasresultbytes)
    * 5.7.8 [RQ.SRS-006.RBAC.Quotas.ReadBytes](#rqsrs-006rbacquotasreadbytes)
    * 5.7.9 [RQ.SRS-006.RBAC.Quotas.ExecutionTime](#rqsrs-006rbacquotasexecutiontime)
    * 5.7.10 [Create Quotas](#create-quotas)
      * 5.7.10.1 [RQ.SRS-006.RBAC.Quota.Create](#rqsrs-006rbacquotacreate)
      * 5.7.10.2 [RQ.SRS-006.RBAC.Quota.Create.IfNotExists](#rqsrs-006rbacquotacreateifnotexists)
      * 5.7.10.3 [RQ.SRS-006.RBAC.Quota.Create.Replace](#rqsrs-006rbacquotacreatereplace)
      * 5.7.10.4 [RQ.SRS-006.RBAC.Quota.Create.Cluster](#rqsrs-006rbacquotacreatecluster)
      * 5.7.10.5 [RQ.SRS-006.RBAC.Quota.Create.Interval](#rqsrs-006rbacquotacreateinterval)
      * 5.7.10.6 [RQ.SRS-006.RBAC.Quota.Create.Interval.Randomized](#rqsrs-006rbacquotacreateintervalrandomized)
      * 5.7.10.7 [RQ.SRS-006.RBAC.Quota.Create.Queries](#rqsrs-006rbacquotacreatequeries)
      * 5.7.10.8 [RQ.SRS-006.RBAC.Quota.Create.Errors](#rqsrs-006rbacquotacreateerrors)
      * 5.7.10.9 [RQ.SRS-006.RBAC.Quota.Create.ResultRows](#rqsrs-006rbacquotacreateresultrows)
      * 5.7.10.10 [RQ.SRS-006.RBAC.Quota.Create.ReadRows](#rqsrs-006rbacquotacreatereadrows)
      * 5.7.10.11 [RQ.SRS-006.RBAC.Quota.Create.ResultBytes](#rqsrs-006rbacquotacreateresultbytes)
      * 5.7.10.12 [RQ.SRS-006.RBAC.Quota.Create.ReadBytes](#rqsrs-006rbacquotacreatereadbytes)
      * 5.7.10.13 [RQ.SRS-006.RBAC.Quota.Create.ExecutionTime](#rqsrs-006rbacquotacreateexecutiontime)
      * 5.7.10.14 [RQ.SRS-006.RBAC.Quota.Create.NoLimits](#rqsrs-006rbacquotacreatenolimits)
      * 5.7.10.15 [RQ.SRS-006.RBAC.Quota.Create.TrackingOnly](#rqsrs-006rbacquotacreatetrackingonly)
      * 5.7.10.16 [RQ.SRS-006.RBAC.Quota.Create.KeyedBy](#rqsrs-006rbacquotacreatekeyedby)
      * 5.7.10.17 [RQ.SRS-006.RBAC.Quota.Create.KeyedByOptions](#rqsrs-006rbacquotacreatekeyedbyoptions)
      * 5.7.10.18 [RQ.SRS-006.RBAC.Quota.Create.Assignment](#rqsrs-006rbacquotacreateassignment)
      * 5.7.10.19 [RQ.SRS-006.RBAC.Quota.Create.Assignment.None](#rqsrs-006rbacquotacreateassignmentnone)
      * 5.7.10.20 [RQ.SRS-006.RBAC.Quota.Create.Assignment.All](#rqsrs-006rbacquotacreateassignmentall)
      * 5.7.10.21 [RQ.SRS-006.RBAC.Quota.Create.Assignment.Except](#rqsrs-006rbacquotacreateassignmentexcept)
      * 5.7.10.22 [RQ.SRS-006.RBAC.Quota.Create.Syntax](#rqsrs-006rbacquotacreatesyntax)
    * 5.7.11 [Alter Quota](#alter-quota)
      * 5.7.11.1 [RQ.SRS-006.RBAC.Quota.Alter](#rqsrs-006rbacquotaalter)
      * 5.7.11.2 [RQ.SRS-006.RBAC.Quota.Alter.IfExists](#rqsrs-006rbacquotaalterifexists)
      * 5.7.11.3 [RQ.SRS-006.RBAC.Quota.Alter.Rename](#rqsrs-006rbacquotaalterrename)
      * 5.7.11.4 [RQ.SRS-006.RBAC.Quota.Alter.Cluster](#rqsrs-006rbacquotaaltercluster)
      * 5.7.11.5 [RQ.SRS-006.RBAC.Quota.Alter.Interval](#rqsrs-006rbacquotaalterinterval)
      * 5.7.11.6 [RQ.SRS-006.RBAC.Quota.Alter.Interval.Randomized](#rqsrs-006rbacquotaalterintervalrandomized)
      * 5.7.11.7 [RQ.SRS-006.RBAC.Quota.Alter.Queries](#rqsrs-006rbacquotaalterqueries)
      * 5.7.11.8 [RQ.SRS-006.RBAC.Quota.Alter.Errors](#rqsrs-006rbacquotaaltererrors)
      * 5.7.11.9 [RQ.SRS-006.RBAC.Quota.Alter.ResultRows](#rqsrs-006rbacquotaalterresultrows)
      * 5.7.11.10 [RQ.SRS-006.RBAC.Quota.Alter.ReadRows](#rqsrs-006rbacquotaalterreadrows)
      * 5.7.11.11 [RQ.SRS-006.RBAC.Quota.ALter.ResultBytes](#rqsrs-006rbacquotaalterresultbytes)
      * 5.7.11.12 [RQ.SRS-006.RBAC.Quota.Alter.ReadBytes](#rqsrs-006rbacquotaalterreadbytes)
      * 5.7.11.13 [RQ.SRS-006.RBAC.Quota.Alter.ExecutionTime](#rqsrs-006rbacquotaalterexecutiontime)
      * 5.7.11.14 [RQ.SRS-006.RBAC.Quota.Alter.NoLimits](#rqsrs-006rbacquotaalternolimits)
      * 5.7.11.15 [RQ.SRS-006.RBAC.Quota.Alter.TrackingOnly](#rqsrs-006rbacquotaaltertrackingonly)
      * 5.7.11.16 [RQ.SRS-006.RBAC.Quota.Alter.KeyedBy](#rqsrs-006rbacquotaalterkeyedby)
      * 5.7.11.17 [RQ.SRS-006.RBAC.Quota.Alter.KeyedByOptions](#rqsrs-006rbacquotaalterkeyedbyoptions)
      * 5.7.11.18 [RQ.SRS-006.RBAC.Quota.Alter.Assignment](#rqsrs-006rbacquotaalterassignment)
      * 5.7.11.19 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.None](#rqsrs-006rbacquotaalterassignmentnone)
      * 5.7.11.20 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.All](#rqsrs-006rbacquotaalterassignmentall)
      * 5.7.11.21 [RQ.SRS-006.RBAC.Quota.Alter.Assignment.Except](#rqsrs-006rbacquotaalterassignmentexcept)
      * 5.7.11.22 [RQ.SRS-006.RBAC.Quota.Alter.Syntax](#rqsrs-006rbacquotaaltersyntax)
    * 5.7.12 [Drop Quota](#drop-quota)
      * 5.7.12.1 [RQ.SRS-006.RBAC.Quota.Drop](#rqsrs-006rbacquotadrop)
      * 5.7.12.2 [RQ.SRS-006.RBAC.Quota.Drop.IfExists](#rqsrs-006rbacquotadropifexists)
      * 5.7.12.3 [RQ.SRS-006.RBAC.Quota.Drop.Cluster](#rqsrs-006rbacquotadropcluster)
      * 5.7.12.4 [RQ.SRS-006.RBAC.Quota.Drop.Syntax](#rqsrs-006rbacquotadropsyntax)
    * 5.7.13 [Show Quotas](#show-quotas)
      * 5.7.13.1 [RQ.SRS-006.RBAC.Quota.ShowQuotas](#rqsrs-006rbacquotashowquotas)
      * 5.7.13.2 [RQ.SRS-006.RBAC.Quota.ShowQuotas.IntoOutfile](#rqsrs-006rbacquotashowquotasintooutfile)
      * 5.7.13.3 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Format](#rqsrs-006rbacquotashowquotasformat)
      * 5.7.13.4 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Settings](#rqsrs-006rbacquotashowquotassettings)
      * 5.7.13.5 [RQ.SRS-006.RBAC.Quota.ShowQuotas.Syntax](#rqsrs-006rbacquotashowquotassyntax)
    * 5.7.14 [Show Create Quota](#show-create-quota)
      * 5.7.14.1 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Name](#rqsrs-006rbacquotashowcreatequotaname)
      * 5.7.14.2 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Current](#rqsrs-006rbacquotashowcreatequotacurrent)
      * 5.7.14.3 [RQ.SRS-006.RBAC.Quota.ShowCreateQuota.Syntax](#rqsrs-006rbacquotashowcreatequotasyntax)
  * 5.8 [Row Policy](#row-policy)
    * 5.8.1 [RQ.SRS-006.RBAC.RowPolicy](#rqsrs-006rbacrowpolicy)
    * 5.8.2 [RQ.SRS-006.RBAC.RowPolicy.Condition](#rqsrs-006rbacrowpolicycondition)
    * 5.8.3 [RQ.SRS-006.RBAC.RowPolicy.Restriction](#rqsrs-006rbacrowpolicyrestriction)
    * 5.8.4 [RQ.SRS-006.RBAC.RowPolicy.Nesting](#rqsrs-006rbacrowpolicynesting)
    * 5.8.5 [Create Row Policy](#create-row-policy)
      * 5.8.5.1 [RQ.SRS-006.RBAC.RowPolicy.Create](#rqsrs-006rbacrowpolicycreate)
      * 5.8.5.2 [RQ.SRS-006.RBAC.RowPolicy.Create.IfNotExists](#rqsrs-006rbacrowpolicycreateifnotexists)
      * 5.8.5.3 [RQ.SRS-006.RBAC.RowPolicy.Create.Replace](#rqsrs-006rbacrowpolicycreatereplace)
      * 5.8.5.4 [RQ.SRS-006.RBAC.RowPolicy.Create.OnCluster](#rqsrs-006rbacrowpolicycreateoncluster)
      * 5.8.5.5 [RQ.SRS-006.RBAC.RowPolicy.Create.On](#rqsrs-006rbacrowpolicycreateon)
      * 5.8.5.6 [RQ.SRS-006.RBAC.RowPolicy.Create.Access](#rqsrs-006rbacrowpolicycreateaccess)
      * 5.8.5.7 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Permissive](#rqsrs-006rbacrowpolicycreateaccesspermissive)
      * 5.8.5.8 [RQ.SRS-006.RBAC.RowPolicy.Create.Access.Restrictive](#rqsrs-006rbacrowpolicycreateaccessrestrictive)
      * 5.8.5.9 [RQ.SRS-006.RBAC.RowPolicy.Create.ForSelect](#rqsrs-006rbacrowpolicycreateforselect)
      * 5.8.5.10 [RQ.SRS-006.RBAC.RowPolicy.Create.Condition](#rqsrs-006rbacrowpolicycreatecondition)
      * 5.8.5.11 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment](#rqsrs-006rbacrowpolicycreateassignment)
      * 5.8.5.12 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.None](#rqsrs-006rbacrowpolicycreateassignmentnone)
      * 5.8.5.13 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.All](#rqsrs-006rbacrowpolicycreateassignmentall)
      * 5.8.5.14 [RQ.SRS-006.RBAC.RowPolicy.Create.Assignment.AllExcept](#rqsrs-006rbacrowpolicycreateassignmentallexcept)
      * 5.8.5.15 [RQ.SRS-006.RBAC.RowPolicy.Create.Syntax](#rqsrs-006rbacrowpolicycreatesyntax)
    * 5.8.6 [Alter Row Policy](#alter-row-policy)
      * 5.8.6.1 [RQ.SRS-006.RBAC.RowPolicy.Alter](#rqsrs-006rbacrowpolicyalter)
      * 5.8.6.2 [RQ.SRS-006.RBAC.RowPolicy.Alter.IfExists](#rqsrs-006rbacrowpolicyalterifexists)
      * 5.8.6.3 [RQ.SRS-006.RBAC.RowPolicy.Alter.ForSelect](#rqsrs-006rbacrowpolicyalterforselect)
      * 5.8.6.4 [RQ.SRS-006.RBAC.RowPolicy.Alter.OnCluster](#rqsrs-006rbacrowpolicyalteroncluster)
      * 5.8.6.5 [RQ.SRS-006.RBAC.RowPolicy.Alter.On](#rqsrs-006rbacrowpolicyalteron)
      * 5.8.6.6 [RQ.SRS-006.RBAC.RowPolicy.Alter.Rename](#rqsrs-006rbacrowpolicyalterrename)
      * 5.8.6.7 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access](#rqsrs-006rbacrowpolicyalteraccess)
      * 5.8.6.8 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive](#rqsrs-006rbacrowpolicyalteraccesspermissive)
      * 5.8.6.9 [RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive](#rqsrs-006rbacrowpolicyalteraccessrestrictive)
      * 5.8.6.10 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition](#rqsrs-006rbacrowpolicyaltercondition)
      * 5.8.6.11 [RQ.SRS-006.RBAC.RowPolicy.Alter.Condition.None](#rqsrs-006rbacrowpolicyalterconditionnone)
      * 5.8.6.12 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment](#rqsrs-006rbacrowpolicyalterassignment)
      * 5.8.6.13 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.None](#rqsrs-006rbacrowpolicyalterassignmentnone)
      * 5.8.6.14 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.All](#rqsrs-006rbacrowpolicyalterassignmentall)
      * 5.8.6.15 [RQ.SRS-006.RBAC.RowPolicy.Alter.Assignment.AllExcept](#rqsrs-006rbacrowpolicyalterassignmentallexcept)
      * 5.8.6.16 [RQ.SRS-006.RBAC.RowPolicy.Alter.Syntax](#rqsrs-006rbacrowpolicyaltersyntax)
    * 5.8.7 [Drop Row Policy](#drop-row-policy)
      * 5.8.7.1 [RQ.SRS-006.RBAC.RowPolicy.Drop](#rqsrs-006rbacrowpolicydrop)
      * 5.8.7.2 [RQ.SRS-006.RBAC.RowPolicy.Drop.IfExists](#rqsrs-006rbacrowpolicydropifexists)
      * 5.8.7.3 [RQ.SRS-006.RBAC.RowPolicy.Drop.On](#rqsrs-006rbacrowpolicydropon)
      * 5.8.7.4 [RQ.SRS-006.RBAC.RowPolicy.Drop.OnCluster](#rqsrs-006rbacrowpolicydroponcluster)
      * 5.8.7.5 [RQ.SRS-006.RBAC.RowPolicy.Drop.Syntax](#rqsrs-006rbacrowpolicydropsyntax)
    * 5.8.8 [Show Create Row Policy](#show-create-row-policy)
      * 5.8.8.1 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy](#rqsrs-006rbacrowpolicyshowcreaterowpolicy)
      * 5.8.8.2 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.On](#rqsrs-006rbacrowpolicyshowcreaterowpolicyon)
      * 5.8.8.3 [RQ.SRS-006.RBAC.RowPolicy.ShowCreateRowPolicy.Syntax](#rqsrs-006rbacrowpolicyshowcreaterowpolicysyntax)
      * 5.8.8.4 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies](#rqsrs-006rbacrowpolicyshowrowpolicies)
      * 5.8.8.5 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.On](#rqsrs-006rbacrowpolicyshowrowpolicieson)
      * 5.8.8.6 [RQ.SRS-006.RBAC.RowPolicy.ShowRowPolicies.Syntax](#rqsrs-006rbacrowpolicyshowrowpoliciessyntax)
  * 5.9 [Set Default Role](#set-default-role)
    * 5.9.1 [RQ.SRS-006.RBAC.SetDefaultRole](#rqsrs-006rbacsetdefaultrole)
    * 5.9.2 [RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser](#rqsrs-006rbacsetdefaultrolecurrentuser)
    * 5.9.3 [RQ.SRS-006.RBAC.SetDefaultRole.All](#rqsrs-006rbacsetdefaultroleall)
    * 5.9.4 [RQ.SRS-006.RBAC.SetDefaultRole.AllExcept](#rqsrs-006rbacsetdefaultroleallexcept)
    * 5.9.5 [RQ.SRS-006.RBAC.SetDefaultRole.None](#rqsrs-006rbacsetdefaultrolenone)
    * 5.9.6 [RQ.SRS-006.RBAC.SetDefaultRole.Syntax](#rqsrs-006rbacsetdefaultrolesyntax)
  * 5.10 [Set Role](#set-role)
    * 5.10.1 [RQ.SRS-006.RBAC.SetRole](#rqsrs-006rbacsetrole)
    * 5.10.2 [RQ.SRS-006.RBAC.SetRole.Default](#rqsrs-006rbacsetroledefault)
    * 5.10.3 [RQ.SRS-006.RBAC.SetRole.None](#rqsrs-006rbacsetrolenone)
    * 5.10.4 [RQ.SRS-006.RBAC.SetRole.All](#rqsrs-006rbacsetroleall)
    * 5.10.5 [RQ.SRS-006.RBAC.SetRole.AllExcept](#rqsrs-006rbacsetroleallexcept)
    * 5.10.6 [RQ.SRS-006.RBAC.SetRole.Syntax](#rqsrs-006rbacsetrolesyntax)
  * 5.11 [Grant](#grant)
    * 5.11.1 [RQ.SRS-006.RBAC.Grant.Privilege.To](#rqsrs-006rbacgrantprivilegeto)
    * 5.11.2 [RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser](#rqsrs-006rbacgrantprivilegetocurrentuser)
    * 5.11.3 [RQ.SRS-006.RBAC.Grant.Privilege.Select](#rqsrs-006rbacgrantprivilegeselect)
    * 5.11.4 [RQ.SRS-006.RBAC.Grant.Privilege.Insert](#rqsrs-006rbacgrantprivilegeinsert)
    * 5.11.5 [RQ.SRS-006.RBAC.Grant.Privilege.Alter](#rqsrs-006rbacgrantprivilegealter)
    * 5.11.6 [RQ.SRS-006.RBAC.Grant.Privilege.Create](#rqsrs-006rbacgrantprivilegecreate)
    * 5.11.7 [RQ.SRS-006.RBAC.Grant.Privilege.Drop](#rqsrs-006rbacgrantprivilegedrop)
    * 5.11.8 [RQ.SRS-006.RBAC.Grant.Privilege.Truncate](#rqsrs-006rbacgrantprivilegetruncate)
    * 5.11.9 [RQ.SRS-006.RBAC.Grant.Privilege.Optimize](#rqsrs-006rbacgrantprivilegeoptimize)
    * 5.11.10 [RQ.SRS-006.RBAC.Grant.Privilege.Show](#rqsrs-006rbacgrantprivilegeshow)
    * 5.11.11 [RQ.SRS-006.RBAC.Grant.Privilege.KillQuery](#rqsrs-006rbacgrantprivilegekillquery)
    * 5.11.12 [RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement](#rqsrs-006rbacgrantprivilegeaccessmanagement)
    * 5.11.13 [RQ.SRS-006.RBAC.Grant.Privilege.System](#rqsrs-006rbacgrantprivilegesystem)
    * 5.11.14 [RQ.SRS-006.RBAC.Grant.Privilege.Introspection](#rqsrs-006rbacgrantprivilegeintrospection)
    * 5.11.15 [RQ.SRS-006.RBAC.Grant.Privilege.Sources](#rqsrs-006rbacgrantprivilegesources)
    * 5.11.16 [RQ.SRS-006.RBAC.Grant.Privilege.DictGet](#rqsrs-006rbacgrantprivilegedictget)
    * 5.11.17 [RQ.SRS-006.RBAC.Grant.Privilege.None](#rqsrs-006rbacgrantprivilegenone)
    * 5.11.18 [RQ.SRS-006.RBAC.Grant.Privilege.All](#rqsrs-006rbacgrantprivilegeall)
    * 5.11.19 [RQ.SRS-006.RBAC.Grant.Privilege.GrantOption](#rqsrs-006rbacgrantprivilegegrantoption)
    * 5.11.20 [RQ.SRS-006.RBAC.Grant.Privilege.On](#rqsrs-006rbacgrantprivilegeon)
    * 5.11.21 [RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns](#rqsrs-006rbacgrantprivilegeprivilegecolumns)
    * 5.11.22 [RQ.SRS-006.RBAC.Grant.Privilege.OnCluster](#rqsrs-006rbacgrantprivilegeoncluster)
    * 5.11.23 [RQ.SRS-006.RBAC.Grant.Privilege.Syntax](#rqsrs-006rbacgrantprivilegesyntax)
  * 5.12 [Revoke](#revoke)
    * 5.12.1 [RQ.SRS-006.RBAC.Revoke.Privilege.Cluster](#rqsrs-006rbacrevokeprivilegecluster)
    * 5.12.2 [RQ.SRS-006.RBAC.Revoke.Privilege.Select](#rqsrs-006rbacrevokeprivilegeselect)
    * 5.12.3 [RQ.SRS-006.RBAC.Revoke.Privilege.Insert](#rqsrs-006rbacrevokeprivilegeinsert)
    * 5.12.4 [RQ.SRS-006.RBAC.Revoke.Privilege.Alter](#rqsrs-006rbacrevokeprivilegealter)
    * 5.12.5 [RQ.SRS-006.RBAC.Revoke.Privilege.Create](#rqsrs-006rbacrevokeprivilegecreate)
    * 5.12.6 [RQ.SRS-006.RBAC.Revoke.Privilege.Drop](#rqsrs-006rbacrevokeprivilegedrop)
    * 5.12.7 [RQ.SRS-006.RBAC.Revoke.Privilege.Truncate](#rqsrs-006rbacrevokeprivilegetruncate)
    * 5.12.8 [RQ.SRS-006.RBAC.Revoke.Privilege.Optimize](#rqsrs-006rbacrevokeprivilegeoptimize)
    * 5.12.9 [RQ.SRS-006.RBAC.Revoke.Privilege.Show](#rqsrs-006rbacrevokeprivilegeshow)
    * 5.12.10 [RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery](#rqsrs-006rbacrevokeprivilegekillquery)
    * 5.12.11 [RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement](#rqsrs-006rbacrevokeprivilegeaccessmanagement)
    * 5.12.12 [RQ.SRS-006.RBAC.Revoke.Privilege.System](#rqsrs-006rbacrevokeprivilegesystem)
    * 5.12.13 [RQ.SRS-006.RBAC.Revoke.Privilege.Introspection](#rqsrs-006rbacrevokeprivilegeintrospection)
    * 5.12.14 [RQ.SRS-006.RBAC.Revoke.Privilege.Sources](#rqsrs-006rbacrevokeprivilegesources)
    * 5.12.15 [RQ.SRS-006.RBAC.Revoke.Privilege.DictGet](#rqsrs-006rbacrevokeprivilegedictget)
    * 5.12.16 [RQ.SRS-006.RBAC.Revoke.Privilege.PrivilegeColumns](#rqsrs-006rbacrevokeprivilegeprivilegecolumns)
    * 5.12.17 [RQ.SRS-006.RBAC.Revoke.Privilege.Multiple](#rqsrs-006rbacrevokeprivilegemultiple)
    * 5.12.18 [RQ.SRS-006.RBAC.Revoke.Privilege.All](#rqsrs-006rbacrevokeprivilegeall)
    * 5.12.19 [RQ.SRS-006.RBAC.Revoke.Privilege.None](#rqsrs-006rbacrevokeprivilegenone)
    * 5.12.20 [RQ.SRS-006.RBAC.Revoke.Privilege.On](#rqsrs-006rbacrevokeprivilegeon)
    * 5.12.21 [RQ.SRS-006.RBAC.Revoke.Privilege.From](#rqsrs-006rbacrevokeprivilegefrom)
    * 5.12.22 [RQ.SRS-006.RBAC.Revoke.Privilege.Syntax](#rqsrs-006rbacrevokeprivilegesyntax)
  * 5.13 [Grant Role](#grant-role)
    * 5.13.1 [RQ.SRS-006.RBAC.Grant.Role](#rqsrs-006rbacgrantrole)
    * 5.13.2 [RQ.SRS-006.RBAC.Grant.Role.CurrentUser](#rqsrs-006rbacgrantrolecurrentuser)
    * 5.13.3 [RQ.SRS-006.RBAC.Grant.Role.AdminOption](#rqsrs-006rbacgrantroleadminoption)
    * 5.13.4 [RQ.SRS-006.RBAC.Grant.Role.OnCluster](#rqsrs-006rbacgrantroleoncluster)
    * 5.13.5 [RQ.SRS-006.RBAC.Grant.Role.Syntax](#rqsrs-006rbacgrantrolesyntax)
  * 5.14 [Revoke Role](#revoke-role)
    * 5.14.1 [RQ.SRS-006.RBAC.Revoke.Role](#rqsrs-006rbacrevokerole)
    * 5.14.2 [RQ.SRS-006.RBAC.Revoke.Role.Keywords](#rqsrs-006rbacrevokerolekeywords)
    * 5.14.3 [RQ.SRS-006.RBAC.Revoke.Role.Cluster](#rqsrs-006rbacrevokerolecluster)
    * 5.14.4 [RQ.SRS-006.RBAC.Revoke.AdminOption](#rqsrs-006rbacrevokeadminoption)
    * 5.14.5 [RQ.SRS-006.RBAC.Revoke.Role.Syntax](#rqsrs-006rbacrevokerolesyntax)
  * 5.15 [Show Grants](#show-grants)
    * 5.15.1 [RQ.SRS-006.RBAC.Show.Grants](#rqsrs-006rbacshowgrants)
    * 5.15.2 [RQ.SRS-006.RBAC.Show.Grants.For](#rqsrs-006rbacshowgrantsfor)
    * 5.15.3 [RQ.SRS-006.RBAC.Show.Grants.Syntax](#rqsrs-006rbacshowgrantssyntax)
  * 5.16 [Table Privileges](#table-privileges)
    * 5.16.1 [RQ.SRS-006.RBAC.Table.PublicTables](#rqsrs-006rbactablepublictables)
    * 5.16.2 [RQ.SRS-006.RBAC.Table.SensitiveTables](#rqsrs-006rbactablesensitivetables)
  * 5.17 [Distributed Tables](#distributed-tables)
    * 5.17.1 [RQ.SRS-006.RBAC.DistributedTable.Create](#rqsrs-006rbacdistributedtablecreate)
    * 5.17.2 [RQ.SRS-006.RBAC.DistributedTable.Select](#rqsrs-006rbacdistributedtableselect)
    * 5.17.3 [RQ.SRS-006.RBAC.DistributedTable.Insert](#rqsrs-006rbacdistributedtableinsert)
    * 5.17.4 [RQ.SRS-006.RBAC.DistributedTable.SpecialTables](#rqsrs-006rbacdistributedtablespecialtables)
    * 5.17.5 [RQ.SRS-006.RBAC.DistributedTable.LocalUser](#rqsrs-006rbacdistributedtablelocaluser)
    * 5.17.6 [RQ.SRS-006.RBAC.DistributedTable.SameUserDifferentNodesDifferentPrivileges](#rqsrs-006rbacdistributedtablesameuserdifferentnodesdifferentprivileges)
  * 5.18 [Views](#views)
    * 5.18.1 [View](#view)
      * 5.18.1.1 [RQ.SRS-006.RBAC.View](#rqsrs-006rbacview)
      * 5.18.1.2 [RQ.SRS-006.RBAC.View.Create](#rqsrs-006rbacviewcreate)
      * 5.18.1.3 [RQ.SRS-006.RBAC.View.Select](#rqsrs-006rbacviewselect)
      * 5.18.1.4 [RQ.SRS-006.RBAC.View.Drop](#rqsrs-006rbacviewdrop)
    * 5.18.2 [Materialized View](#materialized-view)
      * 5.18.2.1 [RQ.SRS-006.RBAC.MaterializedView](#rqsrs-006rbacmaterializedview)
      * 5.18.2.2 [RQ.SRS-006.RBAC.MaterializedView.Create](#rqsrs-006rbacmaterializedviewcreate)
      * 5.18.2.3 [RQ.SRS-006.RBAC.MaterializedView.Select](#rqsrs-006rbacmaterializedviewselect)
      * 5.18.2.4 [RQ.SRS-006.RBAC.MaterializedView.Select.TargetTable](#rqsrs-006rbacmaterializedviewselecttargettable)
      * 5.18.2.5 [RQ.SRS-006.RBAC.MaterializedView.Select.SourceTable](#rqsrs-006rbacmaterializedviewselectsourcetable)
      * 5.18.2.6 [RQ.SRS-006.RBAC.MaterializedView.Drop](#rqsrs-006rbacmaterializedviewdrop)
      * 5.18.2.7 [RQ.SRS-006.RBAC.MaterializedView.ModifyQuery](#rqsrs-006rbacmaterializedviewmodifyquery)
      * 5.18.2.8 [RQ.SRS-006.RBAC.MaterializedView.Insert](#rqsrs-006rbacmaterializedviewinsert)
      * 5.18.2.9 [RQ.SRS-006.RBAC.MaterializedView.Insert.SourceTable](#rqsrs-006rbacmaterializedviewinsertsourcetable)
      * 5.18.2.10 [RQ.SRS-006.RBAC.MaterializedView.Insert.TargetTable](#rqsrs-006rbacmaterializedviewinserttargettable)
    * 5.18.3 [Live View](#live-view)
      * 5.18.3.1 [RQ.SRS-006.RBAC.LiveView](#rqsrs-006rbacliveview)
      * 5.18.3.2 [RQ.SRS-006.RBAC.LiveView.Create](#rqsrs-006rbacliveviewcreate)
      * 5.18.3.3 [RQ.SRS-006.RBAC.LiveView.Select](#rqsrs-006rbacliveviewselect)
      * 5.18.3.4 [RQ.SRS-006.RBAC.LiveView.Drop](#rqsrs-006rbacliveviewdrop)
      * 5.18.3.5 [RQ.SRS-006.RBAC.LiveView.Refresh](#rqsrs-006rbacliveviewrefresh)
  * 5.19 [Select](#select)
    * 5.19.1 [RQ.SRS-006.RBAC.Select](#rqsrs-006rbacselect)
    * 5.19.2 [RQ.SRS-006.RBAC.Select.Column](#rqsrs-006rbacselectcolumn)
    * 5.19.3 [RQ.SRS-006.RBAC.Select.Cluster](#rqsrs-006rbacselectcluster)
    * 5.19.4 [RQ.SRS-006.RBAC.Select.TableEngines](#rqsrs-006rbacselecttableengines)
  * 5.20 [Insert](#insert)
    * 5.20.1 [RQ.SRS-006.RBAC.Insert](#rqsrs-006rbacinsert)
    * 5.20.2 [RQ.SRS-006.RBAC.Insert.Column](#rqsrs-006rbacinsertcolumn)
    * 5.20.3 [RQ.SRS-006.RBAC.Insert.Cluster](#rqsrs-006rbacinsertcluster)
    * 5.20.4 [RQ.SRS-006.RBAC.Insert.TableEngines](#rqsrs-006rbacinserttableengines)
  * 5.21 [Alter](#alter)
    * 5.21.1 [Alter Column](#alter-column)
      * 5.21.1.1 [RQ.SRS-006.RBAC.Privileges.AlterColumn](#rqsrs-006rbacprivilegesaltercolumn)
      * 5.21.1.2 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Grant](#rqsrs-006rbacprivilegesaltercolumngrant)
      * 5.21.1.3 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Revoke](#rqsrs-006rbacprivilegesaltercolumnrevoke)
      * 5.21.1.4 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Column](#rqsrs-006rbacprivilegesaltercolumncolumn)
      * 5.21.1.5 [RQ.SRS-006.RBAC.Privileges.AlterColumn.Cluster](#rqsrs-006rbacprivilegesaltercolumncluster)
      * 5.21.1.6 [RQ.SRS-006.RBAC.Privileges.AlterColumn.TableEngines](#rqsrs-006rbacprivilegesaltercolumntableengines)
    * 5.21.2 [Alter Index](#alter-index)
      * 5.21.2.1 [RQ.SRS-006.RBAC.Privileges.AlterIndex](#rqsrs-006rbacprivilegesalterindex)
      * 5.21.2.2 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Grant](#rqsrs-006rbacprivilegesalterindexgrant)
      * 5.21.2.3 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Revoke](#rqsrs-006rbacprivilegesalterindexrevoke)
      * 5.21.2.4 [RQ.SRS-006.RBAC.Privileges.AlterIndex.Cluster](#rqsrs-006rbacprivilegesalterindexcluster)
      * 5.21.2.5 [RQ.SRS-006.RBAC.Privileges.AlterIndex.TableEngines](#rqsrs-006rbacprivilegesalterindextableengines)
    * 5.21.3 [Alter Constraint](#alter-constraint)
      * 5.21.3.1 [RQ.SRS-006.RBAC.Privileges.AlterConstraint](#rqsrs-006rbacprivilegesalterconstraint)
      * 5.21.3.2 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Grant](#rqsrs-006rbacprivilegesalterconstraintgrant)
      * 5.21.3.3 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Revoke](#rqsrs-006rbacprivilegesalterconstraintrevoke)
      * 5.21.3.4 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.Cluster](#rqsrs-006rbacprivilegesalterconstraintcluster)
      * 5.21.3.5 [RQ.SRS-006.RBAC.Privileges.AlterConstraint.TableEngines](#rqsrs-006rbacprivilegesalterconstrainttableengines)
    * 5.21.4 [Alter TTL](#alter-ttl)
      * 5.21.4.1 [RQ.SRS-006.RBAC.Privileges.AlterTTL](#rqsrs-006rbacprivilegesalterttl)
      * 5.21.4.2 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Grant](#rqsrs-006rbacprivilegesalterttlgrant)
      * 5.21.4.3 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Revoke](#rqsrs-006rbacprivilegesalterttlrevoke)
      * 5.21.4.4 [RQ.SRS-006.RBAC.Privileges.AlterTTL.Cluster](#rqsrs-006rbacprivilegesalterttlcluster)
      * 5.21.4.5 [RQ.SRS-006.RBAC.Privileges.AlterTTL.TableEngines](#rqsrs-006rbacprivilegesalterttltableengines)
    * 5.21.5 [Alter Settings](#alter-settings)
      * 5.21.5.1 [RQ.SRS-006.RBAC.Privileges.AlterSettings](#rqsrs-006rbacprivilegesaltersettings)
      * 5.21.5.2 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Grant](#rqsrs-006rbacprivilegesaltersettingsgrant)
      * 5.21.5.3 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Revoke](#rqsrs-006rbacprivilegesaltersettingsrevoke)
      * 5.21.5.4 [RQ.SRS-006.RBAC.Privileges.AlterSettings.Cluster](#rqsrs-006rbacprivilegesaltersettingscluster)
      * 5.21.5.5 [RQ.SRS-006.RBAC.Privileges.AlterSettings.TableEngines](#rqsrs-006rbacprivilegesaltersettingstableengines)
    * 5.21.6 [Alter Update](#alter-update)
      * 5.21.6.1 [RQ.SRS-006.RBAC.Privileges.AlterUpdate](#rqsrs-006rbacprivilegesalterupdate)
      * 5.21.6.2 [RQ.SRS-006.RBAC.Privileges.AlterUpdate.Grant](#rqsrs-006rbacprivilegesalterupdategrant)
      * 5.21.6.3 [RQ.SRS-006.RBAC.Privileges.AlterUpdate.Revoke](#rqsrs-006rbacprivilegesalterupdaterevoke)
      * 5.21.6.4 [RQ.SRS-006.RBAC.Privileges.AlterUpdate.TableEngines](#rqsrs-006rbacprivilegesalterupdatetableengines)
    * 5.21.7 [Alter Delete](#alter-delete)
      * 5.21.7.1 [RQ.SRS-006.RBAC.Privileges.AlterDelete](#rqsrs-006rbacprivilegesalterdelete)
      * 5.21.7.2 [RQ.SRS-006.RBAC.Privileges.AlterDelete.Grant](#rqsrs-006rbacprivilegesalterdeletegrant)
      * 5.21.7.3 [RQ.SRS-006.RBAC.Privileges.AlterDelete.Revoke](#rqsrs-006rbacprivilegesalterdeleterevoke)
      * 5.21.7.4 [RQ.SRS-006.RBAC.Privileges.AlterDelete.TableEngines](#rqsrs-006rbacprivilegesalterdeletetableengines)
    * 5.21.8 [Alter Freeze Partition](#alter-freeze-partition)
      * 5.21.8.1 [RQ.SRS-006.RBAC.Privileges.AlterFreeze](#rqsrs-006rbacprivilegesalterfreeze)
      * 5.21.8.2 [RQ.SRS-006.RBAC.Privileges.AlterFreeze.Grant](#rqsrs-006rbacprivilegesalterfreezegrant)
      * 5.21.8.3 [RQ.SRS-006.RBAC.Privileges.AlterFreeze.Revoke](#rqsrs-006rbacprivilegesalterfreezerevoke)
      * 5.21.8.4 [RQ.SRS-006.RBAC.Privileges.AlterFreeze.TableEngines](#rqsrs-006rbacprivilegesalterfreezetableengines)
    * 5.21.9 [Alter Fetch Partition](#alter-fetch-partition)
      * 5.21.9.1 [RQ.SRS-006.RBAC.Privileges.AlterFetch](#rqsrs-006rbacprivilegesalterfetch)
      * 5.21.9.2 [RQ.SRS-006.RBAC.Privileges.AlterFetch.Grant](#rqsrs-006rbacprivilegesalterfetchgrant)
      * 5.21.9.3 [RQ.SRS-006.RBAC.Privileges.AlterFetch.Revoke](#rqsrs-006rbacprivilegesalterfetchrevoke)
      * 5.21.9.4 [RQ.SRS-006.RBAC.Privileges.AlterFetch.TableEngines](#rqsrs-006rbacprivilegesalterfetchtableengines)
    * 5.21.10 [Alter Move Partition](#alter-move-partition)
      * 5.21.10.1 [RQ.SRS-006.RBAC.Privileges.AlterMove](#rqsrs-006rbacprivilegesaltermove)
      * 5.21.10.2 [RQ.SRS-006.RBAC.Privileges.AlterMove.Grant](#rqsrs-006rbacprivilegesaltermovegrant)
      * 5.21.10.3 [RQ.SRS-006.RBAC.Privileges.AlterMove.Revoke](#rqsrs-006rbacprivilegesaltermoverevoke)
      * 5.21.10.4 [RQ.SRS-006.RBAC.Privileges.AlterMove.TableEngines](#rqsrs-006rbacprivilegesaltermovetableengines)
  * 5.22 [Create](#create)
    * 5.22.1 [RQ.SRS-006.RBAC.Privileges.CreateTable](#rqsrs-006rbacprivilegescreatetable)
    * 5.22.2 [RQ.SRS-006.RBAC.Privileges.CreateDatabase](#rqsrs-006rbacprivilegescreatedatabase)
    * 5.22.3 [RQ.SRS-006.RBAC.Privileges.CreateDictionary](#rqsrs-006rbacprivilegescreatedictionary)
    * 5.22.4 [RQ.SRS-006.RBAC.Privileges.CreateTemporaryTable](#rqsrs-006rbacprivilegescreatetemporarytable)
  * 5.23 [Attach](#attach)
    * 5.23.1 [RQ.SRS-006.RBAC.Privileges.AttachDatabase](#rqsrs-006rbacprivilegesattachdatabase)
    * 5.23.2 [RQ.SRS-006.RBAC.Privileges.AttachDictionary](#rqsrs-006rbacprivilegesattachdictionary)
    * 5.23.3 [RQ.SRS-006.RBAC.Privileges.AttachTemporaryTable](#rqsrs-006rbacprivilegesattachtemporarytable)
    * 5.23.4 [RQ.SRS-006.RBAC.Privileges.AttachTable](#rqsrs-006rbacprivilegesattachtable)
  * 5.24 [Drop](#drop)
    * 5.24.1 [RQ.SRS-006.RBAC.Privileges.DropTable](#rqsrs-006rbacprivilegesdroptable)
    * 5.24.2 [RQ.SRS-006.RBAC.Privileges.DropDatabase](#rqsrs-006rbacprivilegesdropdatabase)
    * 5.24.3 [RQ.SRS-006.RBAC.Privileges.DropDictionary](#rqsrs-006rbacprivilegesdropdictionary)
  * 5.25 [Detach](#detach)
    * 5.25.1 [RQ.SRS-006.RBAC.Privileges.DetachTable](#rqsrs-006rbacprivilegesdetachtable)
    * 5.25.2 [RQ.SRS-006.RBAC.Privileges.DetachView](#rqsrs-006rbacprivilegesdetachview)
    * 5.25.3 [RQ.SRS-006.RBAC.Privileges.DetachDatabase](#rqsrs-006rbacprivilegesdetachdatabase)
    * 5.25.4 [RQ.SRS-006.RBAC.Privileges.DetachDictionary](#rqsrs-006rbacprivilegesdetachdictionary)
  * 5.26 [Truncate](#truncate)
    * 5.26.1 [RQ.SRS-006.RBAC.Privileges.Truncate](#rqsrs-006rbacprivilegestruncate)
  * 5.27 [Optimize](#optimize)
    * 5.27.1 [RQ.SRS-006.RBAC.Privileges.Optimize](#rqsrs-006rbacprivilegesoptimize)
  * 5.28 [Kill Query](#kill-query)
    * 5.28.1 [RQ.SRS-006.RBAC.Privileges.KillQuery](#rqsrs-006rbacprivilegeskillquery)
  * 5.29 [Kill Mutation](#kill-mutation)
    * 5.29.1 [RQ.SRS-006.RBAC.Privileges.KillMutation](#rqsrs-006rbacprivilegeskillmutation)
    * 5.29.2 [RQ.SRS-006.RBAC.Privileges.KillMutation.AlterUpdate](#rqsrs-006rbacprivilegeskillmutationalterupdate)
    * 5.29.3 [RQ.SRS-006.RBAC.Privileges.KillMutation.AlterDelete](#rqsrs-006rbacprivilegeskillmutationalterdelete)
    * 5.29.4 [RQ.SRS-006.RBAC.Privileges.KillMutation.AlterDropColumn](#rqsrs-006rbacprivilegeskillmutationalterdropcolumn)
  * 5.30 [Show](#show)
    * 5.30.1 [RQ.SRS-006.RBAC.ShowTables.Privilege](#rqsrs-006rbacshowtablesprivilege)
    * 5.30.2 [RQ.SRS-006.RBAC.ShowTables.RequiredPrivilege](#rqsrs-006rbacshowtablesrequiredprivilege)
    * 5.30.3 [RQ.SRS-006.RBAC.ExistsTable.RequiredPrivilege](#rqsrs-006rbacexiststablerequiredprivilege)
    * 5.30.4 [RQ.SRS-006.RBAC.CheckTable.RequiredPrivilege](#rqsrs-006rbacchecktablerequiredprivilege)
    * 5.30.5 [RQ.SRS-006.RBAC.ShowDatabases.Privilege](#rqsrs-006rbacshowdatabasesprivilege)
    * 5.30.6 [RQ.SRS-006.RBAC.ShowDatabases.RequiredPrivilege](#rqsrs-006rbacshowdatabasesrequiredprivilege)
    * 5.30.7 [RQ.SRS-006.RBAC.ShowCreateDatabase.RequiredPrivilege](#rqsrs-006rbacshowcreatedatabaserequiredprivilege)
    * 5.30.8 [RQ.SRS-006.RBAC.UseDatabase.RequiredPrivilege](#rqsrs-006rbacusedatabaserequiredprivilege)
    * 5.30.9 [RQ.SRS-006.RBAC.ShowColumns.Privilege](#rqsrs-006rbacshowcolumnsprivilege)
    * 5.30.10 [RQ.SRS-006.RBAC.ShowCreateTable.RequiredPrivilege](#rqsrs-006rbacshowcreatetablerequiredprivilege)
    * 5.30.11 [RQ.SRS-006.RBAC.DescribeTable.RequiredPrivilege](#rqsrs-006rbacdescribetablerequiredprivilege)
    * 5.30.12 [RQ.SRS-006.RBAC.ShowDictionaries.Privilege](#rqsrs-006rbacshowdictionariesprivilege)
    * 5.30.13 [RQ.SRS-006.RBAC.ShowDictionaries.RequiredPrivilege](#rqsrs-006rbacshowdictionariesrequiredprivilege)
    * 5.30.14 [RQ.SRS-006.RBAC.ShowCreateDictionary.RequiredPrivilege](#rqsrs-006rbacshowcreatedictionaryrequiredprivilege)
    * 5.30.15 [RQ.SRS-006.RBAC.ExistsDictionary.RequiredPrivilege](#rqsrs-006rbacexistsdictionaryrequiredprivilege)
  * 5.31 [Access Management](#access-management)
    * 5.31.1 [RQ.SRS-006.RBAC.Privileges.CreateUser](#rqsrs-006rbacprivilegescreateuser)
    * 5.31.2 [RQ.SRS-006.RBAC.Privileges.CreateUser.DefaultRole](#rqsrs-006rbacprivilegescreateuserdefaultrole)
    * 5.31.3 [RQ.SRS-006.RBAC.Privileges.AlterUser](#rqsrs-006rbacprivilegesalteruser)
    * 5.31.4 [RQ.SRS-006.RBAC.Privileges.DropUser](#rqsrs-006rbacprivilegesdropuser)
    * 5.31.5 [RQ.SRS-006.RBAC.Privileges.CreateRole](#rqsrs-006rbacprivilegescreaterole)
    * 5.31.6 [RQ.SRS-006.RBAC.Privileges.AlterRole](#rqsrs-006rbacprivilegesalterrole)
    * 5.31.7 [RQ.SRS-006.RBAC.Privileges.DropRole](#rqsrs-006rbacprivilegesdroprole)
    * 5.31.8 [RQ.SRS-006.RBAC.Privileges.CreateRowPolicy](#rqsrs-006rbacprivilegescreaterowpolicy)
    * 5.31.9 [RQ.SRS-006.RBAC.Privileges.AlterRowPolicy](#rqsrs-006rbacprivilegesalterrowpolicy)
    * 5.31.10 [RQ.SRS-006.RBAC.Privileges.DropRowPolicy](#rqsrs-006rbacprivilegesdroprowpolicy)
    * 5.31.11 [RQ.SRS-006.RBAC.Privileges.CreateQuota](#rqsrs-006rbacprivilegescreatequota)
    * 5.31.12 [RQ.SRS-006.RBAC.Privileges.AlterQuota](#rqsrs-006rbacprivilegesalterquota)
    * 5.31.13 [RQ.SRS-006.RBAC.Privileges.DropQuota](#rqsrs-006rbacprivilegesdropquota)
    * 5.31.14 [RQ.SRS-006.RBAC.Privileges.CreateSettingsProfile](#rqsrs-006rbacprivilegescreatesettingsprofile)
    * 5.31.15 [RQ.SRS-006.RBAC.Privileges.AlterSettingsProfile](#rqsrs-006rbacprivilegesaltersettingsprofile)
    * 5.31.16 [RQ.SRS-006.RBAC.Privileges.DropSettingsProfile](#rqsrs-006rbacprivilegesdropsettingsprofile)
    * 5.31.17 [RQ.SRS-006.RBAC.Privileges.RoleAdmin](#rqsrs-006rbacprivilegesroleadmin)
    * 5.31.18 [Show Access](#show-access)
      * 5.31.18.1 [RQ.SRS-006.RBAC.ShowUsers.Privilege](#rqsrs-006rbacshowusersprivilege)
      * 5.31.18.2 [RQ.SRS-006.RBAC.ShowUsers.RequiredPrivilege](#rqsrs-006rbacshowusersrequiredprivilege)
      * 5.31.18.3 [RQ.SRS-006.RBAC.ShowCreateUser.RequiredPrivilege](#rqsrs-006rbacshowcreateuserrequiredprivilege)
      * 5.31.18.4 [RQ.SRS-006.RBAC.ShowRoles.Privilege](#rqsrs-006rbacshowrolesprivilege)
      * 5.31.18.5 [RQ.SRS-006.RBAC.ShowRoles.RequiredPrivilege](#rqsrs-006rbacshowrolesrequiredprivilege)
      * 5.31.18.6 [RQ.SRS-006.RBAC.ShowCreateRole.RequiredPrivilege](#rqsrs-006rbacshowcreaterolerequiredprivilege)
      * 5.31.18.7 [RQ.SRS-006.RBAC.ShowRowPolicies.Privilege](#rqsrs-006rbacshowrowpoliciesprivilege)
      * 5.31.18.8 [RQ.SRS-006.RBAC.ShowRowPolicies.RequiredPrivilege](#rqsrs-006rbacshowrowpoliciesrequiredprivilege)
      * 5.31.18.9 [RQ.SRS-006.RBAC.ShowCreateRowPolicy.RequiredPrivilege](#rqsrs-006rbacshowcreaterowpolicyrequiredprivilege)
      * 5.31.18.10 [RQ.SRS-006.RBAC.ShowQuotas.Privilege](#rqsrs-006rbacshowquotasprivilege)
      * 5.31.18.11 [RQ.SRS-006.RBAC.ShowQuotas.RequiredPrivilege](#rqsrs-006rbacshowquotasrequiredprivilege)
      * 5.31.18.12 [RQ.SRS-006.RBAC.ShowCreateQuota.RequiredPrivilege](#rqsrs-006rbacshowcreatequotarequiredprivilege)
      * 5.31.18.13 [RQ.SRS-006.RBAC.ShowSettingsProfiles.Privilege](#rqsrs-006rbacshowsettingsprofilesprivilege)
      * 5.31.18.14 [RQ.SRS-006.RBAC.ShowSettingsProfiles.RequiredPrivilege](#rqsrs-006rbacshowsettingsprofilesrequiredprivilege)
      * 5.31.18.15 [RQ.SRS-006.RBAC.ShowCreateSettingsProfile.RequiredPrivilege](#rqsrs-006rbacshowcreatesettingsprofilerequiredprivilege)
  * 5.32 [dictGet](#dictget)
    * 5.32.1 [RQ.SRS-006.RBAC.dictGet.Privilege](#rqsrs-006rbacdictgetprivilege)
    * 5.32.2 [RQ.SRS-006.RBAC.dictGet.RequiredPrivilege](#rqsrs-006rbacdictgetrequiredprivilege)
    * 5.32.3 [RQ.SRS-006.RBAC.dictGet.Type.RequiredPrivilege](#rqsrs-006rbacdictgettyperequiredprivilege)
    * 5.32.4 [RQ.SRS-006.RBAC.dictGet.OrDefault.RequiredPrivilege](#rqsrs-006rbacdictgetordefaultrequiredprivilege)
    * 5.32.5 [RQ.SRS-006.RBAC.dictHas.RequiredPrivilege](#rqsrs-006rbacdicthasrequiredprivilege)
    * 5.32.6 [RQ.SRS-006.RBAC.dictGetHierarchy.RequiredPrivilege](#rqsrs-006rbacdictgethierarchyrequiredprivilege)
    * 5.32.7 [RQ.SRS-006.RBAC.dictIsIn.RequiredPrivilege](#rqsrs-006rbacdictisinrequiredprivilege)
  * 5.33 [Introspection](#introspection)
    * 5.33.1 [RQ.SRS-006.RBAC.Privileges.Introspection](#rqsrs-006rbacprivilegesintrospection)
    * 5.33.2 [RQ.SRS-006.RBAC.Privileges.Introspection.addressToLine](#rqsrs-006rbacprivilegesintrospectionaddresstoline)
    * 5.33.3 [RQ.SRS-006.RBAC.Privileges.Introspection.addressToSymbol](#rqsrs-006rbacprivilegesintrospectionaddresstosymbol)
    * 5.33.4 [RQ.SRS-006.RBAC.Privileges.Introspection.demangle](#rqsrs-006rbacprivilegesintrospectiondemangle)
  * 5.34 [System](#system)
    * 5.34.1 [RQ.SRS-006.RBAC.Privileges.System.Shutdown](#rqsrs-006rbacprivilegessystemshutdown)
    * 5.34.2 [RQ.SRS-006.RBAC.Privileges.System.DropCache](#rqsrs-006rbacprivilegessystemdropcache)
    * 5.34.3 [RQ.SRS-006.RBAC.Privileges.System.DropCache.DNS](#rqsrs-006rbacprivilegessystemdropcachedns)
    * 5.34.4 [RQ.SRS-006.RBAC.Privileges.System.DropCache.Mark](#rqsrs-006rbacprivilegessystemdropcachemark)
    * 5.34.5 [RQ.SRS-006.RBAC.Privileges.System.DropCache.Uncompressed](#rqsrs-006rbacprivilegessystemdropcacheuncompressed)
    * 5.34.6 [RQ.SRS-006.RBAC.Privileges.System.Reload](#rqsrs-006rbacprivilegessystemreload)
    * 5.34.7 [RQ.SRS-006.RBAC.Privileges.System.Reload.Config](#rqsrs-006rbacprivilegessystemreloadconfig)
    * 5.34.8 [RQ.SRS-006.RBAC.Privileges.System.Reload.Dictionary](#rqsrs-006rbacprivilegessystemreloaddictionary)
    * 5.34.9 [RQ.SRS-006.RBAC.Privileges.System.Reload.Dictionaries](#rqsrs-006rbacprivilegessystemreloaddictionaries)
    * 5.34.10 [RQ.SRS-006.RBAC.Privileges.System.Reload.EmbeddedDictionaries](#rqsrs-006rbacprivilegessystemreloadembeddeddictionaries)
    * 5.34.11 [RQ.SRS-006.RBAC.Privileges.System.Merges](#rqsrs-006rbacprivilegessystemmerges)
    * 5.34.12 [RQ.SRS-006.RBAC.Privileges.System.TTLMerges](#rqsrs-006rbacprivilegessystemttlmerges)
    * 5.34.13 [RQ.SRS-006.RBAC.Privileges.System.Fetches](#rqsrs-006rbacprivilegessystemfetches)
    * 5.34.14 [RQ.SRS-006.RBAC.Privileges.System.Moves](#rqsrs-006rbacprivilegessystemmoves)
    * 5.34.15 [RQ.SRS-006.RBAC.Privileges.System.Sends](#rqsrs-006rbacprivilegessystemsends)
    * 5.34.16 [RQ.SRS-006.RBAC.Privileges.System.Sends.Distributed](#rqsrs-006rbacprivilegessystemsendsdistributed)
    * 5.34.17 [RQ.SRS-006.RBAC.Privileges.System.Sends.Replicated](#rqsrs-006rbacprivilegessystemsendsreplicated)
    * 5.34.18 [RQ.SRS-006.RBAC.Privileges.System.ReplicationQueues](#rqsrs-006rbacprivilegessystemreplicationqueues)
    * 5.34.19 [RQ.SRS-006.RBAC.Privileges.System.SyncReplica](#rqsrs-006rbacprivilegessystemsyncreplica)
    * 5.34.20 [RQ.SRS-006.RBAC.Privileges.System.RestartReplica](#rqsrs-006rbacprivilegessystemrestartreplica)
    * 5.34.21 [RQ.SRS-006.RBAC.Privileges.System.Flush](#rqsrs-006rbacprivilegessystemflush)
    * 5.34.22 [RQ.SRS-006.RBAC.Privileges.System.Flush.Distributed](#rqsrs-006rbacprivilegessystemflushdistributed)
    * 5.34.23 [RQ.SRS-006.RBAC.Privileges.System.Flush.Logs](#rqsrs-006rbacprivilegessystemflushlogs)
  * 5.35 [Sources](#sources)
    * 5.35.1 [RQ.SRS-006.RBAC.Privileges.Sources](#rqsrs-006rbacprivilegessources)
    * 5.35.2 [RQ.SRS-006.RBAC.Privileges.Sources.File](#rqsrs-006rbacprivilegessourcesfile)
    * 5.35.3 [RQ.SRS-006.RBAC.Privileges.Sources.URL](#rqsrs-006rbacprivilegessourcesurl)
    * 5.35.4 [RQ.SRS-006.RBAC.Privileges.Sources.Remote](#rqsrs-006rbacprivilegessourcesremote)
    * 5.35.5 [RQ.SRS-006.RBAC.Privileges.Sources.MySQL](#rqsrs-006rbacprivilegessourcesmysql)
    * 5.35.6 [RQ.SRS-006.RBAC.Privileges.Sources.ODBC](#rqsrs-006rbacprivilegessourcesodbc)
    * 5.35.7 [RQ.SRS-006.RBAC.Privileges.Sources.JDBC](#rqsrs-006rbacprivilegessourcesjdbc)
    * 5.35.8 [RQ.SRS-006.RBAC.Privileges.Sources.HDFS](#rqsrs-006rbacprivilegessourceshdfs)
    * 5.35.9 [RQ.SRS-006.RBAC.Privileges.Sources.S3](#rqsrs-006rbacprivilegessourcess3)
  * 5.36 [RQ.SRS-006.RBAC.Privileges.GrantOption](#rqsrs-006rbacprivilegesgrantoption)
  * 5.37 [RQ.SRS-006.RBAC.Privileges.All](#rqsrs-006rbacprivilegesall)
  * 5.38 [RQ.SRS-006.RBAC.Privileges.RoleAll](#rqsrs-006rbacprivilegesroleall)
  * 5.39 [RQ.SRS-006.RBAC.Privileges.None](#rqsrs-006rbacprivilegesnone)
  * 5.40 [RQ.SRS-006.RBAC.Privileges.AdminOption](#rqsrs-006rbacprivilegesadminoption)
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

### Login

#### RQ.SRS-006.RBAC.Login
version: 1.0

[ClickHouse] SHALL only allow access to the server for a given
user only when correct username and password are used during
the connection to the server.

#### RQ.SRS-006.RBAC.Login.DefaultUser
version: 1.0

[ClickHouse] SHALL use the **default user** when no username and password
are specified during the connection to the server.

### User

#### RQ.SRS-006.RBAC.User
version: 1.0

[ClickHouse] SHALL support creation and manipulation of
one or more **user** accounts to which roles, privileges,
settings profile, quotas and row policies can be assigned.

#### RQ.SRS-006.RBAC.User.Roles
version: 1.0

[ClickHouse] SHALL support assigning one or more **roles**
to a **user**.

#### RQ.SRS-006.RBAC.User.Privileges
version: 1.0

[ClickHouse] SHALL support assigning one or more privileges to a **user**.

#### RQ.SRS-006.RBAC.User.Variables
version: 1.0

[ClickHouse] SHALL support assigning one or more variables to a **user**.

#### RQ.SRS-006.RBAC.User.Variables.Constraints
version: 1.0

[ClickHouse] SHALL support assigning min, max and read-only constraints
for the variables that can be set and read by the **user**.

#### RQ.SRS-006.RBAC.User.SettingsProfile
version: 1.0

[ClickHouse] SHALL support assigning one or more **settings profiles**
to a **user**.

#### RQ.SRS-006.RBAC.User.Quotas
version: 1.0

[ClickHouse] SHALL support assigning one or more **quotas** to a **user**.

#### RQ.SRS-006.RBAC.User.RowPolicies
version: 1.0

[ClickHouse] SHALL support assigning one or more **row policies** to a **user**.

#### RQ.SRS-006.RBAC.User.DefaultRole
version: 1.0

[ClickHouse] SHALL support assigning a default role to a **user**.

#### RQ.SRS-006.RBAC.User.RoleSelection
version: 1.0

[ClickHouse] SHALL support selection of one or more **roles** from the available roles
that are assigned to a **user** using `SET ROLE` statement.

#### RQ.SRS-006.RBAC.User.ShowCreate
version: 1.0

[ClickHouse] SHALL support showing the command of how **user** account was created.

#### RQ.SRS-006.RBAC.User.ShowPrivileges
version: 1.0

[ClickHouse] SHALL support listing the privileges of the **user**.

#### RQ.SRS-006.RBAC.User.Use.DefaultRole
version: 1.0

[ClickHouse] SHALL by default use default role or roles assigned
to the user if specified.

#### RQ.SRS-006.RBAC.User.Use.AllRolesWhenNoDefaultRole
version: 1.0

[ClickHouse] SHALL by default use all the roles assigned to the user
if no default role or roles are specified for the user.

#### Create User

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

#### Alter User

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
to skip raising an exception (producing a warning instead) if a user with the specified **name** does not exist.
If the `IF EXISTS` clause is not specified then an exception SHALL be raised if a user with the **name** does not exist.

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

[ClickHouse] SHALL support altering user by adding and dropping access to hosts
with the `ADD HOST` or the `DROP HOST` in the `ALTER USER` statement.

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

[ClickHouse] SHALL support specifying one or more similar hosts using `LIKE` command syntax
using the `HOST LIKE` clause in the `ALTER USER` statement.

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

#### Show Create User

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

#### Drop User

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

### Role

#### RQ.SRS-006.RBAC.Role
version: 1.0

[ClikHouse] SHALL support creation and manipulation of **roles**
to which privileges, settings profile, quotas and row policies can be
assigned.

#### RQ.SRS-006.RBAC.Role.Privileges
version: 1.0

[ClickHouse] SHALL support assigning one or more privileges to a **role**.

#### RQ.SRS-006.RBAC.Role.Variables
version: 1.0

[ClickHouse] SHALL support assigning one or more variables to a **role**.

#### RQ.SRS-006.RBAC.Role.SettingsProfile
version: 1.0

[ClickHouse] SHALL support assigning one or more **settings profiles**
to a **role**.

#### RQ.SRS-006.RBAC.Role.Quotas
version: 1.0

[ClickHouse] SHALL support assigning one or more **quotas** to a **role**.

#### RQ.SRS-006.RBAC.Role.RowPolicies
version: 1.0

[ClickHouse] SHALL support assigning one or more **row policies** to a **role**.

#### Create Role

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

#### Alter Role

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

#### Drop Role

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

#### Show Create Role

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

### Partial Revokes

#### RQ.SRS-006.RBAC.PartialRevokes
version: 1.0

[ClickHouse] SHALL support partial revoking of privileges granted
to a **user** or a **role**.

#### RQ.SRS-006.RBAC.PartialRevoke.Syntax
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

### Settings Profile

#### RQ.SRS-006.RBAC.SettingsProfile
version: 1.0

[ClickHouse] SHALL support creation and manipulation of **settings profiles**
that can include value definition for one or more variables and can
can be assigned to one or more **users** or **roles**.

#### RQ.SRS-006.RBAC.SettingsProfile.Constraints
version: 1.0

[ClickHouse] SHALL support assigning min, max and read-only constraints
for the variables specified in the **settings profile**.

#### Create Settings Profile

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

#### Alter Settings Profile

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

#### Drop Settings Profile

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

#### Show Create Settings Profile

##### RQ.SRS-006.RBAC.SettingsProfile.ShowCreateSettingsProfile
version: 1.0

[ClickHouse] SHALL support showing the `CREATE SETTINGS PROFILE` statement used to create the settings profile
using the `SHOW CREATE SETTINGS PROFILE` statement with the following syntax

``` sql
SHOW CREATE SETTINGS PROFILE name
```

### Quotas

#### RQ.SRS-006.RBAC.Quotas
version: 1.0

[ClickHouse] SHALL support creation and manipulation of **quotas**
that can be used to limit resource usage by a **user** or a **role**
over a period of time.

#### RQ.SRS-006.RBAC.Quotas.Keyed
version: 1.0

[ClickHouse] SHALL support creating **quotas** that are keyed
so that a quota is tracked separately for each key value.

#### RQ.SRS-006.RBAC.Quotas.Queries
version: 1.0

[ClickHouse] SHALL support setting **queries** quota to limit the total number of requests.

#### RQ.SRS-006.RBAC.Quotas.Errors
version: 1.0

[ClickHouse] SHALL support setting **errors** quota to limit the number of queries that threw an exception.

#### RQ.SRS-006.RBAC.Quotas.ResultRows
version: 1.0

[ClickHouse] SHALL support setting **result rows** quota to limit the
the total number of rows given as the result.

#### RQ.SRS-006.RBAC.Quotas.ReadRows
version: 1.0

[ClickHouse] SHALL support setting **read rows** quota to limit the total
number of source rows read from tables for running the query on all remote servers.

#### RQ.SRS-006.RBAC.Quotas.ResultBytes
version: 1.0

[ClickHouse] SHALL support setting **result bytes** quota to limit the total number
of bytes that can be returned as the result.

#### RQ.SRS-006.RBAC.Quotas.ReadBytes
version: 1.0

[ClickHouse] SHALL support setting **read bytes** quota to limit the total number
of source bytes read from tables for running the query on all remote servers.

#### RQ.SRS-006.RBAC.Quotas.ExecutionTime
version: 1.0

[ClickHouse] SHALL support setting **execution time** quota to limit the maximum
query execution time.

#### Create Quotas

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

#### Alter Quota

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

#### Drop Quota

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

#### Show Quotas

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
https://clickhouse.com/docs/en/interfaces/formats/

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

#### Show Create Quota

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

### Row Policy

#### RQ.SRS-006.RBAC.RowPolicy
version: 1.0

[ClickHouse] SHALL support creation and manipulation of table **row policies**
that can be used to limit access to the table contents for a **user** or a **role**
using a specified **condition**.

#### RQ.SRS-006.RBAC.RowPolicy.Condition
version: 1.0

[ClickHouse] SHALL support row policy **conditions** that can be any SQL
expression that returns a boolean.

#### RQ.SRS-006.RBAC.RowPolicy.Restriction
version: 1.0

[ClickHouse] SHALL restrict all access to a table when a row policy with a condition is created on that table.
All users require a permissive row policy in order to view the table.

#### RQ.SRS-006.RBAC.RowPolicy.Nesting
version: 1.0

[ClickHouse] SHALL restrict rows of tables or views created on top of a table with row policies according to those policies.

#### Create Row Policy

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
REQUIRES CONDITION.

##### RQ.SRS-006.RBAC.RowPolicy.Create.Condition
version: 1.0

[ClickHouse] SHALL support specifying a condition that
that can be any SQL expression which returns a boolean using the `USING`
clause in the `CREATE ROW POLICY` statement.

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

#### Alter Row Policy

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
`AS` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Permissive
version: 1.0

[ClickHouse] SHALL support permitting access to rows using the
`AS PERMISSIVE` clause in the `ALTER ROW POLICY` statement.

##### RQ.SRS-006.RBAC.RowPolicy.Alter.Access.Restrictive
version: 1.0

[ClickHouse] SHALL support restricting access to rows using the
`AS RESTRICTIVE` clause in the `ALTER ROW POLICY` statement.

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

#### Drop Row Policy

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

#### Show Create Row Policy

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

### Set Default Role

#### RQ.SRS-006.RBAC.SetDefaultRole
version: 1.0

[ClickHouse] SHALL support setting or changing granted roles to default for one or more
users using `SET DEFAULT ROLE` statement which
SHALL permanently change the default roles for the user or users if successful.

#### RQ.SRS-006.RBAC.SetDefaultRole.CurrentUser
version: 1.0

[ClickHouse] SHALL support setting or changing granted roles to default for
the current user using `CURRENT_USER` clause in the `SET DEFAULT ROLE` statement.

#### RQ.SRS-006.RBAC.SetDefaultRole.All
version: 1.0

[ClickHouse] SHALL support setting or changing all granted roles to default
for one or more users using `ALL` clause in the `SET DEFAULT ROLE` statement.

#### RQ.SRS-006.RBAC.SetDefaultRole.AllExcept
version: 1.0

[ClickHouse] SHALL support setting or changing all granted roles except those specified
to default for one or more users using `ALL EXCEPT` clause in the `SET DEFAULT ROLE` statement.

#### RQ.SRS-006.RBAC.SetDefaultRole.None
version: 1.0

[ClickHouse] SHALL support removing all granted roles from default
for one or more users using `NONE` clause in the `SET DEFAULT ROLE` statement.

#### RQ.SRS-006.RBAC.SetDefaultRole.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `SET DEFAULT ROLE` statement.

```sql
SET DEFAULT ROLE
    {NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
    TO {user|CURRENT_USER} [,...]

```

### Set Role

#### RQ.SRS-006.RBAC.SetRole
version: 1.0

[ClickHouse] SHALL support activating role or roles for the current user
using `SET ROLE` statement.

#### RQ.SRS-006.RBAC.SetRole.Default
version: 1.0

[ClickHouse] SHALL support activating default roles for the current user
using `DEFAULT` clause in the `SET ROLE` statement.

#### RQ.SRS-006.RBAC.SetRole.None
version: 1.0

[ClickHouse] SHALL support activating no roles for the current user
using `NONE` clause in the `SET ROLE` statement.

#### RQ.SRS-006.RBAC.SetRole.All
version: 1.0

[ClickHouse] SHALL support activating all roles for the current user
using `ALL` clause in the `SET ROLE` statement.

#### RQ.SRS-006.RBAC.SetRole.AllExcept
version: 1.0

[ClickHouse] SHALL support activating all roles except those specified
for the current user using `ALL EXCEPT` clause in the `SET ROLE` statement.

#### RQ.SRS-006.RBAC.SetRole.Syntax
version: 1.0

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

### Grant

#### RQ.SRS-006.RBAC.Grant.Privilege.To
version: 1.0

[ClickHouse] SHALL support granting privileges to one or more users or roles using `TO` clause
in the `GRANT PRIVILEGE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.ToCurrentUser
version: 1.0

[ClickHouse] SHALL support granting privileges to current user using `TO CURRENT_USER` clause
in the `GRANT PRIVILEGE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Select
version: 1.0

[ClickHouse] SHALL support granting the **select** privilege to one or more users or roles
for a database or a table using the `GRANT SELECT` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support granting the **insert** privilege to one or more users or roles
for a database or a table using the `GRANT INSERT` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support granting the **alter** privilege to one or more users or roles
for a database or a table using the `GRANT ALTER` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Create
version: 1.0

[ClickHouse] SHALL support granting the **create** privilege to one or more users or roles
using the `GRANT CREATE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support granting the **drop** privilege to one or more users or roles
using the `GRANT DROP` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support granting the **truncate** privilege to one or more users or roles
for a database or a table using `GRANT TRUNCATE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support granting the **optimize** privilege to one or more users or roles
for a database or a table using `GRANT OPTIMIZE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Show
version: 1.0

[ClickHouse] SHALL support granting the **show** privilege to one or more users or roles
for a database or a table using `GRANT SHOW` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support granting the **kill query** privilege to one or more users or roles
for a database or a table using `GRANT KILL QUERY` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support granting the **access management** privileges to one or more users or roles
for a database or a table using `GRANT ACCESS MANAGEMENT` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.System
version: 1.0

[ClickHouse] SHALL support granting the **system** privileges to one or more users or roles
for a database or a table using `GRANT SYSTEM` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support granting the **introspection** privileges to one or more users or roles
for a database or a table using `GRANT INTROSPECTION` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support granting the **sources** privileges to one or more users or roles
for a database or a table using `GRANT SOURCES` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support granting the **dictGet** privilege to one or more users or roles
for a database or a table using `GRANT dictGet` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.None
version: 1.0

[ClickHouse] SHALL support granting no privileges to one or more users or roles
for a database or a table using `GRANT NONE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.All
version: 1.0

[ClickHouse] SHALL support granting the **all** privileges to one or more users or roles
using the `GRANT ALL` or `GRANT ALL PRIVILEGES` statements.

#### RQ.SRS-006.RBAC.Grant.Privilege.GrantOption
version: 1.0

[ClickHouse] SHALL support granting the **grant option** privilege to one or more users or roles
for a database or a table using the `WITH GRANT OPTION` clause in the `GRANT` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.On
version: 1.0

[ClickHouse] SHALL support the `ON` clause in the `GRANT` privilege statement
which SHALL allow to specify one or more tables to which the privilege SHALL
be granted using the following patterns

* `*.*` any table in any database
* `database.*` any table in the specified database
* `database.table` specific table in the specified database
* `*` any table in the current database
* `table` specific table in the current database

#### RQ.SRS-006.RBAC.Grant.Privilege.PrivilegeColumns
version: 1.0

[ClickHouse] SHALL support granting the privilege **some_privilege** to one or more users or roles
for a database or a table using the `GRANT some_privilege(column)` statement for one column.
Multiple columns will be supported with `GRANT some_privilege(column1, column2...)` statement.
The privileges will be granted for only the specified columns.

#### RQ.SRS-006.RBAC.Grant.Privilege.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which to grant privileges using the `ON CLUSTER`
clause in the `GRANT PRIVILEGE` statement.

#### RQ.SRS-006.RBAC.Grant.Privilege.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `GRANT` statement that
grants explicit privileges to a user or a role.

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...]
    ON {db.table|db.*|*.*|table|*}
    TO {user | role | CURRENT_USER} [,...]
    [WITH GRANT OPTION]
```

### Revoke

#### RQ.SRS-006.RBAC.Revoke.Privilege.Cluster
version: 1.0

[ClickHouse] SHALL support revoking privileges to one or more users or roles
for a database or a table on some specific cluster using the `REVOKE ON CLUSTER cluster_name` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Select
version: 1.0

[ClickHouse] SHALL support revoking the **select** privilege to one or more users or roles
for a database or a table using the `REVOKE SELECT` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Insert
version: 1.0

[ClickHouse] SHALL support revoking the **insert** privilege to one or more users or roles
for a database or a table using the `REVOKE INSERT` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Alter
version: 1.0

[ClickHouse] SHALL support revoking the **alter** privilege to one or more users or roles
for a database or a table using the `REVOKE ALTER` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Create
version: 1.0

[ClickHouse] SHALL support revoking the **create** privilege to one or more users or roles
using the `REVOKE CREATE` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Drop
version: 1.0

[ClickHouse] SHALL support revoking the **drop** privilege to one or more users or roles
using the `REVOKE DROP` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Truncate
version: 1.0

[ClickHouse] SHALL support revoking the **truncate** privilege to one or more users or roles
for a database or a table using the `REVOKE TRUNCATE` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Optimize
version: 1.0

[ClickHouse] SHALL support revoking the **optimize** privilege to one or more users or roles
for a database or a table using the `REVOKE OPTIMIZE` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Show
version: 1.0

[ClickHouse] SHALL support revoking the **show** privilege to one or more users or roles
for a database or a table using the `REVOKE SHOW` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.KillQuery
version: 1.0

[ClickHouse] SHALL support revoking the **kill query** privilege to one or more users or roles
for a database or a table using the `REVOKE KILL QUERY` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.AccessManagement
version: 1.0

[ClickHouse] SHALL support revoking the **access management** privilege to one or more users or roles
for a database or a table using the `REVOKE ACCESS MANAGEMENT` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.System
version: 1.0

[ClickHouse] SHALL support revoking the **system** privilege to one or more users or roles
for a database or a table using the `REVOKE SYSTEM` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Introspection
version: 1.0

[ClickHouse] SHALL support revoking the **introspection** privilege to one or more users or roles
for a database or a table using the `REVOKE INTROSPECTION` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Sources
version: 1.0

[ClickHouse] SHALL support revoking the **sources** privilege to one or more users or roles
for a database or a table using the `REVOKE SOURCES` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.DictGet
version: 1.0

[ClickHouse] SHALL support revoking the **dictGet** privilege to one or more users or roles
for a database or a table using the `REVOKE dictGet` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.PrivilegeColumns
version: 1.0

[ClickHouse] SHALL support revoking the privilege **some_privilege** to one or more users or roles
for a database or a table using the `REVOKE some_privilege(column)` statement for one column.
Multiple columns will be supported with `REVOKE some_privilege(column1, column2...)` statement.
The privileges will be revoked for only the specified columns.

#### RQ.SRS-006.RBAC.Revoke.Privilege.Multiple
version: 1.0

[ClickHouse] SHALL support revoking MULTIPLE **privileges** to one or more users or roles
for a database or a table using the `REVOKE privilege1, privilege2...` statement.
**privileges** refers to any set of Clickhouse defined privilege, whose hierarchy includes
SELECT, INSERT, ALTER, CREATE, DROP, TRUNCATE, OPTIMIZE, SHOW, KILL QUERY, ACCESS MANAGEMENT,
SYSTEM, INTROSPECTION, SOURCES, dictGet and all of their sub-privileges.

#### RQ.SRS-006.RBAC.Revoke.Privilege.All
version: 1.0

[ClickHouse] SHALL support revoking **all** privileges to one or more users or roles
for a database or a table using the `REVOKE ALL` or `REVOKE ALL PRIVILEGES` statements.

#### RQ.SRS-006.RBAC.Revoke.Privilege.None
version: 1.0

[ClickHouse] SHALL support revoking **no** privileges to one or more users or roles
for a database or a table using the `REVOKE NONE` statement.

#### RQ.SRS-006.RBAC.Revoke.Privilege.On
version: 1.0

[ClickHouse] SHALL support the `ON` clause in the `REVOKE` privilege statement
which SHALL allow to specify one or more tables to which the privilege SHALL
be revoked using the following patterns

* `db.table` specific table in the specified database
* `db.*` any table in the specified database
* `*.*` any table in any database
* `table` specific table in the current database
* `*` any table in the current database

#### RQ.SRS-006.RBAC.Revoke.Privilege.From
version: 1.0

[ClickHouse] SHALL support the `FROM` clause in the `REVOKE` privilege statement
which SHALL allow to specify one or more users to which the privilege SHALL
be revoked using the following patterns

* `{user | CURRENT_USER} [,...]` some combination of users by name, which may include the current user
* `ALL` all users
* `ALL EXCEPT {user | CURRENT_USER} [,...]` the logical reverse of the first pattern

#### RQ.SRS-006.RBAC.Revoke.Privilege.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `REVOKE` statement that
revokes explicit privileges of a user or a role.

```sql
REVOKE [ON CLUSTER cluster_name] privilege
    [(column_name [,...])] [,...]
    ON {db.table|db.*|*.*|table|*}
    FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

### Grant Role

#### RQ.SRS-006.RBAC.Grant.Role
version: 1.0

[ClickHouse] SHALL support granting one or more roles to
one or more users or roles using the `GRANT` role statement.

#### RQ.SRS-006.RBAC.Grant.Role.CurrentUser
version: 1.0

[ClickHouse] SHALL support granting one or more roles to current user using
`TO CURRENT_USER` clause in the `GRANT` role statement.

#### RQ.SRS-006.RBAC.Grant.Role.AdminOption
version: 1.0

[ClickHouse] SHALL support granting `admin option` privilege
to one or more users or roles using the `WITH ADMIN OPTION` clause
in the `GRANT` role statement.

#### RQ.SRS-006.RBAC.Grant.Role.OnCluster
version: 1.0

[ClickHouse] SHALL support specifying cluster on which the user is to be granted one or more roles
using `ON CLUSTER` clause in the `GRANT` statement.

#### RQ.SRS-006.RBAC.Grant.Role.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for `GRANT` role statement

``` sql
GRANT
    ON CLUSTER cluster_name
    role [, role ...]
    TO {user | role | CURRENT_USER} [,...]
    [WITH ADMIN OPTION]
```

### Revoke Role

#### RQ.SRS-006.RBAC.Revoke.Role
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
one or more users or roles using the `REVOKE` role statement.

#### RQ.SRS-006.RBAC.Revoke.Role.Keywords
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
special groupings of one or more users or roles with the `ALL`, `ALL EXCEPT`,
and `CURRENT_USER` keywords.

#### RQ.SRS-006.RBAC.Revoke.Role.Cluster
version: 1.0

[ClickHouse] SHALL support revoking one or more roles from
one or more users or roles from one or more clusters
using the `REVOKE ON CLUSTER` role statement.

#### RQ.SRS-006.RBAC.Revoke.AdminOption
version: 1.0

[ClickHouse] SHALL support revoking `admin option` privilege
in one or more users or roles using the `ADMIN OPTION FOR` clause
in the `REVOKE` role statement.

#### RQ.SRS-006.RBAC.Revoke.Role.Syntax
version: 1.0

[ClickHouse] SHALL support the following syntax for the `REVOKE` role statement

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR]
    role [,...]
    FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

### Show Grants

#### RQ.SRS-006.RBAC.Show.Grants
version: 1.0

[ClickHouse] SHALL support listing all the privileges granted to current user and role
using the `SHOW GRANTS` statement.

#### RQ.SRS-006.RBAC.Show.Grants.For
version: 1.0

[ClickHouse] SHALL support listing all the privileges granted to a user or a role
using the `FOR` clause in the `SHOW GRANTS` statement.

#### RQ.SRS-006.RBAC.Show.Grants.Syntax
version: 1.0

[Clickhouse] SHALL use the following syntax for the `SHOW GRANTS` statement

``` sql
SHOW GRANTS [FOR user_or_role]
```

### Table Privileges

#### RQ.SRS-006.RBAC.Table.PublicTables
version: 1.0

[ClickHouse] SHALL support that a user without any privileges will be able to access the following tables

* system.one
* system.numbers
* system.contributors
* system.functions

#### RQ.SRS-006.RBAC.Table.SensitiveTables
version: 1.0

[ClickHouse] SHALL not support a user with no privileges accessing the following `system` tables:

* processes
* query_log
* query_thread_log
* query_views_log
* clusters
* events
* graphite_retentions
* stack_trace
* trace_log
* user_directories
* zookeeper
* macros

### Distributed Tables

#### RQ.SRS-006.RBAC.DistributedTable.Create
version: 1.0

[ClickHouse] SHALL successfully `CREATE` a distributed table if and only if
the user has **create table** privilege on the table and **remote** privilege on *.*

#### RQ.SRS-006.RBAC.DistributedTable.Select
version: 1.0

[ClickHouse] SHALL successfully `SELECT` from a distributed table if and only if
the user has **select** privilege on the table and on the remote table specified in the `CREATE` query of the distributed table.

Does not require **select** privilege for the remote table if the remote table does not exist on the same server as the user.

#### RQ.SRS-006.RBAC.DistributedTable.Insert
version: 1.0

[ClickHouse] SHALL successfully `INSERT` into a distributed table if and only if
the user has **insert** privilege on the table and on the remote table specified in the `CREATE` query of the distributed table.

Does not require **insert** privilege for the remote table if the remote table does not exist on the same server as the user,
insert executes into the remote table on a different server.

#### RQ.SRS-006.RBAC.DistributedTable.SpecialTables
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table that uses one of the special tables if and only if
the user has the necessary privileges to interact with that special table, either granted directly or through a role.
Special tables include:
* materialized view
* distributed table
* source table of a materialized view

#### RQ.SRS-006.RBAC.DistributedTable.LocalUser
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table from
a user present locally, but not remotely.

#### RQ.SRS-006.RBAC.DistributedTable.SameUserDifferentNodesDifferentPrivileges
version: 1.0

[ClickHouse] SHALL successfully execute a query using a distributed table by a user that exists on multiple nodes
if and only if the user has the required privileges on the node the query is being executed from.

### Views

#### View

##### RQ.SRS-006.RBAC.View
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select** and **drop**
privileges for a view for users or roles.

##### RQ.SRS-006.RBAC.View.Create
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

##### RQ.SRS-006.RBAC.View.Select
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

##### RQ.SRS-006.RBAC.View.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

#### Materialized View

##### RQ.SRS-006.RBAC.MaterializedView
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select**, **alter** and **drop**
privileges for a materialized view for users or roles.

##### RQ.SRS-006.RBAC.MaterializedView.Create
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

##### RQ.SRS-006.RBAC.MaterializedView.Select
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

##### RQ.SRS-006.RBAC.MaterializedView.Select.TargetTable
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from the target table, implicit or explicit, of a materialized view if and only if
the user has `SELECT` privilege for the table, either explicitly or through a role.

##### RQ.SRS-006.RBAC.MaterializedView.Select.SourceTable
version: 1.0

[ClickHouse] SHALL only successfully `SELECT` from the source table of a materialized view if and only if
the user has `SELECT` privilege for the table, either explicitly or through a role.

##### RQ.SRS-006.RBAC.MaterializedView.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

##### RQ.SRS-006.RBAC.MaterializedView.ModifyQuery
version: 1.0

[ClickHouse] SHALL only successfully execute a `MODIFY QUERY` command if and only if
the user has **modify query** privilege on that view either explicitly or through a role.

If the new query includes one or more source tables, the user must have **select** privilege
on all the source tables either explicitly or through a role.
For example,
```sql
ALTER TABLE view MODIFY QUERY SELECT * FROM source_table
```

##### RQ.SRS-006.RBAC.MaterializedView.Insert
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a materialized view if and only if
the user has `INSERT` privilege on the view, either explicitly or through a role.

##### RQ.SRS-006.RBAC.MaterializedView.Insert.SourceTable
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a source table of a materialized view if and only if
the user has `INSERT` privilege on the source table, either explicitly or through a role.

##### RQ.SRS-006.RBAC.MaterializedView.Insert.TargetTable
version: 1.0

[ClickHouse] SHALL only succesfully `INSERT` into a target table of a materialized view if and only if
the user has `INSERT` privelege on the target table, either explicitly or through a role.

#### Live View

##### RQ.SRS-006.RBAC.LiveView
version: 1.0

[ClickHouse] SHALL support controlling access to **create**, **select**, **alter** and **drop**
privileges for a live view for users or roles.

##### RQ.SRS-006.RBAC.LiveView.Create
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

##### RQ.SRS-006.RBAC.LiveView.Select
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

##### RQ.SRS-006.RBAC.LiveView.Drop
version: 1.0

[ClickHouse] SHALL only successfully execute a `DROP VIEW` command if and only if
the user has **drop view** privilege on that view either explicitly or through a role.

##### RQ.SRS-006.RBAC.LiveView.Refresh
version: 1.0

[ClickHouse] SHALL only successfully execute an `ALTER LIVE VIEW REFRESH` command if and only if
the user has **refresh** privilege on that view either explicitly or through a role.

### Select

#### RQ.SRS-006.RBAC.Select
version: 1.0

[ClickHouse] SHALL execute `SELECT` if and only if the user
has the **select** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

#### RQ.SRS-006.RBAC.Select.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **select** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `SELECT` statements SHALL not to be executed, unless the user
has the **select** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

#### RQ.SRS-006.RBAC.Select.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **select** privilege
on a specified cluster to one or more **users** or **roles**.
Any `SELECT` statements SHALL succeed only on nodes where
the table exists and privilege was granted.

#### RQ.SRS-006.RBAC.Select.TableEngines
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

### Insert

#### RQ.SRS-006.RBAC.Insert
version: 1.0

[ClickHouse] SHALL execute `INSERT INTO` if and only if the user
has the **insert** privilege for the destination table
either because of the explicit grant or through one of the roles assigned to the user.

#### RQ.SRS-006.RBAC.Insert.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **insert** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `INSERT INTO` statements SHALL not to be executed, unless the user
has the **insert** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

#### RQ.SRS-006.RBAC.Insert.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **insert** privilege
on a specified cluster to one or more **users** or **roles**.
Any `INSERT INTO` statements SHALL succeed only on nodes where
the table exists and privilege was granted.

#### RQ.SRS-006.RBAC.Insert.TableEngines
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

### Alter

#### Alter Column

##### RQ.SRS-006.RBAC.Privileges.AlterColumn
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter column** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN` statements SHALL
return an error, unless the user has the **alter column** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

##### RQ.SRS-006.RBAC.Privileges.AlterColumn.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter column** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterColumn.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter column** privilege
for a database or a specific table to one or more **users** or **roles**

##### RQ.SRS-006.RBAC.Privileges.AlterColumn.Column
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter column** privilege
for one or more specified columns in a table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN` statements SHALL return an error,
unless the user has the **alter column** privilege for the destination column
either because of the explicit grant or through one of the roles assigned to the user.

##### RQ.SRS-006.RBAC.Privileges.AlterColumn.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter column** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

##### RQ.SRS-006.RBAC.Privileges.AlterColumn.TableEngines
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

#### Alter Index

##### RQ.SRS-006.RBAC.Privileges.AlterIndex
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter index** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ORDER BY | ADD|DROP|MATERIALIZE|CLEAR INDEX` statements SHALL
return an error, unless the user has the **alter index** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

##### RQ.SRS-006.RBAC.Privileges.AlterIndex.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter index** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterIndex.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter index** privilege
for a database or a specific table to one or more **users** or **roles**

##### RQ.SRS-006.RBAC.Privileges.AlterIndex.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter index** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ORDER BY | ADD|DROP|MATERIALIZE|CLEAR INDEX`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

##### RQ.SRS-006.RBAC.Privileges.AlterIndex.TableEngines
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

#### Alter Constraint

##### RQ.SRS-006.RBAC.Privileges.AlterConstraint
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|CREATE CONSTRAINT` statements SHALL
return an error, unless the user has the **alter constraint** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

##### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter constraint** privilege
for a database or a specific table to one or more **users** or **roles**

##### RQ.SRS-006.RBAC.Privileges.AlterConstraint.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter constraint** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ADD|DROP CONSTRAINT`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

##### RQ.SRS-006.RBAC.Privileges.AlterConstraint.TableEngines
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

#### Alter TTL

##### RQ.SRS-006.RBAC.Privileges.AlterTTL
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... ALTER TTL | ALTER MATERIALIZE TTL` statements SHALL
return an error, unless the user has the **alter ttl** or **alter materialize ttl** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user.

##### RQ.SRS-006.RBAC.Privileges.AlterTTL.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterTTL.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter ttl** or **alter materialize ttl** privilege
for a database or a specific table to one or more **users** or **roles**

##### RQ.SRS-006.RBAC.Privileges.AlterTTL.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter ttl** or **alter materialize ttl** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... ALTER TTL | ALTER MATERIALIZE TTL`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

##### RQ.SRS-006.RBAC.Privileges.AlterTTL.TableEngines
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter ttl** or **alter materialize ttl** privilege
on tables created using the following engines

* MergeTree

#### Alter Settings

##### RQ.SRS-006.RBAC.Privileges.AlterSettings
version: 1.0

[ClickHouse] SHALL support controlling access to the **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**.
Any `ALTER TABLE ... MODIFY SETTING setting` statements SHALL
return an error, unless the user has the **alter settings** privilege for
the destination table either because of the explicit grant or through one of
the roles assigned to the user. The **alter settings** privilege allows
modifying table engine settings. It doesn’t affect settings or server configuration parameters.

##### RQ.SRS-006.RBAC.Privileges.AlterSettings.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterSettings.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter settings** privilege
for a database or a specific table to one or more **users** or **roles**

##### RQ.SRS-006.RBAC.Privileges.AlterSettings.Cluster
version: 1.0

[ClickHouse] SHALL support granting or revoking **alter settings** privilege
on a specified cluster to one or more **users** or **roles**.
Any `ALTER TABLE ... MODIFY SETTING setting`
statements SHALL succeed only on nodes where the table exists and privilege was granted.

##### RQ.SRS-006.RBAC.Privileges.AlterSettings.TableEngines
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

#### Alter Update

##### RQ.SRS-006.RBAC.Privileges.AlterUpdate
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER UPDATE` statement if and only if the user has **alter update** privilege for that column,
either directly or through a role.

##### RQ.SRS-006.RBAC.Privileges.AlterUpdate.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter update** privilege on a column level
to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterUpdate.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter update** privilege on a column level
from one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterUpdate.TableEngines
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

#### Alter Delete

##### RQ.SRS-006.RBAC.Privileges.AlterDelete
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER DELETE` statement if and only if the user has **alter delete** privilege for that table,
either directly or through a role.

##### RQ.SRS-006.RBAC.Privileges.AlterDelete.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter delete** privilege on a column level
to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterDelete.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter delete** privilege on a column level
from one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterDelete.TableEngines
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

#### Alter Freeze Partition

##### RQ.SRS-006.RBAC.Privileges.AlterFreeze
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER FREEZE` statement if and only if the user has **alter freeze** privilege for that table,
either directly or through a role.

##### RQ.SRS-006.RBAC.Privileges.AlterFreeze.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter freeze** privilege on a column level
to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterFreeze.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter freeze** privilege on a column level
from one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterFreeze.TableEngines
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

#### Alter Fetch Partition

##### RQ.SRS-006.RBAC.Privileges.AlterFetch
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER FETCH` statement if and only if the user has **alter fetch** privilege for that table,
either directly or through a role.

##### RQ.SRS-006.RBAC.Privileges.AlterFetch.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter fetch** privilege on a column level
to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterFetch.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter fetch** privilege on a column level
from one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterFetch.TableEngines
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

#### Alter Move Partition

##### RQ.SRS-006.RBAC.Privileges.AlterMove
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER MOVE` statement if and only if the user has **alter move**, **select**, and **alter delete** privilege on the source table
and **insert** privilege on the target table, either directly or through a role.
For example,
```sql
ALTER TABLE source_table MOVE PARTITION 1 TO target_table
```

##### RQ.SRS-006.RBAC.Privileges.AlterMove.Grant
version: 1.0

[ClickHouse] SHALL support granting **alter move** privilege on a column level
to one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterMove.Revoke
version: 1.0

[ClickHouse] SHALL support revoking **alter move** privilege on a column level
from one or more **users** or **roles**.

##### RQ.SRS-006.RBAC.Privileges.AlterMove.TableEngines
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

### Create

#### RQ.SRS-006.RBAC.Privileges.CreateTable
version: 1.0

[ClickHouse] SHALL only successfully execute a `CREATE TABLE` command if and only if
the user has **create table** privilege either explicitly or through roles.

If the stored query includes one or more source tables, the user must have **select** privilege
on all the source tables and **insert** for the table they're trying to create either explicitly or through a role.
For example,
```sql
CREATE TABLE table AS SELECT * FROM source_table
CREATE TABLE table AS SELECT * FROM table0 WHERE column IN (SELECT column FROM table1 WHERE column IN (SELECT column FROM table2 WHERE expression))
CREATE TABLE table AS SELECT * FROM table0 JOIN table1 USING column
CREATE TABLE table AS SELECT * FROM table0 UNION ALL SELECT * FROM table1 UNION ALL SELECT * FROM table2
CREATE TABLE table AS SELECT column FROM table0 JOIN table1 USING column UNION ALL SELECT column FROM table2 WHERE column IN (SELECT column FROM table3 WHERE column IN (SELECT column FROM table4 WHERE expression))
CREATE TABLE table0 AS SELECT column FROM table1 UNION ALL SELECT column FROM table2
```

#### RQ.SRS-006.RBAC.Privileges.CreateDatabase
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE DATABASE` statement if and only if the user has **create database** privilege on the database,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateDictionary
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE DICTIONARY` statement if and only if the user has **create dictionary** privilege on the dictionary,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateTemporaryTable
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE TEMPORARY TABLE` statement if and only if the user has **create temporary table** privilege on the table,
either directly or through a role.

### Attach

#### RQ.SRS-006.RBAC.Privileges.AttachDatabase
version: 1.0

[ClickHouse] SHALL successfully execute `ATTACH DATABASE` statement if and only if the user has **create database** privilege on the database,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AttachDictionary
version: 1.0

[ClickHouse] SHALL successfully execute `ATTACH DICTIONARY` statement if and only if the user has **create dictionary** privilege on the dictionary,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AttachTemporaryTable
version: 1.0

[ClickHouse] SHALL successfully execute `ATTACH TEMPORARY TABLE` statement if and only if the user has **create temporary table** privilege on the table,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AttachTable
version: 1.0

[ClickHouse] SHALL successfully execute `ATTACH TABLE` statement if and only if the user has **create table** privilege on the table,
either directly or through a role.

### Drop

#### RQ.SRS-006.RBAC.Privileges.DropTable
version: 1.0

[ClickHouse] SHALL successfully execute `DROP TABLE` statement if and only if the user has **drop table** privilege on the table,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropDatabase
version: 1.0

[ClickHouse] SHALL successfully execute `DROP DATABASE` statement if and only if the user has **drop database** privilege on the database,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropDictionary
version: 1.0

[ClickHouse] SHALL successfully execute `DROP DICTIONARY` statement if and only if the user has **drop dictionary** privilege on the dictionary,
either directly or through a role.

### Detach

#### RQ.SRS-006.RBAC.Privileges.DetachTable
version: 1.0

[ClickHouse] SHALL successfully execute `DETACH TABLE` statement if and only if the user has **drop table** privilege on the table,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DetachView
version: 1.0

[ClickHouse] SHALL successfully execute `DETACH VIEW` statement if and only if the user has **drop view** privilege on the view,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DetachDatabase
version: 1.0

[ClickHouse] SHALL successfully execute `DETACH DATABASE` statement if and only if the user has **drop database** privilege on the database,
either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DetachDictionary
version: 1.0

[ClickHouse] SHALL successfully execute `DETACH DICTIONARY` statement if and only if the user has **drop dictionary** privilege on the dictionary,
either directly or through a role.

### Truncate

#### RQ.SRS-006.RBAC.Privileges.Truncate
version: 1.0

[ClickHouse] SHALL successfully execute `TRUNCATE TABLE` statement if and only if the user has **truncate table** privilege on the table,
either directly or through a role.

### Optimize

#### RQ.SRS-006.RBAC.Privileges.Optimize
version: 1.0

[ClickHouse] SHALL successfully execute `OPTIMIZE TABLE` statement if and only if the user has **optimize table** privilege on the table,
either directly or through a role.

### Kill Query

#### RQ.SRS-006.RBAC.Privileges.KillQuery
version: 1.0

[ClickHouse] SHALL successfully execute `KILL QUERY` statement if and only if the user has **kill query** privilege,
either directly or through a role.

### Kill Mutation

#### RQ.SRS-006.RBAC.Privileges.KillMutation
version: 1.0

[ClickHouse] SHALL successfully execute `KILL MUTATION` statement if and only if
the user has the privilege that created the mutation, either directly or through a role.
For example, to `KILL MUTATION` after `ALTER UPDATE` query, the user needs `ALTER UPDATE` privilege.

#### RQ.SRS-006.RBAC.Privileges.KillMutation.AlterUpdate
version: 1.0

[ClickHouse] SHALL successfully execute `KILL MUTATION` query on an `ALTER UPDATE` mutation if and only if
the user has `ALTER UPDATE` privilege on the table where the mutation was created, either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.KillMutation.AlterDelete
version: 1.0

[ClickHouse] SHALL successfully execute `KILL MUTATION` query on an `ALTER DELETE` mutation if and only if
the user has `ALTER DELETE` privilege on the table where the mutation was created, either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.KillMutation.AlterDropColumn
version: 1.0

[ClickHouse] SHALL successfully execute `KILL MUTATION` query on an `ALTER DROP COLUMN` mutation if and only if
the user has `ALTER DROP COLUMN` privilege on the table where the mutation was created, either directly or through a role.

### Show

#### RQ.SRS-006.RBAC.ShowTables.Privilege
version: 1.0

[ClickHouse] SHALL grant **show tables** privilege on a table to a user if that user has recieved any grant,
including `SHOW TABLES`, on that table, either directly or through a role.

#### RQ.SRS-006.RBAC.ShowTables.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW TABLES` statement if and only if the user has **show tables** privilege,
or any privilege on the table either directly or through a role.

#### RQ.SRS-006.RBAC.ExistsTable.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `EXISTS table` statement if and only if the user has **show tables** privilege,
or any privilege on the table either directly or through a role.

#### RQ.SRS-006.RBAC.CheckTable.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `CHECK table` statement if and only if the user has **show tables** privilege,
or any privilege on the table either directly or through a role.

#### RQ.SRS-006.RBAC.ShowDatabases.Privilege
version: 1.0

[ClickHouse] SHALL grant **show databases** privilege on a database to a user if that user has recieved any grant,
including `SHOW DATABASES`, on that table, either directly or through a role.

#### RQ.SRS-006.RBAC.ShowDatabases.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW DATABASES` statement if and only if the user has **show databases** privilege,
or any privilege on the database either directly or through a role.

#### RQ.SRS-006.RBAC.ShowCreateDatabase.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE DATABASE` statement if and only if the user has **show databases** privilege,
or any privilege on the database either directly or through a role.

#### RQ.SRS-006.RBAC.UseDatabase.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `USE database` statement if and only if the user has **show databases** privilege,
or any privilege on the database either directly or through a role.

#### RQ.SRS-006.RBAC.ShowColumns.Privilege
version: 1.0

[ClickHouse] SHALL support granting or revoking the `SHOW COLUMNS` privilege.

#### RQ.SRS-006.RBAC.ShowCreateTable.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE TABLE` statement if and only if the user has **show columns** privilege on that table,
either directly or through a role.

#### RQ.SRS-006.RBAC.DescribeTable.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `DESCRIBE table` statement if and only if the user has **show columns** privilege on that table,
either directly or through a role.

#### RQ.SRS-006.RBAC.ShowDictionaries.Privilege
version: 1.0

[ClickHouse] SHALL grant **show dictionaries** privilege on a dictionary to a user if that user has recieved any grant,
including `SHOW DICTIONARIES`, on that dictionary, either directly or through a role.

#### RQ.SRS-006.RBAC.ShowDictionaries.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW DICTIONARIES` statement if and only if the user has **show dictionaries** privilege,
or any privilege on the dictionary either directly or through a role.

#### RQ.SRS-006.RBAC.ShowCreateDictionary.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE DICTIONARY` statement if and only if the user has **show dictionaries** privilege,
or any privilege on the dictionary either directly or through a role.

#### RQ.SRS-006.RBAC.ExistsDictionary.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `EXISTS dictionary` statement if and only if the user has **show dictionaries** privilege,
or any privilege on the dictionary either directly or through a role.

### Access Management

#### RQ.SRS-006.RBAC.Privileges.CreateUser
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE USER` statement if and only if the user has **create user** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateUser.DefaultRole
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE USER` statement with `DEFAULT ROLE <role>` clause if and only if
the user has **create user** privilege and the role with **admin option**, or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AlterUser
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER USER` statement if and only if the user has **alter user** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropUser
version: 1.0

[ClickHouse] SHALL successfully execute `DROP USER` statement if and only if the user has **drop user** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateRole
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE ROLE` statement if and only if the user has **create role** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AlterRole
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER ROLE` statement if and only if the user has **alter role** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropRole
version: 1.0

[ClickHouse] SHALL successfully execute `DROP ROLE` statement if and only if the user has **drop role** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateRowPolicy
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE ROW POLICY` statement if and only if the user has **create row policy** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AlterRowPolicy
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER ROW POLICY` statement if and only if the user has **alter row policy** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropRowPolicy
version: 1.0

[ClickHouse] SHALL successfully execute `DROP ROW POLICY` statement if and only if the user has **drop row policy** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateQuota
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE QUOTA` statement if and only if the user has **create quota** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AlterQuota
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER QUOTA` statement if and only if the user has **alter quota** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropQuota
version: 1.0

[ClickHouse] SHALL successfully execute `DROP QUOTA` statement if and only if the user has **drop quota** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.CreateSettingsProfile
version: 1.0

[ClickHouse] SHALL successfully execute `CREATE SETTINGS PROFILE` statement if and only if the user has **create settings profile** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.AlterSettingsProfile
version: 1.0

[ClickHouse] SHALL successfully execute `ALTER SETTINGS PROFILE` statement if and only if the user has **alter settings profile** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.DropSettingsProfile
version: 1.0

[ClickHouse] SHALL successfully execute `DROP SETTINGS PROFILE` statement if and only if the user has **drop settings profile** privilege,
or either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.RoleAdmin
version: 1.0

[ClickHouse] SHALL successfully execute any role grant or revoke by a user with `ROLE ADMIN` privilege.

#### Show Access

##### RQ.SRS-006.RBAC.ShowUsers.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `SHOW USERS` privilege when
the user is granted `SHOW USERS`, `SHOW CREATE USER`, `SHOW ACCESS`, or `ACCESS MANAGEMENT`.

##### RQ.SRS-006.RBAC.ShowUsers.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW USERS` statement if and only if the user has **show users** privilege,
either directly or through a role.

##### RQ.SRS-006.RBAC.ShowCreateUser.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE USER` statement if and only if the user has **show users** privilege,
either directly or through a role.

##### RQ.SRS-006.RBAC.ShowRoles.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `SHOW ROLES` privilege when
the user is granted `SHOW ROLES`, `SHOW CREATE ROLE`, `SHOW ACCESS`, or `ACCESS MANAGEMENT`.

##### RQ.SRS-006.RBAC.ShowRoles.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW ROLES` statement if and only if the user has **show roles** privilege,
either directly or through a role.

##### RQ.SRS-006.RBAC.ShowCreateRole.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE ROLE` statement if and only if the user has **show roles** privilege,
either directly or through a role.

##### RQ.SRS-006.RBAC.ShowRowPolicies.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `SHOW ROW POLICIES` privilege when
the user is granted `SHOW ROW POLICIES`, `SHOW POLICIES`, `SHOW CREATE ROW POLICY`,
`SHOW CREATE POLICY`, `SHOW ACCESS`, or `ACCESS MANAGEMENT`.

##### RQ.SRS-006.RBAC.ShowRowPolicies.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW ROW POLICIES` or `SHOW POLICIES` statement if and only if
the user has **show row policies** privilege, either directly or through a role.

##### RQ.SRS-006.RBAC.ShowCreateRowPolicy.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE ROW POLICY` or `SHOW CREATE POLICY` statement
if and only if the user has **show row policies** privilege,either directly or through a role.

##### RQ.SRS-006.RBAC.ShowQuotas.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `SHOW QUOTAS` privilege when
the user is granted `SHOW QUOTAS`, `SHOW CREATE QUOTA`, `SHOW ACCESS`, or `ACCESS MANAGEMENT`.

##### RQ.SRS-006.RBAC.ShowQuotas.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW QUOTAS` statement if and only if the user has **show quotas** privilege,
either directly or through a role.

##### RQ.SRS-006.RBAC.ShowCreateQuota.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE QUOTA` statement if and only if
the user has **show quotas** privilege, either directly or through a role.

##### RQ.SRS-006.RBAC.ShowSettingsProfiles.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `SHOW SETTINGS PROFILES` privilege when
the user is granted `SHOW SETTINGS PROFILES`, `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`,
`SHOW SETTINGS PROFILE`, `SHOW ACCESS`, or `ACCESS MANAGEMENT`.

##### RQ.SRS-006.RBAC.ShowSettingsProfiles.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW SETTINGS PROFILES` or `SHOW PROFILES` statement
if and only if the user has **show settings profiles** privilege, either directly or through a role.

##### RQ.SRS-006.RBAC.ShowCreateSettingsProfile.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `SHOW CREATE SETTINGS PROFILE` or `SHOW CREATE PROFILE` statement
if and only if the user has **show settings profiles** privilege, either directly or through a role.

### dictGet

#### RQ.SRS-006.RBAC.dictGet.Privilege
version: 1.0

[ClickHouse] SHALL successfully grant `dictGet` privilege when
the user is granted `dictGet`, `dictHas`, `dictGetHierarchy`, or `dictIsIn`.

#### RQ.SRS-006.RBAC.dictGet.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictGet` statement
if and only if the user has **dictGet** privilege on that dictionary, either directly or through a role.

#### RQ.SRS-006.RBAC.dictGet.Type.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictGet[TYPE]` statement
if and only if the user has **dictGet** privilege on that dictionary, either directly or through a role.
Available types:

* Int8
* Int16
* Int32
* Int64
* UInt8
* UInt16
* UInt32
* UInt64
* Float32
* Float64
* Date
* DateTime
* UUID
* String

#### RQ.SRS-006.RBAC.dictGet.OrDefault.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictGetOrDefault` statement
if and only if the user has **dictGet** privilege on that dictionary, either directly or through a role.

#### RQ.SRS-006.RBAC.dictHas.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictHas` statement
if and only if the user has **dictGet** privilege, either directly or through a role.

#### RQ.SRS-006.RBAC.dictGetHierarchy.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictGetHierarchy` statement
if and only if the user has **dictGet** privilege, either directly or through a role.

#### RQ.SRS-006.RBAC.dictIsIn.RequiredPrivilege
version: 1.0

[ClickHouse] SHALL successfully execute `dictIsIn` statement
if and only if the user has **dictGet** privilege, either directly or through a role.

### Introspection

#### RQ.SRS-006.RBAC.Privileges.Introspection
version: 1.0

[ClickHouse] SHALL successfully grant `INTROSPECTION` privilege when
the user is granted `INTROSPECTION` or `INTROSPECTION FUNCTIONS`.

#### RQ.SRS-006.RBAC.Privileges.Introspection.addressToLine
version: 1.0

[ClickHouse] SHALL successfully execute `addressToLine` statement if and only if
the user has **introspection** privilege, either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Introspection.addressToSymbol
version: 1.0

[ClickHouse] SHALL successfully execute `addressToSymbol` statement if and only if
the user has **introspection** privilege, either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Introspection.demangle
version: 1.0

[ClickHouse] SHALL successfully execute `demangle` statement if and only if
the user has **introspection** privilege, either directly or through a role.

### System

#### RQ.SRS-006.RBAC.Privileges.System.Shutdown
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM SHUTDOWN` privilege when
the user is granted `SYSTEM`, `SYSTEM SHUTDOWN`, `SHUTDOWN`,or `SYSTEM KILL`.

#### RQ.SRS-006.RBAC.Privileges.System.DropCache
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM DROP CACHE` privilege when
the user is granted `SYSTEM`, `SYSTEM DROP CACHE`, or `DROP CACHE`.

#### RQ.SRS-006.RBAC.Privileges.System.DropCache.DNS
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM DROP DNS CACHE` privilege when
the user is granted `SYSTEM`, `SYSTEM DROP CACHE`, `DROP CACHE`, `SYSTEM DROP DNS CACHE`,
`SYSTEM DROP DNS`, `DROP DNS CACHE`, or `DROP DNS`.

#### RQ.SRS-006.RBAC.Privileges.System.DropCache.Mark
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM DROP MARK CACHE` privilege when
the user is granted `SYSTEM`, `SYSTEM DROP CACHE`, `DROP CACHE`, `SYSTEM DROP MARK CACHE`,
`SYSTEM DROP MARK`, `DROP MARK CACHE`, or `DROP MARKS`.

#### RQ.SRS-006.RBAC.Privileges.System.DropCache.Uncompressed
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM DROP UNCOMPRESSED CACHE` privilege when
the user is granted `SYSTEM`, `SYSTEM DROP CACHE`, `DROP CACHE`, `SYSTEM DROP UNCOMPRESSED CACHE`,
`SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, or `DROP UNCOMPRESSED`.

#### RQ.SRS-006.RBAC.Privileges.System.Reload
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RELOAD` privilege when
the user is granted `SYSTEM` or `SYSTEM RELOAD`.

#### RQ.SRS-006.RBAC.Privileges.System.Reload.Config
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RELOAD CONFIG` privilege when
the user is granted `SYSTEM`, `SYSTEM RELOAD`, `SYSTEM RELOAD CONFIG`, or `RELOAD CONFIG`.

#### RQ.SRS-006.RBAC.Privileges.System.Reload.Dictionary
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RELOAD DICTIONARY` privilege when
the user is granted `SYSTEM`, `SYSTEM RELOAD`, `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARIES`, or `RELOAD DICTIONARY`.

#### RQ.SRS-006.RBAC.Privileges.System.Reload.Dictionaries
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RELOAD DICTIONARIES` privilege when
the user is granted `SYSTEM`, `SYSTEM RELOAD`, `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARIES`, or `RELOAD DICTIONARY`.

#### RQ.SRS-006.RBAC.Privileges.System.Reload.EmbeddedDictionaries
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege when
the user is granted `SYSTEM`, `SYSTEM RELOAD`, `SYSTEM RELOAD DICTIONARY ON *.*`, or `SYSTEM RELOAD EMBEDDED DICTIONARIES`.

#### RQ.SRS-006.RBAC.Privileges.System.Merges
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM MERGES` privilege when
the user is granted `SYSTEM`, `SYSTEM MERGES`, `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, or `START MERGES`.

#### RQ.SRS-006.RBAC.Privileges.System.TTLMerges
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM TTL MERGES` privilege when
the user is granted `SYSTEM`, `SYSTEM TTL MERGES`, `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, or `START TTL MERGES`.

#### RQ.SRS-006.RBAC.Privileges.System.Fetches
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM FETCHES` privilege when
the user is granted `SYSTEM`, `SYSTEM FETCHES`, `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, or `START FETCHES`.

#### RQ.SRS-006.RBAC.Privileges.System.Moves
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM MOVES` privilege when
the user is granted `SYSTEM`, `SYSTEM MOVES`, `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, or `START MOVES`.

#### RQ.SRS-006.RBAC.Privileges.System.Sends
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM SENDS` privilege when
the user is granted `SYSTEM`, `SYSTEM SENDS`, `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, or `START SENDS`.

#### RQ.SRS-006.RBAC.Privileges.System.Sends.Distributed
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM DISTRIBUTED SENDS` privilege when
the user is granted `SYSTEM`, `SYSTEM DISTRIBUTED SENDS`, `SYSTEM STOP DISTRIBUTED SENDS`,
`SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, or `START DISTRIBUTED SENDS`.

#### RQ.SRS-006.RBAC.Privileges.System.Sends.Replicated
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM REPLICATED SENDS` privilege when
the user is granted `SYSTEM`, `SYSTEM REPLICATED SENDS`, `SYSTEM STOP REPLICATED SENDS`,
`SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, or `START REPLICATED SENDS`.

#### RQ.SRS-006.RBAC.Privileges.System.ReplicationQueues
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM REPLICATION QUEUES` privilege when
the user is granted `SYSTEM`, `SYSTEM REPLICATION QUEUES`, `SYSTEM STOP REPLICATION QUEUES`,
`SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, or `START REPLICATION QUEUES`.

#### RQ.SRS-006.RBAC.Privileges.System.SyncReplica
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM SYNC REPLICA` privilege when
the user is granted `SYSTEM`, `SYSTEM SYNC REPLICA`, or `SYNC REPLICA`.

#### RQ.SRS-006.RBAC.Privileges.System.RestartReplica
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM RESTART REPLICA` privilege when
the user is granted `SYSTEM`, `SYSTEM RESTART REPLICA`, or `RESTART REPLICA`.

#### RQ.SRS-006.RBAC.Privileges.System.Flush
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM FLUSH` privilege when
the user is granted `SYSTEM` or `SYSTEM FLUSH`.

#### RQ.SRS-006.RBAC.Privileges.System.Flush.Distributed
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM FLUSH DISTRIBUTED` privilege when
the user is granted `SYSTEM`, `SYSTEM FLUSH DISTRIBUTED`, or `FLUSH DISTRIBUTED`.

#### RQ.SRS-006.RBAC.Privileges.System.Flush.Logs
version: 1.0

[ClickHouse] SHALL successfully grant `SYSTEM FLUSH LOGS` privilege when
the user is granted `SYSTEM`, `SYSTEM FLUSH LOGS`, or `FLUSH LOGS`.

### Sources

#### RQ.SRS-006.RBAC.Privileges.Sources
version: 1.0

[ClickHouse] SHALL support granting or revoking `SOURCES` privilege from
the user, either directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.File
version: 1.0

[ClickHouse] SHALL support the use of `FILE` source by a user if and only if
the user has `FILE` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.URL
version: 1.0

[ClickHouse] SHALL support the use of `URL` source by a user if and only if
the user has `URL` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.Remote
version: 1.0

[ClickHouse] SHALL support the use of `REMOTE` source by a user if and only if
the user has `REMOTE` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.MySQL
version: 1.0

[ClickHouse] SHALL support the use of `MySQL` source by a user if and only if
the user has `MySQL` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.ODBC
version: 1.0

[ClickHouse] SHALL support the use of `ODBC` source by a user if and only if
the user has `ODBC` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.JDBC
version: 1.0

[ClickHouse] SHALL support the use of `JDBC` source by a user if and only if
the user has `JDBC` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.HDFS
version: 1.0

[ClickHouse] SHALL support the use of `HDFS` source by a user if and only if
the user has `HDFS` or `SOURCES` privileges granted to them directly or through a role.

#### RQ.SRS-006.RBAC.Privileges.Sources.S3
version: 1.0

[ClickHouse] SHALL support the use of `S3` source by a user if and only if
the user has `S3` or `SOURCES` privileges granted to them directly or through a role.

### RQ.SRS-006.RBAC.Privileges.GrantOption
version: 1.0

[ClickHouse] SHALL successfully execute `GRANT` or `REVOKE` privilege statements by a user if and only if
the user has that privilege with `GRANT OPTION`, either directly or through a role.

### RQ.SRS-006.RBAC.Privileges.All
version: 1.0

[ClickHouse] SHALL support granting or revoking `ALL` privilege
using `GRANT ALL ON *.* TO user`.

### RQ.SRS-006.RBAC.Privileges.RoleAll
version: 1.0

[ClickHouse] SHALL support granting a role named `ALL` using `GRANT ALL TO user`.
This shall only grant the user the privileges that have been granted to the role.

### RQ.SRS-006.RBAC.Privileges.None
version: 1.0

[ClickHouse] SHALL support granting or revoking `NONE` privilege
using `GRANT NONE TO user` or `GRANT USAGE ON *.* TO user`.

### RQ.SRS-006.RBAC.Privileges.AdminOption
version: 1.0

[ClickHouse] SHALL support a user granting or revoking a role if and only if
the user has that role with `ADMIN OPTION` privilege.

## References

* **ClickHouse:** https://clickhouse.com
* **GitHub repository:** https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/rbac/requirements/requirements.md
* **Revision history:** https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/rbac/requirements/requirements.md
* **Git:** https://git-scm.com/
* **MySQL:** https://dev.mysql.com/doc/refman/8.0/en/account-management-statements.html
* **PostgreSQL:** https://www.postgresql.org/docs/12/user-manag.html

[ClickHouse]: https://clickhouse.com
[GitHub repository]: https://github.com/ClickHouse/ClickHouse/blob/master/tests/testflows/rbac/requirements/requirements.md
[Revision history]: https://github.com/ClickHouse/ClickHouse/commits/master/tests/testflows/rbac/requirements/requirements.md
[Git]: https://git-scm.com/
[MySQL]: https://dev.mysql.com/doc/refman/8.0/en/account-management-statements.html
[PostgreSQL]: https://www.postgresql.org/docs/12/user-manag.html
