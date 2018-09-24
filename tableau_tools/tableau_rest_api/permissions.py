from ..tableau_base import TableauBase
from ..tableau_exceptions import *


# Represents the Permissions from any given user or group. Equivalent to GranteeCapabilities in the API
class Permissions(TableauBase):
    def __init__(self, group_or_user, luid, content_type=None):
        TableauBase.__init__(self)
        if group_or_user not in [u'group', u'user']:
            raise InvalidOptionException(u'group_or_user must be "group" or "user"')
        self.content_type = content_type
        self.obj_type = group_or_user
        self._luid = luid
        # Get total set of capabilities, set to None by default
        self.capabilities = {}
        self.__server_to_rest_capability_map = self.server_to_rest_capability_map
        self.__allowable_modes = [u'Allow', u'Deny', None]
        self.role_set = {
            u'Publisher': {
                u'all': u'Allow',
                u'Connect': None,
                u'Download': None,
                u'Move': None,
                u'Delete': None,
                u'Set Permissions': None,
                u'Project Leader': None,
             },
            u'Interactor': {
                u'all': True,
                u'Connect': None,
                u'Download': None,
                u'Move': None,
                u'Delete': None,
                u'Set Permissions': None,
                u'Project Leader': None,
                u'Save': None
            },
            u'Viewer': {
                u'View': u'Allow',
                u'Export Image': u'Allow',
                u'View Summary Data': u'Allow',
                u'View Comments': u'Allow',
                u'Add Comment': u'Allow'
            },
            u'Editor': {
                u'all': True,
                u'Connect': None,
                u'Project Leader': None
            },
            u'Data Source Connector': {
                u'all': None,
                u'Connect': None,
                u'Project Leader': None
            },
            u'Data Source Editor': {
                u'all': None,
                u'View': u'Allow',
                u'Connect': u'Allow',
                u'Save': u'Allow',
                u'Download': u'Allow',
                u'Delete': u'Allow',
                u'Set Permissions': u'Allow'
            },
            u'Project Leader': {
                u'all': None,
                u'Project Leader': u'Allow'
            }
        }

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, new_luid):
        self._luid = new_luid

    @property
    def group_or_user(self):
        """
        :return Either group or user
        :rtype: unicode
        """
        return self.obj_type

    @group_or_user.setter
    def group_or_user(self, group_or_user):
        """
        :param group_or_user: Either group or user
        :type: group_or_user: unicode
        """
        if group_or_user not in [u'group', u'user']:
            raise InvalidOptionException(u'Must set to either group or user')
        self.obj_type = group_or_user

    def set_capability(self, capability_name, mode):
        """
        :param capability_name: You can input the names from the REST API or what you see in Tableau Server
        :type capability_name: unicode
        :param mode: Can only be Allow or Deny. Use set_capability_to_unspecified to set to Unspecified
        :type mode: unicode
        :return:
        """

        if capability_name not in self.__server_to_rest_capability_map.values():
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                # InheritedProjectLeader (2.8+) is Read-Only
                if capability_name == u'InheritedProjectLeader':
                    self.log(u'InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != u'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException(u'"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = mode

    # This exists specifically to allow the setting of read-only permissions
    def _set_capability_from_published_content(self, capability_name, mode):
        """
        :param capability_name: You can input the names from the REST API or what you see in Tableau Server
        :type capability_name: unicode
        :param mode: Can only be Allow or Deny. Use set_capability_to_unspecified to set to Unspecified
        :type mode: unicode
        :return:
        """

        if capability_name not in self.__server_to_rest_capability_map.values():
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != u'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException(u'"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = mode


    def set_capability_to_unspecified(self, capability_name):
        """
        :param capability_name: You can input the names from the REST API or what you see in Tableau Server
        :type capability_name: unicode
        :return:
        """
        if capability_name not in self.capabilities:
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name == u'InheritedProjectLeader':
                    self.log(u'InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != u'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException(u'"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = None

    def get_capabilities_dict(self):
        """
        :rtype: dict
        """
        return self.capabilities

    def get_content_type(self):
        """
        :return: Will be 'project', 'workbook' or 'datasource'
        :rtype unicode
        """
        return self.content_type

    def set_all_to_deny(self):
        """
        :return:
        """
        for cap in self.capabilities:
            if cap != u'all':
                self.capabilities[cap] = u'Deny'

    def set_all_to_allow(self):
        """
        :return:
        """
        for cap in self.capabilities:
            if cap == u'InheritedProjectLeader':
                continue
            if cap != u'all':
                self.capabilities[cap] = u'Allow'

    def set_all_to_unspecified(self):
        """
        :return:
        """
        for cap in self.capabilities:
            if cap == u'InheritedProjectLeader':
                continue
            if cap != u'all':
                self.capabilities[cap] = None

    def set_capabilities_to_match_role(self, role):
        """
        :param role: One of the named Roles you see in the Tableau Server UI when setting permissions
        :type role: unicode
        :return:
        """
        if role not in self.role_set:
            raise InvalidOptionException(u'{} is not a recognized role'.format(role))

        # Clear any previously set capabilities
        self.set_all_to_unspecified()

        role_capabilities = self.role_set[role]
        self.log_debug(u"Setting to role {} with capabilities {}".format(role, unicode(role_capabilities)))
        if u"all" in role_capabilities:
            if role_capabilities[u"all"] == u'Allow':
                self.log_debug(u"Setting all capabilities to Allow")
                self.set_all_to_allow()
            elif role_capabilities[u"all"] == u'Deny':
                self.log_debug(u"Setting all capabilities to Deny")
                self.set_all_to_deny()
        for cap in role_capabilities:
            # Skip the all command, we handled it at the beginning
            if cap == u'all':
                continue
            elif role_capabilities[cap] is not None:
                self.set_capability(cap, role_capabilities[cap])
            elif role_capabilities[cap] is None:
                self.set_capability_to_unspecified(cap)


class WorkbookPermissions20(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'workbook')
        for cap in self.available_capabilities[u'2.0'][u'workbook']:
            if cap != u'all':
                self.capabilities[cap] = None


class WorkbookPermissions21(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'workbook')
        for cap in self.available_capabilities[u'2.1'][u'workbook']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
                    u"Viewer": {
                        u'all': None,
                        u'View': u'Allow',
                        u'Export Image': u'Allow',
                        u'View Summary Data': u'Allow',
                        u'View Comments': u'Allow',
                        u'Add Comment': u'Allow'
                    },
                    u"Interactor": {
                        u'all': u'Allow',
                        u'Download': None,
                        u'Move': None,
                        u'Delete': None,
                        u'Set Permissions': None,
                        u'Save': None
                    },
                    u"Editor": {
                        u'all': u'Allow'
                    }
                }


class WorkbookPermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'workbook')
        for cap in self.available_capabilities[u'2.8'][u'workbook']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
                    u"Viewer": {
                        u'all': None,
                        u'View': u'Allow',
                        u'Export Image': u'Allow',
                        u'View Summary Data': u'Allow',
                        u'View Comments': u'Allow',
                        u'Add Comment': u'Allow'
                    },
                    u"Interactor": {
                        u'all': u'Allow',
                        u'Download': None,
                        u'Move': None,
                        u'Delete': None,
                        u'Set Permissions': None,
                        u'Save': None
                    },
                    u"Editor": {
                        u'all': u'Allow'
                    }
                }

class ProjectPermissions20(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'project')
        for cap in self.available_capabilities[u'2.0'][u'project']:
            if cap != u'all':
                self.capabilities[cap] = None


class ProjectPermissions21(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'project')
        for cap in self.available_capabilities[u'2.1'][u'project']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
            u"Viewer": {
                u'all': None,
                u"View": u"Allow"
            },
            u"Publisher": {
                u'all': None,
                u"View": u"Allow",
                u"Save": u"Allow"
            },
            u"Project Leader": {
                u'all': None,
                u"Project Leader": u"Allow"
            }
        }


class ProjectPermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'project')
        for cap in self.available_capabilities[u'2.8'][u'project']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
            u"Viewer": {
                u'all': None,
                u"View": u"Allow"
            },
            u"Publisher": {
                u'all': None,
                u"View": u"Allow",
                u"Save": u"Allow"
            },
            u"Project Leader": {
                u'all': None,
                u"Project Leader": u"Allow"
            }
        }


class DatasourcePermissions20(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'datasource')
        for cap in self.available_capabilities[u'2.0'][u'datasource']:
            if cap != u'all':
                self.capabilities[cap] = None


class DatasourcePermissions21(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'datasource')
        for cap in self.available_capabilities[u'2.1'][u'datasource']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
            u"Connector": {
                u'all': None,
                u'View': u'Allow',
                u'Connect': u'Allow'
            },
            u"Editor": {
                u'all': u'Allow'
            }
        }


class DatasourcePermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'datasource')
        for cap in self.available_capabilities[u'2.8'][u'datasource']:
            if cap != u'all':
                self.capabilities[cap] = None
        self.role_set = {
            u"Connector": {
                u'all': None,
                u'View': u'Allow',
                u'Connect': u'Allow'
            },
            u"Editor": {
                u'all': u'Allow'
            }
        }
