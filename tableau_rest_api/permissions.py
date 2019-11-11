from ..tableau_base import TableauBase
from ..tableau_exceptions import *
from typing import Union, Any, Optional, List, Dict

# Represents the Permissions from any given user or group. Equivalent to GranteeCapabilities in the API
class Permissions(TableauBase):
    def __init__(self, group_or_user, luid, content_type=None):
        TableauBase.__init__(self)
        if group_or_user not in ['group', 'user']:
            raise InvalidOptionException('group_or_user must be "group" or "user"')
        self.content_type = content_type
        self.obj_type = group_or_user
        self._luid = luid
        # Get total set of capabilities, set to None by default
        self.capabilities = {}
        self.__server_to_rest_capability_map = self.server_to_rest_capability_map
        self.__allowable_modes = ['Allow', 'Deny', None]
        self.role_set = {
            'Publisher': {
                'all': 'Allow',
                'Connect': None,
                'Download': None,
                'Move': None,
                'Delete': None,
                'Set Permissions': None,
                'Project Leader': None,
             },
            'Interactor': {
                'all': True,
                'Connect': None,
                'Download': None,
                'Move': None,
                'Delete': None,
                'Set Permissions': None,
                'Project Leader': None,
                'Save': None
            },
            'Viewer': {
                'View': 'Allow',
                'Export Image': 'Allow',
                'View Summary Data': 'Allow',
                'View Comments': 'Allow',
                'Add Comment': 'Allow'
            },
            'Editor': {
                'all': True,
                'Connect': None,
                'Project Leader': None
            },
            'Data Source Connector': {
                'all': None,
                'Connect': None,
                'Project Leader': None
            },
            'Data Source Editor': {
                'all': None,
                'View': 'Allow',
                'Connect': 'Allow',
                'Save': 'Allow',
                'Download': 'Allow',
                'Delete': 'Allow',
                'Set Permissions': 'Allow'
            },
            'Project Leader': {
                'all': None,
                'Project Leader': 'Allow'
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
        if group_or_user not in ['group', 'user']:
            raise InvalidOptionException('Must set to either group or user')
        self.obj_type = group_or_user

    # Just use the direct "to_allow" and "to_deny" methods
    def set_capability(self, capability_name: str, mode: str):
        if capability_name not in list(self.__server_to_rest_capability_map.values()):
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                # InheritedProjectLeader (2.8+) is Read-Only
                if capability_name == 'InheritedProjectLeader':
                    self.log('InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != 'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = mode

    def set_capability_to_allow(self, capability_name: str):
        self.set_capability(capability_name=capability_name, mode="Allow")

    def set_capability_to_deny(self, capability_name: str):
        self.set_capability(capability_name=capability_name, mode="Deny")

    def set_capability_to_unspecified(self, capability_name: str):
        if capability_name not in self.capabilities:
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name == 'InheritedProjectLeader':
                    self.log('InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != 'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = None

    # This exists specifically to allow the setting of read-only permissions
    def _set_capability_from_published_content(self, capability_name: str, mode: str):
        if capability_name not in list(self.__server_to_rest_capability_map.values()):
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != 'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = mode

    def get_capabilities_dict(self) -> Dict:
        return self.capabilities

    def get_content_type(self) -> str:

        return self.content_type

    def set_all_to_deny(self):
        for cap in self.capabilities:
            if cap != 'all':
                self.capabilities[cap] = 'Deny'

    def set_all_to_allow(self):
        for cap in self.capabilities:
            if cap == 'InheritedProjectLeader':
                continue
            if cap != 'all':
                self.capabilities[cap] = 'Allow'

    def set_all_to_unspecified(self):
        for cap in self.capabilities:
            if cap == 'InheritedProjectLeader':
                continue
            if cap != 'all':
                self.capabilities[cap] = None

    def set_capabilities_to_match_role(self, role: str):
        if role not in self.role_set:
            raise InvalidOptionException('{} is not a recognized role'.format(role))

        # Clear any previously set capabilities
        self.set_all_to_unspecified()

        role_capabilities = self.role_set[role]
        self.log_debug("Setting to role {} with capabilities {}".format(role, str(role_capabilities)))
        if "all" in role_capabilities:
            if role_capabilities["all"] == 'Allow':
                self.log_debug("Setting all capabilities to Allow")
                self.set_all_to_allow()
            elif role_capabilities["all"] == 'Deny':
                self.log_debug("Setting all capabilities to Deny")
                self.set_all_to_deny()
        for cap in role_capabilities:
            # Skip the all command, we handled it at the beginning
            if cap == 'all':
                continue
            elif role_capabilities[cap] is not None:
                self.set_capability(cap, role_capabilities[cap])
            elif role_capabilities[cap] is None:
                self.set_capability_to_unspecified(cap)


class WorkbookPermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'workbook')
        for cap in self.available_capabilities['2.8']['workbook']:
            if cap != 'all':
                self.capabilities[cap] = None
        self.role_set = {
                    "Viewer": {
                        'all': None,
                        'View': 'Allow',
                        'Export Image': 'Allow',
                        'View Summary Data': 'Allow',
                        'View Comments': 'Allow',
                        'Add Comment': 'Allow'
                    },
                    "Interactor": {
                        'all': 'Allow',
                        'Download': None,
                        'Move': None,
                        'Delete': None,
                        'Set Permissions': None,
                        'Save': None
                    },
                    "Editor": {
                        'all': 'Allow'
                    }
                }

class ProjectPermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'project')
        for cap in self.available_capabilities['2.8']['project']:
            if cap != 'all':
                self.capabilities[cap] = None
        self.role_set = {
            "Viewer": {
                'all': None,
                "View": "Allow"
            },
            "Publisher": {
                'all': None,
                "View": "Allow",
                "Save": "Allow"
            },
            "Project Leader": {
                'all': None,
                "Project Leader": "Allow"
            }
        }


class DatasourcePermissions28(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'datasource')
        for cap in self.available_capabilities['2.8']['datasource']:
            if cap != 'all':
                self.capabilities[cap] = None
        self.role_set = {
            "Connector": {
                'all': None,
                'View': 'Allow',
                'Connect': 'Allow'
            },
            "Editor": {
                'all': 'Allow'
            }
        }


class FlowPermissions33(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'flow')
        for cap in self.available_capabilities['3.3']['flow']:
            if cap != 'all':
                self.capabilities[cap] = None
        # Unclear that there are any defined roles for Prep Conductor flows
        self.role_set = {}