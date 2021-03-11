from typing import Union, Any, Optional, List, Dict

from tableau_tools.logging_methods import LoggingMethods
from tableau_tools.tableau_exceptions import *
import xml.etree.ElementTree as ET

# Represents the Permissions from any given user or group. Equivalent to GranteeCapabilities in the API
class Permissions(LoggingMethods):
    capabilities_2_1 = {
        "project": ("Read", "Write", 'ProjectLeader'),
        "workbook": (
            'Read',
            'ExportImage',
            'ExportData',
            'ViewComments',
            'AddComment',
            'Filter',
            'ViewUnderlyingData',
            'ShareView',
            'WebAuthoring',
            'Write',
            'ExportXml',
            'ChangeHierarchy',
            'Delete',
            'ChangePermissions',

        ),
        "datasource": (
            'Read',
            'Connect',
            'Write',
            'ExportXml',
            'Delete',
            'ChangePermissions'
        )
    }

    capabilities_2_8 = {
        "project": ("Read", "Write", 'ProjectLeader', 'InheritedProjectLeader'),
        "workbook": (
            'Read',
            'ExportImage',
            'ExportData',
            'ViewComments',
            'AddComment',
            'Filter',
            'ViewUnderlyingData',
            'ShareView',
            'WebAuthoring',
            'Write',
            'ExportXml',
            'ChangeHierarchy',
            'Delete',
            'ChangePermissions',

        ),
        "datasource": (
            'Read',
            'Connect',
            'Write',
            'ExportXml',
            'Delete',
            'ChangePermissions'
        )
    }

    capabilities_3_3 = {
        "project": ("Read", "Write", 'ProjectLeader', 'InheritedProjectLeader'),
        "workbook": (
            'Read',
            'ExportImage',
            'ExportData',
            'ViewComments',
            'AddComment',
            'Filter',
            'ViewUnderlyingData',
            'ShareView',
            'WebAuthoring',
            'Write',
            'ExportXml',
            'ChangeHierarchy',
            'Delete',
            'ChangePermissions',

        ),
        "datasource": (
            'Read',
            'Connect',
            'Write',
            'ExportXml',
            'Delete',
            'ChangePermissions'
        ),
        'flow': (
            'ChangeHierarchy',
            'ChangePermissions',
            'Delete',
            'ExportXml',
            'Read',
            'Write'
        )
    }

    capabilities_3_5 = {
        "project": ("Read", "Write", 'ProjectLeader', 'InheritedProjectLeader'),
        "workbook": (
            'Read',
            'ExportImage',
            'ExportData',
            'ViewComments',
            'AddComment',
            'Filter',
            'ViewUnderlyingData',
            'ShareView',
            'WebAuthoring',
            'Write',
            'ExportXml',
            'ChangeHierarchy',
            'Delete',
            'ChangePermissions',

        ),
        "datasource": (
            'Read',
            'Connect',
            'Write',
            'ExportXml',
            'Delete',
            'ChangePermissions'
        ),
        'flow': (
            'ChangeHierarchy',
            'ChangePermissions',
            'Delete',
            'ExportXml',
            'Read',
            'Write'
        )
    }

    available_capabilities = {
        '2.6': capabilities_2_1,
        '2.7': capabilities_2_1,
        '2.8': capabilities_2_8,
        '3.0': capabilities_2_8,
        '3.1': capabilities_2_8,
        '3.2': capabilities_2_8,
        '3.3': capabilities_3_3,
        '3.4': capabilities_3_3,
        '3.5': capabilities_3_5,
        '3.6': capabilities_3_5

    }

    def __init__(self, group_or_user: str, luid: str, content_type: Optional[str] = None):
        if group_or_user not in ['group', 'user']:
            raise InvalidOptionException('group_or_user must be "group" or "user"')
        self.content_type = content_type
        self.obj_type = group_or_user
        self._luid = luid
        # Get total set of capabilities, set to None by default
        self.capabilities = {}
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



        self.site_roles = (
            'Interactor',
            'Publisher',
            'SiteAdministrator',
            'Unlicensed',
            'UnlicensedWithPublish',   # This was sunset at some point
            'Viewer',
            'ViewerWithPublish',
            'ServerAdministrator',
            'ReadOnly',
            'Explorer',
            'ExplorerCanPublish',
            'SiteAdministratorExplorer',
            'Creator',
            'SiteAdministratorCreator'
        )

        server_content_roles_2_1 = {
                "project": (
                    'Viewer',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Editor',
                    'Connector'
                )
            }

        server_content_roles_3_3 = {
                "project": (
                    'Viewer',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Editor',
                    'Connector'
                ),
                "flow" : (

                )
            }

        server_content_roles_3_5 = {
            "project": (
                'Viewer',
                'Publisher',
                'Project Leader'
            ),
            "workbook": (
                'Viewer',
                'Interactor',
                'Editor'
            ),
            "datasource": (
                'Editor',
                'Connector'
            ),
            "flow": (

            ),
            "database": (),
            "table" : ()
        }

        self.server_content_roles = {
            "2.6": server_content_roles_2_1,
            "2.7": server_content_roles_2_1,
            "2.8": server_content_roles_2_1,
            '3.0': server_content_roles_2_1,
            '3.1': server_content_roles_2_1,
            '3.2': server_content_roles_2_1,
            '3.3': server_content_roles_3_3,
            '3.4': server_content_roles_3_3,
            '3.5': server_content_roles_3_5,
            '3.6': server_content_roles_3_5
        }

        self.server_to_rest_capability_map = {
            'Add Comment': 'AddComment',
            'Move': 'ChangeHierarchy',
            'Set Permissions': 'ChangePermissions',
            'Connect': 'Connect',
            'Delete': 'Delete',
            'View Summary Data': 'ExportData',
            'Download Summary Data': 'ExportData',
            'Export Image': 'ExportImage',
            'Download Image/PDF': 'ExportImage',
            'Download': 'ExportXml',
            'Download Workbook/Save As': 'ExportXml',
            'Filter': 'Filter',
            'Project Leader': 'ProjectLeader',
            'View': 'Read',
            'Share Customized': 'ShareView',
            'View Comments': 'ViewComments',
            'View Underlying Data': 'ViewUnderlyingData',
            'Download Full Data' : 'ViewUnderlyingData',
            'Web Edit': 'WebAuthoring',
            'Save': 'Write',
            'Inherited Project Leader': 'InheritedProjectLeader',
            'all': 'all'  # special command to do everything
        }



    def convert_server_permission_name_to_rest_permission(self, permission_name: str) -> str:
        if permission_name in self.server_to_rest_capability_map:
            return self.server_to_rest_capability_map[permission_name]
        else:
            raise InvalidOptionException('{} is not a permission name on the Tableau Server'.format(permission_name))

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
        if capability_name not in list(self.server_to_rest_capability_map.values()):
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.server_to_rest_capability_map:
                # InheritedProjectLeader (2.8+) is Read-Only
                if capability_name == 'InheritedProjectLeader':
                    self.log('InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != 'all':
                    capability_name = self.server_to_rest_capability_map[capability_name]
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
            if capability_name in self.server_to_rest_capability_map:
                if capability_name == 'InheritedProjectLeader':
                    self.log('InheritedProjectLeader permission is read-only, skipping')
                    return
                if capability_name != 'all':
                    capability_name = self.server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException('"{}" is not a capability in REST API or Server'.format(capability_name))
        self.capabilities[capability_name] = None

    # This exists specifically to allow the setting of read-only permissions
    def _set_capability_from_published_content(self, capability_name: str, mode: str):
        if capability_name not in list(self.server_to_rest_capability_map.values()):
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.server_to_rest_capability_map:
                if capability_name != 'all':
                    capability_name = self.server_to_rest_capability_map[capability_name]
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

class WorkbookPermissions(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'workbook')
        for cap in self.available_capabilities[u'2.6'][u'workbook']:
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
    def __init__(self, group_or_user: str, group_or_user_luid: str):
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

class ProjectPermissions(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'project')
        for cap in self.available_capabilities[u'2.6'][u'project']:
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
    def __init__(self, group_or_user: str, group_or_user_luid: str):
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

class DatasourcePermissions(Permissions):
    def __init__(self, group_or_user, group_or_user_luid):
        Permissions.__init__(self, group_or_user, group_or_user_luid, u'datasource')
        for cap in self.available_capabilities[u'2.6'][u'datasource']:
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
    def __init__(self, group_or_user: str, group_or_user_luid: str):
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
    def __init__(self, group_or_user: str, group_or_user_luid: str):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'flow')
        for cap in self.available_capabilities['3.3']['flow']:
            if cap != 'all':
                self.capabilities[cap] = None
        # Unclear that there are any defined roles for Prep Conductor flows
        self.role_set = {}

class DatabasePermissions35(Permissions):
    def __init__(self, group_or_user: str, group_or_user_luid: str):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'database')
        for cap in self.available_capabilities['3.5']['database']:
            if cap != 'all':
                self.capabilities[cap] = None
        # No idea what roles might exist for 'databases' or 'tables'
        self.role_set = {}

class TablePermissions35(Permissions):
    def __init__(self, group_or_user: str, group_or_user_luid: str):
        Permissions.__init__(self, group_or_user, group_or_user_luid, 'table')
        for cap in self.available_capabilities['3.5']['table']:
            if cap != 'all':
                self.capabilities[cap] = None
        # No idea what roles might exist for 'databases' or 'tables'
        self.role_set = {}