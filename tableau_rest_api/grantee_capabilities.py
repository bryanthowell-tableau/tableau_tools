from ..tableau_base import TableauBase
from ..tableau_exceptions import *


# Represents the GranteeCapabilities from any given.
class GranteeCapabilities(TableauBase):
    def __init__(self, obj_type, luid, content_type=None, tableau_server_version=u"9.2"):
        super(self.__class__, self).__init__()
        self.set_tableau_server_version(tableau_server_version)
        if obj_type not in [u'group', u'user']:
            raise InvalidOptionException(u'GranteeCapabilites type must be "group" or "user"')
        self.content_type = content_type
        self.obj_type = obj_type
        self.luid = luid
        # Get total set of capabilities, set to None by default
        self.__capabilities = {}
        self.__server_to_rest_capability_map = self.server_to_rest_capability_map
        self.__allowable_modes = [u'Allow', u'Deny', None]
        if content_type is not None:
            # Defined in TableauBase superclass
            self.__role_map = self.server_content_roles[self.api_version][content_type]
            for cap in self.available_capabilities[self.api_version][content_type]:
                if cap != u'all':
                    self.__capabilities[cap] = None

    def set_capability(self, capability_name, mode):
        if capability_name not in self.__server_to_rest_capability_map.values():
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != u'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException(u'"{}" is not a capability in REST API or Server'.format(capability_name))
        self.__capabilities[capability_name] = mode

    def set_capability_to_unspecified(self, capability_name):
        if capability_name not in self.__capabilities:
            # If it's the Tableau UI naming, translate it over
            if capability_name in self.__server_to_rest_capability_map:
                if capability_name != u'all':
                    capability_name = self.__server_to_rest_capability_map[capability_name]
            else:
                raise InvalidOptionException(u'"{}" is not a capability in REST API or Server'.format(capability_name))
        self.__capabilities[capability_name] = None

    def get_capabilities_dict(self):
        return self.__capabilities

    def get_obj_type(self):
        return self.obj_type

    def get_luid(self):
        return self.luid

    def set_obj_type(self, obj_type):
        if obj_type.lower() in [u'group', u'user']:
            self.obj_type = obj_type.lower()
        else:
            raise InvalidOptionException(u'obj_type can only be "group" or "user"')

    def set_luid(self, new_luid):
        self.luid = new_luid

    def set_all_to_deny(self):
        for cap in self.__capabilities:
            if cap != u'all':
                self.__capabilities[cap] = u'Deny'

    def set_all_to_allow(self):
        for cap in self.__capabilities:
            if cap != u'all':
                self.__capabilities[cap] = u'Allow'

    def set_all_to_unspecified(self):
        for cap in self.__capabilities:
            if cap != u'all':
                self.__capabilities[cap] = None

    def set_capabilities_to_match_role(self, role):
        if role not in self.__role_map:
            raise InvalidOptionException(u'{} is not a recognized role'.format(role))

        # Clear any previously set capabilities
        self.__capabilities = {}

        role_set_91_and_earlier_all_types = {
            u'Publisher': {
                u'all': True,
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

        role_set_92 = {
            u"project": {
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
            },
            u"workbook": {
                u"Viewer": {
                    u'all': None,
                    u'View': u'Allow',
                    u'Export Image': u'Allow',
                    u'View Summary Data': u'Allow',
                    u'View Comments': u'Allow',
                    u'Add Comment': u'Allow'
                },
                u"Interactor": {
                    u'all': True,
                    u'Download': None,
                    u'Move': None,
                    u'Delete': None,
                    u'Set Permissions': None,
                    u'Save': None
                },
                u"Editor": {
                    u'all': True
                }
            },
            u"datasource": {
                u"Connector": {
                    u'all': None,
                    u'View': u'Allow',
                    u'Connect': u'Allow'
                },
                u"Editor": {
                    u'all': True
                }
            }
        }

        role_set = {
            u'2.0': {
                u"project": role_set_91_and_earlier_all_types,
                u"workbook": role_set_91_and_earlier_all_types,
                u"datasource": role_set_91_and_earlier_all_types
            },
            u'2.1': role_set_92,
            u'2.2': role_set_92,
            u'2.3': role_set_92
        }
        if role not in role_set[self.api_version][self.content_type]:
            raise InvalidOptionException(u"There is no role in Tableau Server available for {} called {}".format(
                self.content_type, role
            ))
        role_capabilities = role_set[self.api_version][self.content_type][role]
        if u"all" in role_capabilities:
            if role_capabilities[u"all"] is True:
                self.set_all_to_allow()
            elif role_capabilities[u"all"] is False:
                self.set_all_to_deny()
        for cap in role_capabilities:
            # Skip the all command, we handled it at the beginning
            if cap == u'all':
                continue
            elif role_capabilities[cap] is not None:
                self.set_capability(cap, role_capabilities[cap])
            elif role_capabilities[cap] is None:
                self.set_capability_to_unspecified(cap)
