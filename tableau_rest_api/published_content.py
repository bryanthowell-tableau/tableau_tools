
from .permissions import *
import copy
from typing import Union, Any, Optional, List, Dict, TYPE_CHECKING

from ..tableau_rest_xml import TableauRestXml

if TYPE_CHECKING:
    from tableau_tools.logging_methods import LoggingMethods
    from tableau_tools.logger import Logger
    from tableau_tools.tableau_exceptions import *
    from tableau_tools.tableau_rest_api_connection import TableauRestApiConnection
    from tableau_tools.tableau_server_rest import TableauServerRest

# Represents a published workbook, project or datasource
class PublishedContent(LoggingMethods):
    def __init__(self, luid: str, obj_type: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 default: bool = False, logger_obj: Optional['Logger'] = None,
                 content_xml_obj: Optional[ET.Element] = None):

        self.permissionable_objects = ('datasource', 'project', 'workbook', 'flow', 'database', 'table')
        self.logger = logger_obj
        self._luid = luid
        self.t_rest_api: Union[TableauRestApiConnection, TableauServerRest] = tableau_rest_api_obj
        self.obj_type = obj_type
        self.default = default
        self.obj_perms_xml = None
        self.current_perms_obj_list: Optional[List[Permissions]] = None
        self.__permissionable_objects = self.permissionable_objects
        self.get_permissions_from_server()
        #self.log('Creating a Published Project Object from this XML:')
        #self.log_xml_response(content_xml_obj)
        self.api_version = tableau_rest_api_obj.api_version
        self.permissions_object_class = ProjectPermissions  # Override in any child class with specific
        self.xml_obj = content_xml_obj
        # If you want to know the name that matches to the group or user, need these
        # But no need to request every single time
        # self.groups_dict_cache = None
        # self.users_dict_cache = None

    @property
    def luid(self):
        return self._luid

    # Implement in each type for the right lookup
    @luid.setter
    def luid(self, name_or_luid):
        self._luid = name_or_luid

    def get_object_type(self):
        return self.obj_type

    def get_xml_obj(self):
        return self.xml_obj

    def _get_permissions_object(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None, permissions_class_override=None):
        if group_name_or_luid is None and username_or_luid is None:
            raise InvalidOptionException('Must pass either group_name_or_luid or username_or_luid')
        if group_name_or_luid is not None and username_or_luid is not None:
            raise InvalidOptionException('Please only use one of group_name_or_luid or username_or_luid')

        if group_name_or_luid is not None:
            luid = self.t_rest_api.query_group_luid(group_name_or_luid)
        elif username_or_luid is not None:
            luid = self.t_rest_api.query_user_luid(username_or_luid)
        else:
            raise InvalidOptionException('Please pass in one of group_name_or_luid or username_or_luid')
        # This is just for compatibility
        if permissions_class_override is not None:
            perms_obj = permissions_class_override(group_or_user='group', group_or_user_luid=luid)
        else:
            perms_obj = self.permissions_object_class(group_or_user='group', group_or_user_luid=luid)
        perms_obj.enable_logging(self.logger)
        if role is not None:
            perms_obj.set_capabilities_to_match_role(role)
        return perms_obj

    # This is an abstract method to be implemented in each one
    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None):
        pass

   # def get_users_dict(self):
   #     users = self.t_rest_api.query_users()
   #     users_dict = self.t_rest_api.convert_xml_list_to_name_id_dict(users)
   #     return users_dict

    # Copy Permissions for users or group
    def _copy_permissions_obj(self, perms_obj, user_or_group, name_or_luid):
        self.start_log_block()
        if TableauRestXml.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            if user_or_group == 'group':
                luid = self.t_rest_api.query_group_luid(name_or_luid)
            elif user_or_group == 'user':
                luid = self.t_rest_api.query_user_luid(name_or_luid)
            else:
                raise InvalidOptionException('Must send group or user only')
        new_perms_obj = copy.deepcopy(perms_obj)
        new_perms_obj.luid = luid
        self.end_log_block()
        return new_perms_obj

    def copy_permissions_obj(self, perms_obj, group_name_or_luid: Optional[str] = None,
                             username_or_luid: Optional[str] = None):
        if group_name_or_luid is not None and username_or_luid is not None:
            raise InvalidOptionException('Only pass either group_name_or_luid or username_or_luid, but not both')
        if group_name_or_luid is not None:
            return self._copy_permissions_obj(perms_obj, 'group', group_name_or_luid)
        elif username_or_luid is not None:
            return self._copy_permissions_obj(perms_obj, 'user', username_or_luid)
        else:
            raise InvalidOptionException('Must pass one of group_name_or_luid or username_or_luid')

    # Legacy for compatibility
    def copy_permissions_obj_for_group(self, perms_obj, group_name_or_luid):
        return self._copy_permissions_obj(perms_obj, 'group', group_name_or_luid)

    def copy_permissions_obj_for_user(self, perms_obj, username_or_luid):
        return self._copy_permissions_obj(perms_obj, 'user', username_or_luid)



    # Runs through the gcap object list, and tries to do a conversion all principals to matching LUIDs on current site
    # Use case is replicating settings from one site to another
    # Orig_site must be TableauRestApiConnection
    # Not Finished
    def convert_permissions_obj_list_from_orig_site_to_current_site(self, permissions_obj_list: List['Permissions'],
                                                                    orig_site: Union['TableauRestApiConnection', 'TableauServerRest']) -> List['Permissions']:
        """
        :type permissions_obj_list: list[Permissions]
        :type orig_site: TableauRestApiConnection
        :rtype: list[Permissions]
        """
        # If the site is the same, skip the whole thing and just return the original
        if self.t_rest_api.site_content_url == orig_site.site_content_url \
                and self.t_rest_api.server == orig_site.server:
            return permissions_obj_list

        new_perms_obj_list = copy.deepcopy(permissions_obj_list)
        final_perms_obj_list = []
        # Make this more efficient -- should only look up those users it needs to. Question on algorithm for speed

        for perms_obj in new_perms_obj_list:
            orig_luid = perms_obj.luid
            if perms_obj.group_or_user == 'group':
                # Find the name that matches the LUID
                try:
                    orig_name = orig_site.query_group_name(orig_luid)
                    self.log('Found orig luid {} for name {}'.format(orig_luid, orig_name ))
                    n_luid = self.t_rest_api.query_group_luid(orig_name)
                    self.log('Found new luid {} for name {}'.format(n_luid, orig_name ))
                except StopIteration:
                    self.log("No matching name for luid {} found on the original site, dropping from list".format(
                        orig_luid))

            elif perms_obj.group_or_user == 'user':
                # Find the name that matches the LUID
                try:
                    # Individual searches here. Efficient in versions with lookup
                    orig_user = orig_site.query_user(orig_luid)
                    orig_username = orig_user.get('name')
                    n_luid = self.t_rest_api.query_user_luid(orig_username)
                except NoMatchFoundException:
                    self.log("No matching name for luid {} found on the original site, dropping from list".format(
                        orig_luid))
            perms_obj.luid = n_luid
            final_perms_obj_list.append(copy.deepcopy(perms_obj))
        return final_perms_obj_list

    # Runs through the gcap object list, and tries to do a conversion all principals to matching LUIDs on current site
    # Use case is replicating settings from one site to another
    # Orig_site must be TableauRestApiConnection
    # Not Finished
    def convert_permissions_xml_object_from_orig_site_to_current_site(self, permissions_xml_request, orig_site,
                                                                      username_map=None):
        """
        :type permissions_xml_request: ET.Element
        :type orig_site: TableauRestApiConnection
        :type username_map: dict[unicode, unicode]
        :rtype: ET.Element
        """
        # If the site is the same, skip the whole thing and just return the original
        if self.t_rest_api.site_content_url == orig_site.site_content_url \
                and self.t_rest_api.server == orig_site.server:
            return permissions_xml_request

        # new_perms_obj_list = permissions_xml_request
        final_perms_obj_list = []
        # Make this more efficient -- should only look up those users it needs to. Question on algorithm for speed

        # Must loop two levels deep
        for permissions_element in permissions_xml_request:
            for grantee_capabilities in permissions_element:
                # Handle groups first
                for group in grantee_capabilities.iter('group'):

                    orig_luid = group.get('id')
                    # Find the name that matches the LUID
                    try:
                        orig_name = orig_site.query_group_name(orig_luid)
                        self.log('Found orig luid {} for name {}'.format(orig_luid, orig_name ))
                        n_luid = self.t_rest_api.query_group_luid(orig_name)
                        self.log('Found new luid {} for name {}'.format(n_luid, orig_name ))
                        # Update the attribute for the new site
                        group.set('id', n_luid)
                    except StopIteration:
                        self.log("No matching name for luid {} found on the original site, dropping from list".format(
                            orig_luid))
                        # Here, do we remove a group that doesn't exist on the new site? Or just raise that exception?
                        # Going with remove for now, to make things more smooth
                        permissions_element.remove(grantee_capabilities)

                # This works if you anticipate that users will have the same name. Should add an optional dictionary for
                # translating
                for user in grantee_capabilities.iter('user'):
                    orig_luid = user.get('id')
                    # Find the name that matches the LUID
                    try:
                        # Individual searches here. Efficient in versions with lookup
                        orig_user = orig_site.query_user(orig_luid)
                        orig_username = orig_user.get('name')
                        final_username = orig_username
                        if username_map is not None:
                            if orig_username in username_map:
                                final_username = username_map[orig_username]
                            else:
                                self.log("No matching name in the username_map found for original_name '{}' found , dropping from list".format(
                                        orig_username))
                                permissions_element.remove(grantee_capabilities)
                        else:
                            final_username = orig_username
                        n_luid = self.t_rest_api.query_user_luid(final_username)
                        user.set('id', n_luid)
                    except NoMatchFoundException:
                        self.log("No matching name for luid {} found on the original site, dropping from list".format(
                            orig_luid))
                        permissions_element.remove(grantee_capabilities)
        return permissions_xml_request


    def replicate_permissions(self, orig_content):
        self.start_log_block()
        self.clear_all_permissions()

        # Self Permissions
        o_perms_obj_list = orig_content.current_perms_obj_list
        n_perms_obj_list = self.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list,
                                                                                            orig_content.t_rest_api)
        self.set_permissions_by_permissions_obj_list(n_perms_obj_list)
        self.end_log_block()

    @staticmethod
    def _fix_permissions_request_for_replication(tsr: ET.Element) -> ET.Element:
        # Remove the project tag from the original response
        proj_element = None
        for t in tsr.iter():

            if t.tag == 'project':
                proj_element = t
        if proj_element is not None:
            # You have to remove from the immediate parent apparently, which needs to be the permissions tag
            for p in tsr:
                p.remove(proj_element)
        return tsr

    def replicate_permissions_direct_xml(self, orig_content, username_map=None):
        """
        :type orig_content: PublishedContent
        :type username_map: dict[unicode, unicode]
        :return:
        """
        self.start_log_block()

        self.clear_all_permissions()

        # This is for the project Permissions. Handle defaults down below

        perms_tsr = self.t_rest_api.build_request_from_response(orig_content.obj_perms_xml)
        # Remove the project tag from the original response
        perms_tsr = self._fix_permissions_request_for_replication(perms_tsr)

        # Now convert over all groups and users
        self.convert_permissions_xml_object_from_orig_site_to_current_site(perms_tsr, orig_content.t_rest_api,
                                                                           username_map=username_map)
        self.set_permissions_by_permissions_direct_xml(perms_tsr)
        self.end_log_block()

    # Polyfill for removed cmp in Python3  https://portingguide.readthedocs.io/en/latest/comparisons.html
    @staticmethod
    def _cmp(x, y):
        """
        Replacement for built-in function cmp that was removed in Python 3

        Compare the two objects x and y and return an integer according to
        the outcome. The return value is negative if x < y, zero if x == y
        and strictly positive if x > y.
        """

        return (x > y) - (x < y)

    # Determine if capabilities are already set identically (or identically enough) to skip
    def are_capabilities_obj_lists_identical(self, new_obj_list: List['Permissions'],
                                             dest_obj_list: List['Permissions']) -> bool:
        # Grab the LUIDs of each, determine if they match in the first place

        # Create a dict with the LUID as the keys for sorting and comparison
        new_obj_dict = {}
        for obj in new_obj_list:
            new_obj_dict[obj.luid] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.luid] = obj
            # If lengths don't match, they must differ
            if len(new_obj_dict) != len(dest_obj_dict):
                return False
            else:
                # If LUIDs don't match, they must differ
                new_obj_luids = list(new_obj_dict.keys())
                dest_obj_luids = list(dest_obj_dict.keys())
                new_obj_luids.sort()
                dest_obj_luids.sort()
                if self._cmp(new_obj_luids, dest_obj_luids) != 0:
                    return False
                for luid in new_obj_luids:
                    new_obj = new_obj_dict.get(luid)
                    dest_obj = dest_obj_dict.get(luid)
                    return self.are_capabilities_obj_dicts_identical(new_obj.get_capabilities_dict(),
                                                                     dest_obj.get_capabilities_dict())

    @staticmethod
    def are_capabilities_obj_dicts_identical(new_obj_dict: Dict, dest_obj_dict: Dict) -> bool:
        if new_obj_dict.keys() == dest_obj_dict.keys():
            for k in new_obj_dict:
                if new_obj_dict[k] != dest_obj_dict[k]:
                    return False
            return True
        else:
            return False

    # Dict { capability_name : mode } into XML with checks for validity. Set type to 'workbook' or 'datasource'
    def build_capabilities_xml_from_dict(self, capabilities_dict: Dict, obj_type: str) -> ET.Element:
        if obj_type not in self.permissionable_objects:
            error_text = 'objtype can only be "project", "workbook" or "datasource", was given {}'
            raise InvalidOptionException(error_text.format('obj_type'))
        c = ET.Element('capabilities')

        for cap in capabilities_dict:
            # Skip if the capability is set to None
            if capabilities_dict[cap] is None:
                continue
            if capabilities_dict[cap] not in ['Allow', 'Deny']:
                raise InvalidOptionException('Capability mode can only be "Allow",  "Deny" (case-sensitive)')
            if obj_type == 'project':
                if cap not in Permissions.available_capabilities[self.api_version]["project"]:
                    raise InvalidOptionException('{} is not a valid capability for a project'.format(cap))
            if obj_type == 'datasource':
                # Ignore if not available for datasource
                if cap not in Permissions.available_capabilities[self.api_version]["datasource"]:
                    self.log('{} is not a valid capability for a datasource'.format(cap))
                    continue
            if obj_type == 'workbook':
                # Ignore if not available for workbook
                if cap not in Permissions.available_capabilities[self.api_version]["workbook"]:
                    self.log('{} is not a valid capability for a workbook'.format(cap))
                    continue
            capab = ET.Element('capability')
            capab.set('name', cap)
            capab.set('mode', capabilities_dict[cap])
            c.append(capab)
        return c

    def _build_add_permissions_request(self, permissions_obj: 'Permissions') -> ET.Element:
        tsr = ET.Element('tsRequest')
        p = ET.Element('permissions')
        capabilities_dict = permissions_obj.get_capabilities_dict()
        c = self.build_capabilities_xml_from_dict(capabilities_dict, self.obj_type)
        gcap = ET.Element('granteeCapabilities')
        t = ET.Element(permissions_obj.group_or_user)
        t.set('id', permissions_obj.luid)
        gcap.append(t)
        gcap.append(c)
        p.append(gcap)
        tsr.append(p)

        return tsr

    # Template stub
    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['Permissions']:
        pass

    def get_permissions_from_server(self, obj_perms_xml: Optional[ET.Element] = None) -> List['Permissions']:

        self.start_log_block()
        if obj_perms_xml is not None:
            self.obj_perms_xml = obj_perms_xml
        else:
            if self.default is False:
                self.obj_perms_xml = self.t_rest_api.query_resource(
                    "{}s/{}/permissions".format(self.obj_type, self.luid))
            elif self.default is True:
                self.obj_perms_xml = self.t_rest_api.query_resource(
                    "projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
        self.log('Converting XML into Permissions Objects for object type: {}'.format(self.obj_type))
        self.current_perms_obj_list = self.convert_capabilities_xml_into_obj_list(self.obj_perms_xml)
        self.end_log_block()
        return self.current_perms_obj_list

    def get_permissions_xml(self) -> ET.Element:
        return self.obj_perms_xml

    def get_permissions_obj_list(self) -> List['Permissions']:
        return self.current_perms_obj_list

    # This one doesn't do any of the checking or determining if there is a need to change. Only for pure replication
    def set_permissions_by_permissions_direct_xml(self, direct_xml_request: ET.Element):
        self.start_log_block()
        url = None
        if self.default is False:
            url = self.t_rest_api.build_api_url("{}s/{}/permissions".format(self.obj_type, self.luid))
        if self.default is True:
            url = self.t_rest_api.build_api_url(
                "projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
        new_perms_xml = self.t_rest_api.send_update_request(url, direct_xml_request)

        # Update the internal representation from the newly returned permissions XML
        self.get_permissions_from_server(new_perms_xml)

        self.end_log_block()

    # Shorter, cleaner code. Use in the future
    def set_permissions(self, permissions: Optional[List['Permissions']] = None,
                        direct_xml_request: Optional[ET.Element] = None):
        if permissions is not None and direct_xml_request is not None:
            raise InvalidOptionException('Please only send one of the two arguments at a time')
        if permissions is not None:
            self.set_permissions_by_permissions_obj_list(new_permissions_obj_list=permissions)
        elif direct_xml_request is not None:
            self.set_permissions_by_permissions_direct_xml(direct_xml_request=direct_xml_request)
        else:
            raise InvalidOptionException('Please send in at least one argument')

    def set_permissions_by_permissions_obj_list(self, new_permissions_obj_list):
        """
        :type new_permissions_obj_list: list[Permissions]
        """
        self.start_log_block()

        self.log("Permissions object list has {} items:".format(len(new_permissions_obj_list)))
        for new_permissions_obj in new_permissions_obj_list:
            for cur_obj in self.current_perms_obj_list:
                # Check if there are any existing capabilities on the object
                if cur_obj.luid == new_permissions_obj.luid:
                    # Find if anything is set already, add to deletion queue
                    need_to_change = self.are_capabilities_obj_dicts_identical(
                        cur_obj.get_capabilities_dict(), new_permissions_obj.get_capabilities_dict()
                    )
                    self.log("Existing permissions found for luid {}. Are there differences? {}".format(cur_obj.luid,
                                                                                                         str(need_to_change)))
                    # Delete all existing permissions
                    if need_to_change is True:
                        self.log("Removing existing permissions for luid {}".format(cur_obj.luid))
                        self.delete_permissions_by_permissions_obj_list([cur_obj, ])

                    if need_to_change is False:
                        self.log('No changes necessary, skipping update for quicker performance')
                        # self.end_log_block()
                        continue
            # Check if all capabilities are set to Unspecified, and ignore
            specified_cap_count = 0
            caps = new_permissions_obj.get_capabilities_dict()
            self.log("New permissions to be set:")
            self.log(str(caps))
            for cap in caps:
                if caps[cap] is not None:
                    specified_cap_count += 1
            if specified_cap_count > 0:
                self.log("Adding permissions")
                tsr = self._build_add_permissions_request(new_permissions_obj)
                url = None
                if self.default is False:
                    url = self.t_rest_api.build_api_url("{}s/{}/permissions".format(self.obj_type, self.luid))
                if self.default is True:
                    url = self.t_rest_api.build_api_url(
                        "projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
                new_perms_xml = self.t_rest_api.send_update_request(url, tsr)

                # Update the internal representation from the newly returned permissions XML
                self.get_permissions_from_server(new_perms_xml)
            else:
                self.get_permissions_from_server()
        self.end_log_block()

    # Cleaner code for the future
    def delete_permissions(self, permissions: List['Permissions']):
        self.delete_permissions_by_permissions_obj_list(permissions_obj_list=permissions)

    # Legacy longer way to call
    def delete_permissions_by_permissions_obj_list(self, permissions_obj_list: List['Permissions']):
        self.start_log_block()
        for permissions_obj in permissions_obj_list:
            obj_luid = permissions_obj.luid
            group_or_user = permissions_obj.group_or_user
            # Only work if permissions object matches the ContentType
            if permissions_obj.get_content_type() != self.obj_type:
                raise InvalidOptionException("Trying to set permission for a {} using a {} Permissions object".format(
                    self.obj_type, permissions_obj.get_content_type()
                ))
            self.log('Deleting for object LUID {}'.format(self.luid))
            permissions_dict = permissions_obj.get_capabilities_dict()
            for cap in permissions_dict:
                if self.default is True:
                    api_url_start = "projects/{}/default-permissions/{}s/{}s/{}/{}/".format(self.luid, self.obj_type,
                                                                                             group_or_user,
                                                                                             obj_luid, cap)
                else:
                    api_url_start = "{}s/{}/permissions/{}s/{}/{}/".format(self.obj_type, self.luid, group_or_user,
                                                                            obj_luid, cap)

                if permissions_dict.get(cap) == 'Allow':
                    # Delete Allow
                    url = self.t_rest_api.build_api_url(api_url_start + 'Allow')
                    self.t_rest_api.send_delete_request(url)
                elif permissions_dict.get(cap) == 'Deny':
                    # Delete Deny
                    url = self.t_rest_api.build_api_url(api_url_start + 'Deny')
                    self.t_rest_api.send_delete_request(url)
                else:
                    self.log('{} set to none, no action'.format(cap))
        self.end_log_block()

    def clear_all_permissions(self):
        self.start_log_block()
        self.get_permissions_from_server()
        self.log('Current permissions object list')
        self.log(str(self.current_perms_obj_list))
        self.delete_permissions(permissions=self.current_perms_obj_list)
        self.end_log_block()

class Workbook(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid=luid, obj_type="workbook", tableau_rest_api_obj=tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["workbook"]
        self.log("Workbook object initiating")
        self.permissions_object_class = WorkbookPermissions

    @property
    def luid(self) -> str:
        return self._luid

    @luid.setter
    def luid(self, name_or_luid: str):
        luid = self.t_rest_api.query_workbook_luid(name_or_luid)
        self._luid = luid

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'WorkbookPermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['WorkbookPermissions']:

        #self.start_log_block()
        obj_list = []
        xml = xml_obj.findall('.//t:granteeCapabilities', TableauRestXml.ns_map)
        if len(xml) == 0:
           # self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == '{}group'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = WorkbookPermissions('group', luid)
                      #  self.log_debug('group {}'.format(luid))
                    elif tags.tag == '{}user'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = WorkbookPermissions('user', luid)
                       # self.log_debug('user {}'.format(luid))
                    elif tags.tag == '{}capabilities'.format(TableauRestXml.ns_prefix):
                        for caps in tags:
                           # self.log_debug(caps.get('name') + ' : ' + caps.get('mode'))
                            perms_obj.set_capability(caps.get('name'), caps.get('mode'))
                obj_list.append(perms_obj)
            #self.log('Permissions object list has {} items'.format(str(len(obj_list))))
           # self.end_log_block()
            return obj_list

class Workbook28(Workbook):
    def __init__(self, luid, tableau_rest_api_obj, default=False, logger_obj=None,
                 content_xml_obj=None):
        Workbook.__init__(self, luid=luid, tableau_rest_api_obj=tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["workbook"]
        self.log("Workbook object initiating")
        self.permissions_object_class = WorkbookPermissions28

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> WorkbookPermissions28:
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

class Datasource(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj,  default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, "datasource", tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["datasource"]
        self.permissions_object_class = DatasourcePermissions

    @property
    def luid(self) -> str:
        return self._luid

    @luid.setter
    def luid(self, name_or_luid: str):
        ds_luid = self.t_rest_api.query_datasource_luid(name_or_luid)
        self._luid = ds_luid

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'DatasourcePermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['DatasourcePermissions']:
        #self.start_log_block()
        obj_list = []
        xml = xml_obj.findall('.//t:granteeCapabilities', TableauRestXml.ns_map)
        if len(xml) == 0:
            # self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == '{}group'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = DatasourcePermissions('group', luid)
                        #self.log_debug('group {}'.format(luid))
                    elif tags.tag == '{}user'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = DatasourcePermissions('user', luid)
                        #self.log_debug('user {}'.format(luid))
                    elif tags.tag == '{}capabilities'.format(TableauRestXml.ns_prefix):
                        for caps in tags:
                            #self.log_debug(caps.get('name') + ' : ' + caps.get('mode'))
                            perms_obj.set_capability(caps.get('name'), caps.get('mode'))
                obj_list.append(perms_obj)
            #self.log('Permissions object list has {} items'.format(str(len(obj_list))))
            # self.end_log_block()
            return obj_list

class Datasource28(Datasource):
    def __init__(self, luid: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 default: bool = False, logger_obj: Optional['Logger'] = None,
                 content_xml_obj: Optional[ET.Element] = None):
        Datasource.__init__(self, luid=luid, tableau_rest_api_obj=tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["datasource"]
        self.permissions_object_class = DatasourcePermissions28

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'DatasourcePermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

class View(PublishedContent):
    def __init__(self, luid: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 default: bool = False, logger_obj: Optional['Logger'] = None,
                 content_xml_obj: Optional[ET.Element] = None):
        PublishedContent.__init__(self, luid, "view", tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["workbook"]
        self.log("View object initiating")

    @property
    def luid(self) -> str:
        return self._luid

    @luid.setter
    def luid(self, luid: str):
        # Maybe implement a search at some point
        self._luid = luid

    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['WorkbookPermissions']:
        #self.start_log_block()
        obj_list = []
        xml = xml_obj.findall('.//t:granteeCapabilities', TableauRestXml.ns_map)
        if len(xml) == 0:
            #self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == '{}group'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = WorkbookPermissions('group', luid)
                        #self.log_debug('group {}'.format(luid))
                    elif tags.tag == '{}user'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = WorkbookPermissions('user', luid)
                        #self.log_debug('user {}'.format(luid))
                    elif tags.tag == '{}capabilities'.format(TableauRestXml.ns_prefix):
                        for caps in tags:
                            #self.log_debug(caps.get('name') + ' : ' + caps.get('mode'))
                            perms_obj.set_capability(caps.get('name'), caps.get('mode'))
                obj_list.append(perms_obj)
            #self.log('Permissions object list has {} items'.format(str(len(obj_list))))
            #self.end_log_block()
            return obj_list


class Flow33(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, "flow", tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["flow"]
        self.log("Flow object initiating")
        self.permissions_object_class = FlowPermissions33

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'FlowPermissions33':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

class Database35(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, "database", tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["database"]
        self.log("Database object initiating")
        self.permissions_object_class = DatabasePermissions35

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'DatabasePermissions35':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

class Table35(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj,  default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, "table", tableau_rest_api_obj,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["table"]
        self.log("Table object initiating")
        self.permissions_object_class = TablePermissions35

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'TablePermissions35':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)



class Project(PublishedContent):
    def __init__(self, luid: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 logger_obj: Optional['Logger'] = None, content_xml_obj: Optional[ET.Element] = None):
        PublishedContent.__init__(self, luid=luid, obj_type="project", tableau_rest_api_obj=tableau_rest_api_obj,
                                  logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.log('Building Project object from this XML: ')
        self.log_xml_response(content_xml_obj)
        self.log('Project object has this XML: ')
        self.log_xml_response(self.xml_obj)
        # projects in 9.2 have child workbook and datasource permissions
        self._workbook_defaults = Workbook(self.luid, self.t_rest_api,
                                           default=True, logger_obj=logger_obj)
        self._datasource_defaults = Datasource(self.luid, self.t_rest_api,
                                               default=True, logger_obj=logger_obj)

        self.__available_capabilities = Permissions.available_capabilities[self.api_version]["project"]
        self.permissions_locked = None
        self.permissions_locked = self.are_permissions_locked()
        self.permissions_object_class = ProjectPermissions

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, name_or_luid: str):
        if TableauRestXml.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            luid = self.t_rest_api.query_project_luid(name_or_luid)
        self._luid = luid

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'ProjectPermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

    # Simpler synonym
    @staticmethod
    def convert_xml_into_permissions_list(xml_obj: ET.Element) -> List['ProjectPermissions']:
        return Project.convert_capabilities_xml_into_obj_list(xml_obj=xml_obj)

    # Available for legacy
    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['ProjectPermissions']:
        #self.start_log_block()
        obj_list = []
        xml = xml_obj.findall('.//t:granteeCapabilities', TableauRestXml.ns_map)
        if len(xml) == 0:
            #self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == '{}group'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = ProjectPermissions('group', luid)
                       # self.log_debug('group {}'.format(luid))
                    elif tags.tag == '{}user'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = ProjectPermissions('user', luid)
                       # self.log_debug('user {}'.format(luid))
                    elif tags.tag == '{}capabilities'.format(TableauRestXml.ns_prefix):
                        for caps in tags:
                          #  self.log_debug(caps.get('name') + ' : ' + caps.get('mode'))
                            perms_obj.set_capability(caps.get('name'), caps.get('mode'))
                obj_list.append(perms_obj)
           # self.log('Permissions object list has {} items'.format(str(len(obj_list))))
            #self.end_log_block()
            return obj_list

    def replicate_permissions(self, orig_content: 'Project'):
        self.start_log_block()

        self.clear_all_permissions()

        # Self Permissions
        o_perms_obj_list = orig_content.current_perms_obj_list
        n_perms_obj_list = self.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list,
                                                                                            orig_content.t_rest_api)
        self.set_permissions_by_permissions_obj_list(n_perms_obj_list)

        # Workbook Defaults
        o_perms_obj_list = orig_content.workbook_defaults.current_perms_obj_list
        n_perms_obj_list = self.workbook_defaults.convert_permissions_obj_list_from_orig_site_to_current_site(
            o_perms_obj_list, orig_content.t_rest_api)
        self.workbook_defaults.set_permissions_by_permissions_obj_list(n_perms_obj_list)

        # Datasource Defaults
        o_perms_obj_list = orig_content.datasource_defaults.current_perms_obj_list
        n_perms_obj_list = self.datasource_defaults.convert_permissions_obj_list_from_orig_site_to_current_site(
            o_perms_obj_list, orig_content.t_rest_api)
        self.datasource_defaults.set_permissions_by_permissions_obj_list(n_perms_obj_list)

        self.end_log_block()

    def replicate_permissions_direct_xml(self, orig_content: 'Project', username_map: Optional[Dict] = None):
        self.start_log_block()

        self.clear_all_permissions()

        # This is for the project Permissions. Handle defaults down below

        perms_tsr = self.t_rest_api.build_request_from_response(orig_content.obj_perms_xml)
        # Remove the project tag from the original response
        perms_tsr = self._fix_permissions_request_for_replication(perms_tsr)

        # Now convert over all groups and users
        self.convert_permissions_xml_object_from_orig_site_to_current_site(perms_tsr, orig_content.t_rest_api,
                                                                           username_map=username_map)
        self.set_permissions_by_permissions_direct_xml(perms_tsr)

        # Workbook Defaults
        perms_tsr = self.t_rest_api.build_request_from_response(orig_content.workbook_defaults.obj_perms_xml)
        # Remove the project tag from the original response
        perms_tsr = self._fix_permissions_request_for_replication(perms_tsr)

        # Now convert over all groups and users
        self.convert_permissions_xml_object_from_orig_site_to_current_site(perms_tsr, orig_content.t_rest_api,
                                                                           username_map=username_map)
        self.workbook_defaults.set_permissions_by_permissions_direct_xml(perms_tsr)

        # Datasource Defaults
        perms_tsr = self.t_rest_api.build_request_from_response(orig_content.datasource_defaults.obj_perms_xml)
        # Remove the project tag from the original response
        perms_tsr = self._fix_permissions_request_for_replication(perms_tsr)

        # Now convert over all groups and users
        self.convert_permissions_xml_object_from_orig_site_to_current_site(perms_tsr, orig_content.t_rest_api,
                                                                           username_map=username_map)
        self.datasource_defaults.set_permissions_by_permissions_direct_xml(perms_tsr)

        self.end_log_block()

    # There are all legacy for compatibility purposes
    def create_project_permissions_object_for_group(self, group_name_or_luid: str,
                                                    role: Optional[str] = None) -> 'ProjectPermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=ProjectPermissions)

    def create_project_permissions_object_for_user(self, username_or_luid: str,
                                                    role: Optional[str] = None) -> 'ProjectPermissions':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=ProjectPermissions)

    def create_workbook_permissions_object_for_group(self, group_name_or_luid: str,
                                                    role: Optional[str] = None) -> 'WorkbookPermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=WorkbookPermissions)

    def create_workbook_permissions_object_for_user(self, username_or_luid: str,
                                                    role: Optional[str] = None) -> 'WorkbookPermissions':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=WorkbookPermissions)

    def create_datasource_permissions_object_for_group(self, group_name_or_luid: str,
                                                    role: Optional[str] = None) -> 'DatasourcePermissions':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=DatasourcePermissions)

    def create_datasource_permissions_object_for_user(self, username_or_luid: str,
                                                    role: Optional[str] = None) -> 'DatasourcePermissions':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=DatasourcePermissions)

    @property
    def workbook_defaults(self) -> Workbook:
        return self._workbook_defaults

    @property
    def datasource_defaults(self) -> Datasource:
        return self._datasource_defaults

    def clear_all_permissions(self, clear_defaults: bool = True):
        self.start_log_block()
        self.get_permissions_from_server()
        self.delete_permissions_by_permissions_obj_list(self.current_perms_obj_list)
        if clear_defaults is True:
            self.workbook_defaults.clear_all_permissions()
            self.datasource_defaults.clear_all_permissions()
        self.end_log_block()

    def are_permissions_locked(self) -> bool:
        proj = self.xml_obj
        locked_permissions = proj.get('contentPermissions')
        mapping = {'ManagedByOwner' : False, 'LockedToProject': True}
        return mapping[locked_permissions]

    def lock_permissions(self) -> 'Project':
        self.start_log_block()
        if self.permissions_locked is False:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=True)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=True)
                self.end_log_block()
                return new_proj_obj
        else:
            self.end_log_block()
            return self

    def unlock_permissions(self) -> 'Project':
        self.start_log_block()
        if self.permissions_locked is True:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=False)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=False)
            self.end_log_block()
            return new_proj_obj
        else:
            self.end_log_block()
            return self

    # These are speciality methods just for exporting everything out for audit
    def query_all_permissions(self) -> Dict:
        # Returns all_permissions[luid] = { name: , type: , project_caps, workbook_default_caps: ,
        #                                             datasource_default_caps: }

        all_permissions = {}
        for content_type in ['project', 'workbook_default', 'datasource_default']:
            if content_type == 'project':
                perms_obj_list = self.get_permissions_obj_list()
            elif content_type == 'workbook_default':
                perms_obj_list = self.workbook_defaults.get_permissions_obj_list()
            elif content_type == 'datasource_default':
                perms_obj_list = self.datasource_defaults.get_permissions_obj_list()
            else:
                raise InvalidOptionException('content_type must be project, workbook_default or datasource_default')

            for perms_obj in perms_obj_list:
                perm_luid = perms_obj.luid
                if all_permissions.get(perm_luid) is None:
                    all_permissions[perm_luid] = {"name": None, "type": None, "project_caps": None,
                                                  "workbook_default_caps": None, "datasource_default_caps": None}
                    perms_obj_type = perms_obj.group_or_user

                    if perms_obj_type == 'user':
                        all_permissions[perm_luid]["type"] = 'user'
                        name = self.t_rest_api.query_username(perm_luid)
                        all_permissions[perm_luid]["name"] = name

                    elif perms_obj_type == 'group':
                        all_permissions[perm_luid]["type"] = 'group'
                        name = self.t_rest_api.query_group_name(perm_luid)
                        all_permissions[perm_luid]["name"] = name

                perms = perms_obj.get_capabilities_dict()
                all_permissions[perm_luid]["{}_caps".format(content_type)] = perms

        return all_permissions

    # Exports all of the permissions on a project in the order displayed in Tableau Server
    def convert_all_permissions_to_list(self, all_permissions: Dict):
        final_list = []
        # Project

        for cap in Permissions.available_capabilities[self.t_rest_api.api_version]['project']:
            if all_permissions["project_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["project_caps"][cap])
        # Workbook
        for cap in Permissions.available_capabilities[self.t_rest_api.api_version]['workbook']:
            if all_permissions["workbook_default_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["workbook_default_caps"][cap])
        # Datasource
        for cap in Permissions.available_capabilities[self.t_rest_api.api_version]['datasource']:
            if all_permissions["datasource_default_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["datasource_default_caps"][cap])
        return final_list


class Project28(Project):
    def __init__(self, luid: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 logger_obj: Optional['Logger'] = None,
                 content_xml_obj: Optional[ET.Element] = None, parent_project_luid: Optional[str] = None):
        Project.__init__(self, luid=luid, tableau_rest_api_obj=tableau_rest_api_obj, logger_obj=logger_obj,
                         content_xml_obj=content_xml_obj)
        self._parent_project_luid = parent_project_luid
        self.permissions_object_class = ProjectPermissions28

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'ProjectPermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)
    @property
    def parent_project_luid(self) -> str:
        return self._parent_project_luid

    def query_child_projects(self) -> ET.Element:
        self.start_log_block()
        # This allows type checking without importing the class
        if (type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
            projects = self.t_rest_api.query_projects()
        else:
            projects = self.t_rest_api.projects.query_projects()

        child_projects = projects.findall('.//t:project[@parentProjectId="{}"]'.format(self.luid), self.t_rest_api.ns_map)
        self.end_log_block()
        return child_projects

    def lock_permissions(self) -> 'Project28':
        self.start_log_block()
        if self.permissions_locked is False:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=True)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=True)
            self.end_log_block()
            return new_proj_obj
        else:
            self.end_log_block()
            return self

    def unlock_permissions(self) -> 'Project28':
        self.start_log_block()
        if self.permissions_locked is True:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=False)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=False)
            self.end_log_block()
            return new_proj_obj
        else:
            self.end_log_block()
            return self

    @staticmethod
    def convert_capabilities_xml_into_obj_list(xml_obj: ET.Element) -> List['ProjectPermissions']:
        # self.start_log_block()
        obj_list = []
        xml = xml_obj.findall('.//t:granteeCapabilities', TableauRestXml.ns_map)
        if len(xml) == 0:
            # self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == '{}group'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = ProjectPermissions28('group', luid)
                        # self.log_debug('group {}'.format(luid))
                    elif tags.tag == '{}user'.format(TableauRestXml.ns_prefix):
                        luid = tags.get('id')
                        perms_obj = ProjectPermissions28('user', luid)
                        # self.log_debug('user {}'.format(luid))
                    elif tags.tag == '{}capabilities'.format(TableauRestXml.ns_prefix):
                        for caps in tags:
                            # self.log_debug(caps.get('name') + ' : ' + caps.get('mode'))
                            perms_obj._set_capability_from_published_content(caps.get('name'), caps.get('mode'))
                obj_list.append(perms_obj)
            # self.log('Permissions object list has {} items'.format(str(len(obj_list))))
            # self.end_log_block()
            return obj_list

    # There are all legacy for compatibility purposes
    def create_project_permissions_object_for_group(self, group_name_or_luid: str,
                                                    role: Optional[str] = None) -> 'ProjectPermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=ProjectPermissions28)

    def create_project_permissions_object_for_user(self, username_or_luid: str,
                                                   role: Optional[str] = None) -> 'ProjectPermissions28':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=ProjectPermissions28)

    def create_workbook_permissions_object_for_group(self, group_name_or_luid: str,
                                                     role: Optional[str] = None) -> 'WorkbookPermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=WorkbookPermissions28)

    def create_workbook_permissions_object_for_user(self, username_or_luid: str,
                                                    role: Optional[str] = None) -> 'WorkbookPermissions28':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=WorkbookPermissions28)

    def create_datasource_permissions_object_for_group(self, group_name_or_luid: str,
                                                       role: Optional[str] = None) -> 'DatasourcePermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, role=role,
                                            permissions_class_override=DatasourcePermissions28)

    def create_datasource_permissions_object_for_user(self, username_or_luid: str,
                                                      role: Optional[str] = None) -> 'DatasourcePermissions28':
        return self._get_permissions_object(username_or_luid=username_or_luid, role=role,
                                            permissions_class_override=DatasourcePermissions28)


class Project33(Project28):
    def __init__(self, luid: str, tableau_rest_api_obj: Union['TableauRestApiConnection', 'TableauServerRest'],
                 logger_obj: Optional['Logger'] = None, content_xml_obj: Optional[ET.Element] = None,
                 parent_project_luid:str = None):
        Project28.__init__(self, luid=luid, tableau_rest_api_obj=tableau_rest_api_obj, logger_obj=logger_obj,
                           content_xml_obj=content_xml_obj, parent_project_luid=parent_project_luid)
        self.flow_defaults = Flow33(self.luid, self.t_rest_api, default=True, logger_obj=logger_obj)

    def lock_permissions(self) -> 'Project33':
        self.start_log_block()
        if self.permissions_locked is False:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=True)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=True)
            self.end_log_block()
            return new_proj_obj
        else:
            self.end_log_block()
            return self

    def unlock_permissions(self) -> 'Project33':
        self.start_log_block()
        if self.permissions_locked is True:
            # This allows type checking without importing the class
            if(type(self.t_rest_api).__name__.find('TableauRestApiConnection') != -1):
                new_proj_obj = self.t_rest_api.update_project(self.luid, locked_permissions=False)
            else:
                new_proj_obj = self.t_rest_api.projects.update_project(self.luid, locked_permissions=False)
            self.end_log_block()
            return new_proj_obj
        else:
            self.end_log_block()
            return self

    def get_permissions_obj(self, group_name_or_luid: Optional[str] = None, username_or_luid: Optional[str] = None,
                               role: Optional[str] = None) -> 'ProjectPermissions28':
        return self._get_permissions_object(group_name_or_luid=group_name_or_luid, username_or_luid=username_or_luid,
                                            role=role)

