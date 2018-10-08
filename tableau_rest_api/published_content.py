from ..tableau_base import *
from ..tableau_exceptions import *
from permissions import *
import copy


# Represents a published workbook, project or datasource
class PublishedContent(TableauBase):
    def __init__(self, luid, obj_type, tableau_rest_api_obj, tableau_server_version, default=False,
                 logger_obj=None, content_xml_obj=None):
        """
        :type luid: unicode
        :type obj_type: unicode
        :type tableau_rest_api_obj: TableauRestApiConnection
        :type tableau_server_version: unicode
        :type default: boolean
        :type logger_obj: Logger
        """
        TableauBase.__init__(self)
        self.set_tableau_server_version(tableau_server_version)

        self.logger = logger_obj
        self.log(u"Setting Server Version ID to {}".format(tableau_server_version))
        self._luid = luid
        self.t_rest_api = tableau_rest_api_obj
        self.obj_type = obj_type
        self.default = default
        self.obj_perms_xml = None
        self.current_perms_obj_list = None
        self.__permissionable_objects = self.permissionable_objects
        self.get_permissions_from_server()
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

    def get_groups_dict(self):
        groups = self.t_rest_api.query_groups()
        groups_dict = self.convert_xml_list_to_name_id_dict(groups)
        return groups_dict

    def get_xml_obj(self):
        return self.xml_obj

   # def get_users_dict(self):
   #     users = self.t_rest_api.query_users()
   #     users_dict = self.t_rest_api.convert_xml_list_to_name_id_dict(users)
   #     return users_dict

    # Copy Permissions for users or group
    def _copy_permissions_obj(self, perms_obj, user_or_group, name_or_luid):
        self.start_log_block()
        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            if user_or_group == u'group':
                luid = self.t_rest_api.query_group_luid(name_or_luid)
            elif user_or_group == u'user':
                luid = self.t_rest_api.query_user_luid(name_or_luid)
            else:
                raise InvalidOptionException(u'Must send group or user only')
        new_perms_obj = copy.deepcopy(perms_obj)
        new_perms_obj.luid = luid
        self.end_log_block()
        return new_perms_obj

    def copy_permissions_obj_for_group(self, perms_obj, group_name_or_luid):
        return self._copy_permissions_obj(perms_obj, u'group', group_name_or_luid)

    def copy_permissions_obj_for_user(self, perms_obj, username_or_luid):
        return self._copy_permissions_obj(perms_obj, u'user', username_or_luid)

    # Runs through the gcap object list, and tries to do a conversion all principals to matching LUIDs on current site
    # Use case is replicating settings from one site to another
    # Orig_site must be TableauRestApiConnection
    # Not Finished
    def convert_permissions_obj_list_from_orig_site_to_current_site(self, permissions_obj_list, orig_site):
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
            if perms_obj.group_or_user == u'group':
                # Find the name that matches the LUID
                try:
                    orig_name = orig_site.query_group_name(orig_luid)
                    self.log(u'Found orig luid {} for name {}'.format(orig_luid, orig_name ))
                    n_luid = self.t_rest_api.query_group_luid(orig_name)
                    self.log(u'Found new luid {} for name {}'.format(n_luid, orig_name ))
                except StopIteration:
                    self.log(u"No matching name for luid {} found on the original site, dropping from list".format(
                        orig_luid))

            elif perms_obj.group_or_user == u'user':
                # Find the name that matches the LUID
                try:
                    # Individual searches here. Efficient in versions with lookup
                    orig_user = orig_site.query_user(orig_luid)
                    orig_username = orig_user.get(u'name')
                    n_luid = self.t_rest_api.query_user_luid(orig_username)
                except NoMatchFoundException:
                    self.log(u"No matching name for luid {} found on the original site, dropping from list".format(
                        orig_luid))
            perms_obj.luid = n_luid
            final_perms_obj_list.append(copy.deepcopy(perms_obj))
        return final_perms_obj_list

    def replicate_permissions(self, orig_content):
        self.start_log_block()
        self.clear_all_permissions()

        # Self Permissions
        o_perms_obj_list = orig_content.current_perms_obj_list
        n_perms_obj_list = self.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list,
                                                                                            orig_content.t_rest_api)
        self.set_permissions_by_permissions_obj_list(n_perms_obj_list)
        self.end_log_block()

    # Determine if capabilities are already set identically (or identically enough) to skip
    def are_capabilities_obj_lists_identical(self, new_obj_list, dest_obj_list):
        """
        :type new_obj_list: list[Permissions]
        :type dest_obj_list: list[Permissions]
        :return: boolean
        """
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
                new_obj_luids = new_obj_dict.keys()
                dest_obj_luids = dest_obj_dict.keys()
                new_obj_luids.sort()
                dest_obj_luids.sort()
                if cmp(new_obj_luids, dest_obj_luids) != 0:
                    return False
                for luid in new_obj_luids:
                    new_obj = new_obj_dict.get(luid)
                    dest_obj = dest_obj_dict.get(luid)
                    return self.are_capabilities_obj_dicts_identical(new_obj.get_capabilities_dict(),
                                                                     dest_obj.get_capabilities_dict())

    @staticmethod
    def are_capabilities_obj_dicts_identical(new_obj_dict, dest_obj_dict):
        """
        :type new_obj_dict: dict
        :type dest_obj_dict: dict
        :return: bool
        """
        if new_obj_dict.viewkeys() == dest_obj_dict.viewkeys():
            for k in new_obj_dict:
                if new_obj_dict[k] != dest_obj_dict[k]:
                    return False
            return True
        else:
            return False

    # Dict { capability_name : mode } into XML with checks for validity. Set type to 'workbook' or 'datasource'
    def build_capabilities_xml_from_dict(self, capabilities_dict, obj_type):
        """
        :type capabilities_dict: dict
        :type obj_type: unicode
        :return: etree.Element
        """
        if obj_type not in self.permissionable_objects:
            error_text = u'objtype can only be "project", "workbook" or "datasource", was given {}'
            raise InvalidOptionException(error_text.format(u'obj_type'))
        c = etree.Element(u'capabilities')

        for cap in capabilities_dict:
            # Skip if the capability is set to None
            if capabilities_dict[cap] is None:
                continue
            if capabilities_dict[cap] not in [u'Allow', u'Deny']:
                raise InvalidOptionException(u'Capability mode can only be "Allow",  "Deny" (case-sensitive)')
            if obj_type == u'project':
                if cap not in self.available_capabilities[self.api_version][u"project"]:
                    raise InvalidOptionException(u'{} is not a valid capability for a project'.format(cap))
            if obj_type == u'datasource':
                # Ignore if not available for datasource
                if cap not in self.available_capabilities[self.api_version][u"datasource"]:
                    self.log(u'{} is not a valid capability for a datasource'.format(cap))
                    continue
            if obj_type == u'workbook':
                # Ignore if not available for workbook
                if cap not in self.available_capabilities[self.api_version][u"workbook"]:
                    self.log(u'{} is not a valid capability for a workbook'.format(cap))
                    continue
            capab = etree.Element(u'capability')
            capab.set(u'name', cap)
            capab.set(u'mode', capabilities_dict[cap])
            c.append(capab)
        return c

    def build_add_permissions_request(self, permissions_obj):
        """
        :param permissions_obj: Permissions
        :return: etree.Element
        """

        tsr = etree.Element(u'tsRequest')
        p = etree.Element(u'permissions')
        capabilities_dict = permissions_obj.get_capabilities_dict()
        c = self.build_capabilities_xml_from_dict(capabilities_dict, self.obj_type)
        gcap = etree.Element(u'granteeCapabilities')
        t = etree.Element(permissions_obj.group_or_user)
        t.set(u'id', permissions_obj.luid)
        gcap.append(t)
        gcap.append(c)
        p.append(gcap)
        tsr.append(p)

        return tsr

    # Template stub
    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        x = xml_obj

    def get_permissions_from_server(self, obj_perms_xml=None):
        """
        :type obj_perms_xml: etree.Element
        :return:
        """
        self.start_log_block()
        if obj_perms_xml is not None:
            self.obj_perms_xml = obj_perms_xml
        else:
            if self.default is False:
                self.obj_perms_xml = self.t_rest_api.query_resource(
                    u"{}s/{}/permissions".format(self.obj_type, self.luid))
            elif self.default is True:
                self.obj_perms_xml = self.t_rest_api.query_resource(
                    u"projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
        self.log(u'Converting XML into Permissions Objects for object type: {}'.format(self.obj_type))
        self.current_perms_obj_list = self.convert_capabilities_xml_into_obj_list(self.obj_perms_xml)
        self.end_log_block()

    def get_permissions_xml(self):
        return self.obj_perms_xml

    def get_permissions_obj_list(self):
        """
        :rtype: list[Permissions]
        """
        return self.current_perms_obj_list

    def set_permissions_by_permissions_obj_list(self, new_permissions_obj_list):
        """
        :type new_permissions_obj_list: list[Permissions]
        """
        self.start_log_block()

        self.log(u"Permissions object list has {} items:".format(len(new_permissions_obj_list)))
        for new_permissions_obj in new_permissions_obj_list:
            for cur_obj in self.current_perms_obj_list:
                # Check if there are any existing capabilities on the object
                if cur_obj.luid == new_permissions_obj.luid:
                    # Find if anything is set already, add to deletion queue
                    need_to_change = self.are_capabilities_obj_dicts_identical(
                        cur_obj.get_capabilities_dict(), new_permissions_obj.get_capabilities_dict()
                    )
                    self.log(u"Existing permissions found for luid {}. Are there differences? {}".format(cur_obj.luid,
                                                                                                         str(need_to_change)))
                    # Delete all existing permissions
                    if need_to_change is True:
                        self.log(u"Removing existing permissions for luid {}".format(cur_obj.luid))
                        self.delete_permissions_by_permissions_obj_list([cur_obj, ])

                    if need_to_change is False:
                        self.log(u'No changes necessary, skipping update for quicker performance')
                        self.end_log_block()
                        continue
            # Check if all capabilities are set to Unspecified, and ignore
            specified_cap_count = 0
            caps = new_permissions_obj.get_capabilities_dict()
            self.log(u"New permissions to be set:")
            self.log(str(caps))
            for cap in caps:
                if caps[cap] is not None:
                    specified_cap_count += 1
            if specified_cap_count > 0:
                self.log(u"Adding permissions")
                tsr = self.build_add_permissions_request(new_permissions_obj)
                url = None
                if self.default is False:
                    url = self.t_rest_api.build_api_url(u"{}s/{}/permissions".format(self.obj_type, self.luid))
                if self.default is True:
                    url = self.t_rest_api.build_api_url(
                        u"projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
                new_perms_xml = self.t_rest_api.send_update_request(url, tsr)

                # Update the internal representation from the newly returned permissions XML
                self.get_permissions_from_server(new_perms_xml)
            else:
                self.get_permissions_from_server()
        self.end_log_block()

    def delete_permissions_by_permissions_obj_list(self, permissions_obj_list):
        """
        :type permissions_obj_list: list[Permissions]
        :return:
        """
        self.start_log_block()
        for permissions_obj in permissions_obj_list:
            obj_luid = permissions_obj.luid
            group_or_user = permissions_obj.group_or_user
            # Only work if permissions object matches the ContentType
            if permissions_obj.get_content_type() != self.obj_type:
                raise InvalidOptionException(u"Trying to set permission for a {} using a {} Permissions object".format(
                    self.obj_type, permissions_obj.get_content_type()
                ))
            self.log(u'Deleting for object LUID {}'.format(self.luid))
            permissions_dict = permissions_obj.get_capabilities_dict()
            for cap in permissions_dict:
                if self.default is True:
                    api_url_start = u"projects/{}/default-permissions/{}s/{}s/{}/{}/".format(self.luid, self.obj_type,
                                                                                             group_or_user,
                                                                                             obj_luid, cap)
                else:
                    api_url_start = u"{}s/{}/permissions/{}s/{}/{}/".format(self.obj_type, self.luid, group_or_user,
                                                                            obj_luid, cap)

                if permissions_dict.get(cap) == u'Allow':
                    # Delete Allow
                    url = self.t_rest_api.build_api_url(api_url_start + u'Allow')
                    self.t_rest_api.send_delete_request(url)
                elif permissions_dict.get(cap) == u'Deny':
                    # Delete Deny
                    url = self.t_rest_api.build_api_url(api_url_start + u'Deny')
                    self.t_rest_api.send_delete_request(url)
                else:
                    self.log(u'{} set to none, no action'.format(cap))
        self.end_log_block()

    def clear_all_permissions(self):
        self.start_log_block()
        self.get_permissions_from_server()
        self.log(u'Current permissions object list')
        self.log(str(self.current_perms_obj_list))
        self.delete_permissions_by_permissions_obj_list(self.current_perms_obj_list)
        self.end_log_block()


class Project20(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"project", tableau_rest_api_obj, tableau_server_version,
                                  logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.log(u"Passing tableau_server_version {}".format(tableau_server_version))
        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, name_or_luid):
        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            luid = self.t_rest_api.query_project_luid(name_or_luid)
        self._luid = luid

    def _get_permissions_object(self, group_or_user, name_or_luid, obj_type, role=None):

        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            if group_or_user == u'group':
                luid = self.t_rest_api.query_group_luid(name_or_luid)
            elif group_or_user == u'user':
                luid = self.t_rest_api.query_user_luid(name_or_luid)
            else:
                raise InvalidOptionException(u'group_or_user must be group or user')

        if obj_type == u'project':
            perms_obj = ProjectPermissions20(group_or_user, luid)
        elif obj_type == u'workbook':
            perms_obj = WorkbookPermissions20(group_or_user, luid)
        elif obj_type == u'datasource':
            perms_obj = DatasourcePermissions20(group_or_user, luid)
        else:
            raise InvalidOptionException(u'obj_type must be project, workbook or datasource')
        if role is not None:
            perms_obj.set_capabilities_to_match_role(role)
        return perms_obj

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[ProjectPermissions20]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions20(u'group', luid)
                        self.log(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions20(u'user', luid)
                        self.log(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list

    def create_project_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: ProjectPermissions20
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'project', role)

    def create_project_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: ProjectPermissions20
        """
        return self._get_permissions_object(u'user', username_or_luid, u'project', role)

    def create_workbook_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: WorkbookPermissions20
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'workbook', role)

    def create_workbook_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: WorkbookPermissions20
        """
        return self._get_permissions_object(u'user', username_or_luid, u'workbook', role)

    def create_datasource_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: DatasourcePermissions20
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'datasource', role)

    def create_datasource_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: DatasourcePermissions20
        """
        return self._get_permissions_object(u'user', username_or_luid, u'datasource', role)


class Project21(Project20):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None,
                 content_xml_obj=None):
        Project20.__init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=logger_obj,
                           content_xml_obj=content_xml_obj)
        # projects in 9.2 have child workbook and datasource permissions
        self._workbook_defaults = Workbook(self.luid, self.t_rest_api,
                                           tableau_server_version=tableau_server_version,
                                           default=True, logger_obj=logger_obj)
        self._datasource_defaults = Datasource(self.luid, self.t_rest_api,
                                               tableau_server_version=tableau_server_version,
                                               default=True, logger_obj=logger_obj)

        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]
        self.permissions_locked = None
        self.permissions_locked = self.are_permissions_locked()

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[ProjectPermissions21]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions21(u'group', luid)
                        self.log_debug(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions21(u'user', luid)
                        self.log_debug(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log_debug(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list

    def replicate_permissions(self, orig_content):
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

    def _get_permissions_object(self, group_or_user, name_or_luid, obj_type, role=None):

        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            if group_or_user == u'group':
                luid = self.t_rest_api.query_group_luid(name_or_luid)
            elif group_or_user == u'user':
                luid = self.t_rest_api.query_user_luid(name_or_luid)
            else:
                raise InvalidOptionException(u'group_or_user must be group or user')

        if obj_type == u'project':
            perms_obj = ProjectPermissions21(group_or_user, luid)
        elif obj_type == u'workbook':
            perms_obj = WorkbookPermissions21(group_or_user, luid)
        elif obj_type == u'datasource':
            perms_obj = DatasourcePermissions21(group_or_user, luid)
        else:
            raise InvalidOptionException(u'obj_type must be project, workbook or datasource')
        perms_obj.enable_logging(self.logger)
        if role is not None:
            perms_obj.set_capabilities_to_match_role(role)
        return perms_obj

    def create_project_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: ProjectPermissions21
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'project', role)

    def create_project_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: ProjectPermissions21
        """
        return self._get_permissions_object(u'user', username_or_luid, u'project', role)

    def create_workbook_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: WorkbookPermissions21
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'workbook', role)

    def create_workbook_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: WorkbookPermissions21
        """
        return self._get_permissions_object(u'user', username_or_luid, u'workbook', role)

    def create_datasource_permissions_object_for_group(self, group_name_or_luid, role=None):
        """
        :type group_name_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: DatasourcePermissions21
        """
        return self._get_permissions_object(u'group', group_name_or_luid, u'datasource', role)

    def create_datasource_permissions_object_for_user(self, username_or_luid, role=None):
        """
        :type username_or_luid: unicode
        :type role: unicode
        :param role: Optional role from Tableau Server. Shortcut to set_capabilities_to_match_role
        :return: DatasourcePermissions21
        """
        return self._get_permissions_object(u'user', username_or_luid, u'datasource', role)

    @property
    def workbook_defaults(self):
        """
        :rtype: Workbook
        """
        return self._workbook_defaults

    @property
    def datasource_defaults(self):
        """
        :rtype: Datasource
        """
        return self._datasource_defaults

    def clear_all_permissions(self, clear_defaults=True):
        """
        :param clear_defaults: If set to False, the Default Permssiosn will not be cleared
        :type clear_defaults:
        :return:
        """
        self.start_log_block()
        self.get_permissions_from_server()
        self.delete_permissions_by_permissions_obj_list(self.current_perms_obj_list)
        if clear_defaults is True:
            self.workbook_defaults.clear_all_permissions()
            self.datasource_defaults.clear_all_permissions()
        self.end_log_block()

    def are_permissions_locked(self):
        """
        :return: bool
        """
        proj = self.xml_obj
        locked_permissions = proj.get(u'contentPermissions')
        if locked_permissions == u'ManagedByOwner':
            return False
        if locked_permissions == u'LockedToProject':
            return True

    def lock_permissions(self):
        """
        :return:
        """
        self.start_log_block()
        if self.permissions_locked is False:
            self.t_rest_api.update_project(self.luid, locked_permissions=True)
        self.end_log_block()

    def unlock_permissions(self):
        """
        :return:
        """
        self.start_log_block()
        if self.permissions_locked is True:
            self.t_rest_api.update_project(self.luid, locked_permissions=False)

        self.end_log_block()

    def query_all_permissions(self):
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
    def convert_all_permissions_to_list(self, all_permissions):
        final_list = []
        # Project

        for cap in self.t_rest_api.available_capabilities[self.t_rest_api.api_version][u'project']:
            if all_permissions["project_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["project_caps"][cap])
        # Workbook
        for cap in self.t_rest_api.available_capabilities[self.t_rest_api.api_version][u'workbook']:
            if all_permissions["workbook_default_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["workbook_default_caps"][cap])
        # Datasource
        for cap in self.t_rest_api.available_capabilities[self.t_rest_api.api_version][u'datasource']:
            if all_permissions["datasource_default_caps"] is None:
                final_list.append(None)
            else:
                final_list.append(all_permissions["datasource_default_caps"][cap])
        return final_list


class Project28(Project21):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None,
                 content_xml_obj=None, parent_project_luid=None):
        Project21.__init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=logger_obj,
                           content_xml_obj=content_xml_obj)
        self._parent_project_luid = parent_project_luid

    @property
    def parent_project_luid(self):
        return self._parent_project_luid

    def query_child_projects(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        projects = self.t_rest_api.query_projects()
        child_projects = projects.findall(u'.//t:project[@parentProjectId="{}"]'.format(self.luid), self.ns_map)
        self.end_log_block()
        return child_projects

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[ProjectPermissions21]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions28(u'group', luid)
                        self.log_debug(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = ProjectPermissions28(u'user', luid)
                        self.log_debug(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log_debug(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj._set_capability_from_published_content(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list

    def _get_permissions_object(self, group_or_user, name_or_luid, obj_type, role=None):

        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            if group_or_user == u'group':
                luid = self.t_rest_api.query_group_luid(name_or_luid)
            elif group_or_user == u'user':
                luid = self.t_rest_api.query_user_luid(name_or_luid)
            else:
                raise InvalidOptionException(u'group_or_user must be group or user')

        if obj_type == u'project':
            perms_obj = ProjectPermissions28(group_or_user, luid)
        elif obj_type == u'workbook':
            perms_obj = WorkbookPermissions28(group_or_user, luid)
        elif obj_type == u'datasource':
            perms_obj = DatasourcePermissions28(group_or_user, luid)
        else:
            raise InvalidOptionException(u'obj_type must be project, workbook or datasource')
        perms_obj.enable_logging(self.logger)
        if role is not None:
            perms_obj.set_capabilities_to_match_role(role)
        return perms_obj


class Workbook(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"workbook", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"workbook"]
        self.log(u"Workbook object initiating")

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, name_or_luid):
        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            luid = self.t_rest_api.query_workbook_luid(name_or_luid)
        self._luid = luid

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[WorkbookPermissions21]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = WorkbookPermissions21(u'group', luid)
                        self.log_debug(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = WorkbookPermissions21(u'user', luid)
                        self.log_debug(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log_debug(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list


class Datasource(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"datasource", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"datasource"]

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, name_or_luid):
        if self.is_luid(name_or_luid):
            luid = name_or_luid
        else:
            luid = self.t_rest_api.query_datasource_luid(name_or_luid)
        self._luid = luid

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[DatasourcePermissions21]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = DatasourcePermissions21(u'group', luid)
                        self.log_debug(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = DatasourcePermissions21(u'user', luid)
                        self.log_debug(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log_debug(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list


class View(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"view", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"workbook"]
        self.log(u"View object initiating")

    @property
    def luid(self):
        return self._luid

    @luid.setter
    def luid(self, luid):
        # Maybe implement a search at some point
        self._luid = luid

    def convert_capabilities_xml_into_obj_list(self, xml_obj):
        """
        :type xml_obj: etree.Element
        :rtype: list[WorkbookPermissions21]
        """
        self.start_log_block()
        obj_list = []
        xml = xml_obj.findall(u'.//t:granteeCapabilities', self.ns_map)
        if len(xml) == 0:
            self.end_log_block()
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = WorkbookPermissions21(u'group', luid)
                        self.log_debug(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = WorkbookPermissions21(u'user', luid)
                        self.log_debug(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log_debug(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list