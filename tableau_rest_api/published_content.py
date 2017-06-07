from ..tableau_base import *
from ..tableau_exceptions import *
from grantee_capabilities import Permissions
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
        self.luid = luid
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
        self.groups_dict_cache = None
        # self.users_dict_cache = None

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

    def convert_capabilities_xml_into_obj_list(self, xml_obj, group_or_user=None):
        """
        :type xml_obj: etree.Element
        :type group_or_user: unicode
        :return: obj_list: list[Permissions]
        """
        self.start_log_block()
        obj_list = []

        xml = xml_obj.findall(u'.//t:GranteeCapabilities', namespaces=self.ns_map)
        if len(xml) == 0:
            return []
        else:
            for gcaps in xml:
                for tags in gcaps:
                    # Namespace fun
                    if tags.tag == u'{}group'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = Permissions(u'group', luid, group_or_user)
                        self.log(u'group {}'.format(luid))
                    elif tags.tag == u'{}user'.format(self.ns_prefix):
                        luid = tags.get(u'id')
                        perms_obj = Permissions(u'user', luid, group_or_user)
                        self.log(u'user {}'.format(luid))
                    elif tags.tag == u'{}capabilities'.format(self.ns_prefix):
                        for caps in tags:
                            self.log(caps.get(u'name') + ' : ' + caps.get(u'mode'))
                            perms_obj.set_capability(caps.get(u'name'), caps.get(u'mode'))
                obj_list.append(perms_obj)
            self.log(u'Permissions object list has {} items'.format(unicode(len(obj_list))))
            self.end_log_block()
            return obj_list

    # Runs through the gcap object list, and tries to do a conversion all principals to matching LUIDs on current site
    # Use case is replicating settings from one site to another
    # Orig_site must be TableauRestApiConnection
    # Not Finished
    def convert_permissions_obj_list_from_orig_site_to_current_site(self, permissions_obj_list, orig_site):
        """
        :param permissions_obj_list: list[Permissions]
        :param orig_site: TableauRestApiConnection
        :return: list[Permissions]
        """
        new_perms_obj_list = []
        # Make this more efficient -- should only look up those users it needs to. Question on algorithm for speed
        # If
        orig_site_groups = orig_site.query_groups()
        orig_site_users = orig_site.query_users()
        orig_site_groups_dict = self.convert_xml_list_to_name_id_dict(orig_site_groups)
        orig_site_users_dict = self.convert_xml_list_to_name_id_dict(orig_site_users)

        new_site_groups = self.t_rest_api.query_groups()
        new_site_users = self.t_rest_api.query_users()
        new_site_groups_dict = self.convert_xml_list_to_name_id_dict(new_site_groups)
        new_site_users_dict = self.convert_xml_list_to_name_id_dict(new_site_users)
        for perms_obj in permissions_obj_list:
            orig_luid = perms_obj.get_luid()
            if perms_obj.get_group_or_user() == u'group':
                # Find the name that matches the LUID
                try:
                    orig_name = (key for key, value in orig_site_groups_dict.items() if value == orig_luid).next()
                except StopIteration:
                    raise NoMatchFoundException(u"No matching name for luid {} found on the original site".format(
                        orig_luid))
                new_luid = new_site_groups_dict.get(orig_name)

            elif perms_obj.get_group_or_user() == u'user':
                # Find the name that matches the LUID
                try:
                    orig_name = (key for key, value in orig_site_users_dict.items() if value == orig_luid).next()
                except StopIteration:
                    raise NoMatchFoundException(u"No matching name for luid {} found on the original site".format(
                        orig_luid))
                new_luid = new_site_users_dict.get(orig_name)

            new_perms_obj = copy.copy(perms_obj)
            if new_luid is None:
                raise NoMatchFoundException(u"No matching {} named {} found on the new site".format(
                    perms_obj.get_obj_type(), orig_name))
            new_perms_obj.set_luid(new_luid)
            new_perms_obj_list.append(new_perms_obj)
        return new_perms_obj_list

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
            new_obj_dict[obj.get_luid()] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.get_luid()] = obj
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
        if cmp(new_obj_dict, dest_obj_dict) == 0:
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
        t = etree.Element(permissions_obj.get_group_or_user())
        t.set(u'id', permissions_obj.get_luid())
        gcap.append(t)
        gcap.append(c)
        p.append(gcap)
        tsr.append(p)

        return tsr

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
                self.obj_perms_xml = self.t_rest_api.query_resource(u"{}s/{}/permissions".format(self.obj_type, self.luid))
            elif self.default is True:
                self.obj_perms_xml = self.t_rest_api.query_resource(u"projects/{}/default-permissions/{}s".format(self.luid, self.obj_type))
        self.log(u'Converting XML into Permissions Objects for object type: {}'.format(self.obj_type))
        self.current_perms_obj_list = self.convert_capabilities_xml_into_obj_list(self.obj_perms_xml, self.obj_type)
        self.end_log_block()

    def get_permissions_xml(self):
        return self.obj_perms_xml

    def get_permissions_obj_list(self):
        return self.current_perms_obj_list

    def set_permissions_by_permissions_obj_list(self, new_permissions_obj_list):
        """
        :type new_permissions_obj_list: list[Permissions]
        """
        self.start_log_block()
        for new_permissions_obj in new_permissions_obj_list:
            for cur_obj in self.current_perms_obj_list:
                # Check if there are any existing capabilities on the object
                if cur_obj.get_luid() == new_permissions_obj.get_luid():
                    # Find if anything is set already, add to deletion queue
                    need_to_change = self.are_capabilities_obj_dicts_identical(
                        cur_obj.get_capabilities_dict(), new_permissions_obj.get_capabilities_dict()
                    )
                    self.log(u"Existing permissions found for luid {}. Are there differences? {}".format(cur_obj.get_luid(),
                                                                                                         str(need_to_change)))
                    # Delete all existing permissions
                    if need_to_change is True:
                        self.log(u"Removing exisiting permissions for luid {}".format(cur_obj.get_luid()))
                        self.t_rest_api.delete_permissions_by_luids(self.obj_type, self.luid, cur_obj.get_luid(),
                                                                    cur_obj.get_capabilities_dict(),
                                                                    cur_obj.get_obj_type())
                    if need_to_change is False:
                        self.end_log_block()
                        return True
            # Check if all capabilities are set to Unspecified, and ignore
            specified_cap_count = 0
            caps = new_permissions_obj.get_capabilities_dict()
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
            obj_luid = permissions_obj.get_luid()
            group_or_user = permissions_obj.get_group_or_user()
            # Only work if permissions object matches the ContentType
            if permissions_obj.get_content_type() != self.obj_type:
                raise InvalidOptionException(u"Trying to set permission for a {} using a {} Permissions object".format(
                    self.obj_type, permissions_obj.get_content_type()
                ))
            self.log(u'Deleting for object LUID {}'.format(self.luid))
            permissions_dict = permissions_obj.get_capabilities_dict()
            for cap in permissions_dict:
                if self.default is True:
                    api_url_start = u"projects/{}/default-permissions/{}s/permissions/{}s/{}/{}/".format(self.luid, self.obj_type,
                                                                                                         group_or_user,
                                                                                                         obj_luid, cap)
                else:
                    api_url_start = u"{}s/{}/permissions/{}s/{}/{}/".format(self.obj_type, self.luid, group_or_user,
                                                                            obj_luid, cap)

                if permissions_dict.get(cap) == u'Allow':
                    # Delete Allow
                    url = api_url_start + u'Allow'
                    self.t_rest_api.send_delete_request(url)
                elif permissions_dict.get(cap) == u'Deny':
                    # Delete Deny
                    url = api_url_start + u'Deny'
                    self.t_rest_api.send_delete_request(url)
                else:
                    self.log(u'{} set to none, no action'.format(cap))
        self.end_log_block()

    def clear_all_permissions(self):
        self.start_log_block()
        self.get_permissions_from_server()
        self.delete_permissions_by_permissions_obj_list(self.current_perms_obj_list)
        self.end_log_block()

    def replicate_content_permissions(self, destination_object, destination_site=None):
        """
        :type destination_object: PublishedContent
        :return:
        """
        self.start_log_block()
        if self.obj_type != destination_object.get_object_type():
            raise InvalidOptionException(u"Trying to replicate permissions from a {} to a {}".format(self.obj_type, destination_object.get_object_type()))

        capabilities_list = self.convert_capabilities_xml_into_obj_list(self.obj_perms_xml)
        dest_capabilities_list = self.convert_capabilities_xml_into_obj_list(destination_object.get_permissions_xml())
        if self.are_capabilities_obj_lists_identical(capabilities_list, dest_capabilities_list) is False:
            # Delete all first clears the object to have them added
            destination_object.clear_all_permissions()
            # Add each set of capabilities to the cleared object
            self.set_permissions_by_permissions_obj_list(self.convert_capabilities_xml_into_obj_list(self.get_permissions_xml()))
        else:
            self.log(u"Permissions matched, no need to update. Moving to next")
        self.end_log_block()


class Project(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"project", tableau_rest_api_obj, tableau_server_version,
                                  logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.log("Passing tableau_server_version {}".format(tableau_server_version))
        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]


class Project21(Project):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None,
                         content_xml_obj=None):
        Project.__init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=logger_obj,
                         content_xml_obj=content_xml_obj)
        # projects in 9.2 have child workbook and datasource permissions
        self.workbook_default = Workbook(self.luid, self.t_rest_api,
                                         tableau_server_version=tableau_server_version,
                                         default=True, logger_obj=logger_obj)
        self.datasource_default = Datasource(self.luid, self.t_rest_api,
                                             tableau_server_version=tableau_server_version,
                                             default=True, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]
        self.permissions_locked = None
        self.permissions_locked = self.are_permissions_locked()

    def clear_all_permissions(self):
        self.start_log_block()
        self.clear_all_permissions()
        self.workbook_default.clear_all_permissions()
        self.datasource_default.clear_all_permissions()
        self.end_log_block()

    def replicate_content_permissions(self, destination_object, destination_site=None):
        """
        :type destination_object: Project21
        :return:
        """
        self.start_log_block()
        if self.obj_type != destination_object.get_object_type():
            raise InvalidOptionException(u"Trying to replicate permissions from a {} to a {}".format(self.obj_type, destination_object.get_object_type()))

        capabilities_list = self.convert_capabilities_xml_into_obj_list(self.obj_perms_xml)
        dest_capabilities_list = self.convert_capabilities_xml_into_obj_list(destination_object.get_permissions_xml())
        if self.are_capabilities_obj_lists_identical(capabilities_list, dest_capabilities_list) is False:
            # Delete all first clears the object to have them added
            destination_object.clear_all_permissions()
            # Add each set of capabilities to the cleared object
            self.set_permissions_by_permissions_obj_list(self.convert_capabilities_xml_into_obj_list(self.get_permissions_xml()))
        else:
            self.log(u"Permissions matched, no need to update.")
        self.workbook_default.replicate_content_permissions(destination_object.workbook_default)
        self.datasource_default.replicate_content_permissions(destination_object.datasource_default)

        self.end_log_block()

    def are_permissions_locked(self):
        """
        :return: bool
        """
        self.start_log_block()
        proj = self.xml_obj
        locked_permissions = proj.get(u'contentPermissions')
        if locked_permissions == u'ManagedByOwner':
            return False
        if locked_permissions == u'LockedToProject':
            return True
        self.end_log_block()

    def lock_permissions(self):
        """
        :return:
        """
        self.start_log_block()
        if self.permissions_locked is False:
            self.t_rest_api.update_project_by_luid(self.luid, locked_permissions=True)
        self.end_log_block()

    def unlock_permissions(self):
        """
        :return:
        """
        self.start_log_block()
        if self.permissions_locked is True:
            self.t_rest_api.update_project_by_luid(self.luid, locked_permissions=False)

        self.end_log_block()


class Workbook(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"workbook", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"workbook"]


class Datasource(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None,
                 content_xml_obj=None):
        PublishedContent.__init__(self, luid, u"datasource", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj, content_xml_obj=content_xml_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"datasource"]
