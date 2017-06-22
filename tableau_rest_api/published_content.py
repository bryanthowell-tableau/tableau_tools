from ..tableau_base import TableauBase


# Represents a published workbook, project or datasource
class PublishedContent(TableauBase):
    def __init__(self, luid, obj_type, tableau_rest_api_obj, tableau_server_version, default=False,
                 logger_obj=None):
        TableauBase.__init__(self)
        self.set_tableau_server_version(tableau_server_version)

        """
        :type tableau_rest_api_obj: TableauRestApi
        """
        self.logger = logger_obj
        self.log("Setting Server Version ID to {}".format(tableau_server_version))
        self.luid = luid
        self.t_rest_api = tableau_rest_api_obj
        self.obj_type = obj_type
        self.default = default
        self.obj_perms_xml = None
        self.current_gcap_obj_list = None
        self.get_permissions_from_server()

        # If you want to know the name that matches to the group or user, need these
        # But no need to request every single time
        self.groups_dict_cache = None
        self.users_dict_cache = None

    def get_groups_dict(self):
        groups = self.t_rest_api.query_groups()
        groups_dict = self.t_rest_api.convert_xml_list_to_name_id_dict(groups)
        return groups_dict

    def get_users_dict(self):
        users = self.t_rest_api.query_users()
        users_dict = self.t_rest_api.convert_xml_list_to_name_id_dict(users)
        return users_dict

    def get_permissions_from_server(self, obj_perms_xml=None):
        self.start_log_block()
        if obj_perms_xml is not None:
            self.obj_perms_xml = obj_perms_xml
        else:
            if self.default is False:
                self.obj_perms_xml = self.t_rest_api.query_permissions_by_luid(self.obj_type, self.luid)
            if self.default is True:
                self.obj_perms_xml = self.t_rest_api.query_default_permissions_by_project_luid(self.luid, self.obj_type)
        self.log('Converting XML into Gcap Objects for object type: {}'.format(self.obj_type))
        self.current_gcap_obj_list = self.t_rest_api.convert_capabilities_xml_into_obj_list(self.obj_perms_xml,
                                                                                            self.obj_type)
        self.start_log_block()

    def get_permissions_xml(self):
        return self.obj_perms_xml

    def get_gcap_obj_list(self):
        return self.current_gcap_obj_list

    def clear_all_permissions(self):
        self.start_log_block()
        self.get_permissions_from_server()
        gcap_obj_list = self.current_gcap_obj_list
        for cur_gcap_obj in gcap_obj_list:
            self.log(u"Removing exisiting permissions for luid {}".format(cur_gcap_obj.get_luid()))
            if self.default is False:
                self.t_rest_api.delete_permissions_by_luids(self.obj_type, self.luid,
                                                            cur_gcap_obj.get_luid(),
                                                            cur_gcap_obj.get_capabilities_dict(),
                                                            cur_gcap_obj.get_obj_type()
                                                            )
            if self.default is True:
                self.t_rest_api.delete_default_permissions_for_project_by_luids(
                    self.luid, self.obj_type, [True, ],
                    cur_gcap_obj.get_luid(),
                    cur_gcap_obj.get_capabilities_dict(),
                    cur_gcap_obj.get_obj_type()
                )
        self.end_log_block()

    def set_permissions_by_gcap_obj(self, new_gcap_obj):
        """
        :type new_gcap_obj: GranteeCapabilities
        """
        self.start_log_block()
        for cur_gcap_obj in self.current_gcap_obj_list:
            # Check if there are any existing capabilities on the object
            if cur_gcap_obj.get_luid() == new_gcap_obj.get_luid():
                # Find if anything is set already, add to deletion queue
                need_to_change = self.are_capabilities_obj_dicts_identical(
                    cur_gcap_obj.get_capabilities_dict(), new_gcap_obj.get_capabilities_dict()
                )
                self.log(u"Existing permissions found for luid {}. Are there differences? {}".format(cur_gcap_obj.get_luid(),
                                                                                                     str(need_to_change)))
                # Delete all existing permissions
                if need_to_change is True:
                    self.log(u"Removing exisiting permissions for luid {}".format(cur_gcap_obj.get_luid()))
                    if self.default is False:
                        self.t_rest_api.delete_permissions_by_luids(self.obj_type, self.luid,
                                                                    cur_gcap_obj.get_luid(),
                                                                    cur_gcap_obj.get_capabilities_dict(),
                                                                    cur_gcap_obj.get_obj_type()
                                                                    )
                    if self.default is True:
                        self.t_rest_api.delete_default_permissions_for_project_by_luids(
                                                                    self.luid, self.obj_type, [True, ],
                                                                    cur_gcap_obj.get_luid(),
                                                                    cur_gcap_obj.get_capabilities_dict(),
                                                                    cur_gcap_obj.get_obj_type()
                                                                    )
                if need_to_change is False:
                    self.end_log_block()
                    return True
        # Check if all capabilities are set to Unspecified, and ignore
        specified_cap_count = 0
        caps = new_gcap_obj.get_capabilities_dict()
        for cap in caps:
            if caps[cap] is not None:
                specified_cap_count += 1
        if specified_cap_count > 0:
            if self.default is False:
                self.log(u"Adding permissions")
                new_perms_xml = self.t_rest_api.add_permissions_by_gcap_obj_list(self.obj_type, self.luid, [new_gcap_obj, ])
            if self.default is True:
                self.log(u"Adding default permissions")
                new_perms_xml = self.t_rest_api.add_default_permissions_to_project_by_gcap_obj_list(self.luid, self.obj_type,
                                                                                    [True, ], [new_gcap_obj, ])
            # Update the internal representation from the newly returned permissions XML
            self.get_permissions_from_server(new_perms_xml)
        else:
            self.get_permissions_from_server()
        self.end_log_block()


class Project(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, logger_obj=None):
        PublishedContent.__init__(self, luid, u"project", tableau_rest_api_obj, tableau_server_version,
                                  logger_obj=logger_obj)
        self.log("Passing tableau_server_version {}".format(tableau_server_version))
        # projects in 9.2 have child workbook and datasource permissions
        if self.api_version != u"2.0":
                self.workbook_default = Workbook(self.luid, self.t_rest_api,
                                                 tableau_server_version=tableau_server_version,
                                                 default=True, logger_obj=logger_obj)
                self.datasource_default = Datasource(self.luid, self.t_rest_api,
                                                     tableau_server_version=tableau_server_version,
                                                     default=True, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]
        self.permissions_locked = None
        self.permissions_locked = self.are_permissions_locked()

    def are_permissions_locked(self):
        self.start_log_block()
        if self.api_version != u"2.0":
            proj = self.t_rest_api.query_project_by_luid(self.luid)
            locked_permissions = proj.get(u'contentPermissions')
            if locked_permissions == u'ManagedByOwner':
                return False
            if locked_permissions == u'LockedToProject':
                return True
        else:
            self.log(u"Permissions cannot be locked in 9.1 and previous")
            return None
        self.end_log_block()

    def clear_all_permissions_including_defaults(self):
        self.start_log_block()
        self.clear_all_permissions()
        self.workbook_default.clear_all_permissions()
        self.datasource_default.clear_all_permissions()
        self.end_log_block()

    def lock_permissions(self):
        self.start_log_block()
        if self.api_version != u"2.0":
            if self.permissions_locked is False:
                self.t_rest_api.lock_project_permissions(self.luid)
        else:
            self.log(u"Permissions cannot be locked in 9.1 and previous")
        self.end_log_block()

    def unlock_permissions(self):
        self.start_log_block()
        if self.api_version != u"2.0":
            if self.permissions_locked is True:
                self.t_rest_api.unlock_project_permissions(self.luid)
        else:
            self.log(u"Permissions cannot be locked in 9.1 and previous")
        self.end_log_block()

    def query_all_permissions(self):
        # Returns gcap_combined_permissions[luid] = { name: , type: , project_caps, workbook_default_caps: ,
        #                                             datasource_default_caps: }

        if self.users_dict_cache is None:
            self.users_dict_cache = self.get_users_dict()
        if self.groups_dict_cache is None:
            self.groups_dict_cache = self.get_groups_dict()

        all_permissions = {}
        gcap_obj_list = self.get_gcap_obj_list()
        for gcap_obj in gcap_obj_list:
            gcap_luid = gcap_obj.get_luid()
            all_permissions[gcap_luid] = {"name": None, "type": None, "project_caps": None,
                                          "workbook_default_caps": None, "datasource_default_caps": None}
            gcap_obj_type = gcap_obj.get_obj_type()
            gcap_perms = gcap_obj.get_capabilities_dict()
            all_permissions[gcap_luid]["project_caps"] = gcap_perms
            if gcap_obj_type == 'user':
                all_permissions[gcap_luid]["type"] = 'user'
                for name, luid in self.users_dict_cache.items():
                    if gcap_luid == luid:
                        all_permissions[gcap_luid]["name"] = name

            elif gcap_obj_type == 'group':
                all_permissions[gcap_luid]["type"] = 'group'
                for name, luid in self.groups_dict_cache.items():
                    if gcap_luid == luid:
                        all_permissions[gcap_luid]["name"] = name

        # Project Default Workbook permissions
        gcap_obj_list = self.workbook_default.get_gcap_obj_list()
        for gcap_obj in gcap_obj_list:
            gcap_luid = gcap_obj.get_luid()
            gcap_perms = gcap_obj.get_capabilities_dict()
            if all_permissions.get(luid) is None:
                gcap_obj_type = gcap_obj.get_obj_type()
                all_permissions[gcap_luid] = {"name": None, "type": None, "project_caps": None,
                                              "workbook_default_caps": None, "datasource_default_caps": None}
                if gcap_obj_type == 'user':
                    all_permissions[gcap_luid]["type"] = 'user'
                    for name, luid in self.users_dict_cache.items():
                        if gcap_luid == luid:
                            all_permissions[gcap_luid]["name"] = name
                elif gcap_obj_type == 'group':
                    all_permissions[gcap_luid]["type"] = 'group'
                    for name, luid in self.groups_dict_cache.items():
                        if gcap_luid == luid:
                            all_permissions[gcap_luid]["name"] = name

            all_permissions[gcap_luid]["workbook_default_caps"] = gcap_perms

        # Project Default Data Source permissions
        gcap_obj_list = self.datasource_default.get_gcap_obj_list()
        for gcap_obj in gcap_obj_list:
            gcap_luid = gcap_obj.get_luid()
            gcap_perms = gcap_obj.get_capabilities_dict()
            if all_permissions.get(luid) is None:
                gcap_obj_type = gcap_obj.get_obj_type()
                all_permissions[gcap_luid] = {"name": None, "type": None, "project_caps": None,
                                              "workbook_default_caps": None, "datasource_default_caps": None}
                if gcap_obj_type == 'user':
                    all_permissions[gcap_luid]["type"] = 'user'
                    for name, luid in self.users_dict_cache.items():
                        if gcap_luid == luid:
                            all_permissions[gcap_luid]["name"] = name
                elif gcap_obj_type == 'group':
                    all_permissions[gcap_luid]["type"] = 'group'
                    for name, luid in self.groups_dict_cache.items():
                        if gcap_luid == luid:
                            all_permissions[gcap_luid]["name"] = name
            all_permissions[gcap_luid]["datasource_default_caps"] = gcap_perms

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


class Workbook(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None):
        PublishedContent.__init__(self, luid, u"workbook", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"workbook"]


class Datasource(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version, default=False, logger_obj=None):
        PublishedContent.__init__(self, luid, u"datasource", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"datasource"]
