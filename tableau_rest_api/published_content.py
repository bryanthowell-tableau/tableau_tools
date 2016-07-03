from ..tableau_base import TableauBase


# Represents a published workbook, project or datasource
class PublishedContent(TableauBase):
    def __init__(self, luid, obj_type, tableau_rest_api_obj, tableau_server_version=u"9.2", default=False, logger_obj=None):
        TableauBase.__init__(self)
        self.set_tableau_server_version(tableau_server_version)
        """
        :type tableau_rest_api_obj: TableauRestApi
        """
        self.logger = logger_obj
        self.luid = luid
        self.t_rest_api = tableau_rest_api_obj
        self.obj_type = obj_type
        self.default = default
        self.obj_perms_xml = None
        self.current_gcap_obj_list = None
        self.get_permissions_from_server()

    def get_permissions_from_server(self, obj_perms_xml=None):
        self.start_log_block()
        if obj_perms_xml is not None:
            self.obj_perms_xml = obj_perms_xml
        else:
            if self.default is False:
                self.obj_perms_xml = self.t_rest_api.query_permissions_by_luid(self.obj_type, self.luid)
            if self.default is True:
                self.obj_perms_xml = self.t_rest_api.query_default_permissions_by_project_luid(self.luid, self.obj_type)
        self.current_gcap_obj_list = self.t_rest_api.convert_capabilities_xml_into_obj_list(self.obj_perms_xml,
                                                                                            self.obj_type)
        self.start_log_block()

    def get_permissions_xml(self):
        return self.obj_perms_xml

    def get_gcap_obj_list(self):
        return self.current_gcap_obj_list

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
        if self.default is False:
            self.log(u"Adding permissions")
            new_perms_xml = self.t_rest_api.add_permissions_by_gcap_obj_list(self.obj_type, self.luid, [new_gcap_obj, ])
        if self.default is True:
            self.log(u"Adding default permissions")
            new_perms_xml = self.t_rest_api.add_default_permissions_to_project_by_gcap_obj_list(self.luid, self.obj_type,
                                                                                [True, ], [new_gcap_obj, ])
        # Update the internal representation from the newly returned permissions XML
        self.get_permissions_from_server(new_perms_xml)
        self.end_log_block()


class Project(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version=u"9.2", logger_obj=None):
        PublishedContent.__init__(self, luid, u"project", tableau_rest_api_obj, tableau_server_version,
                                  logger_obj=logger_obj)
        # projects in 9.2 have child workbook and datasource permissions
        if self.api_version != u"2.0":
                self.workbook_default = Workbook(self.luid, self.t_rest_api, default=True, logger_obj=logger_obj)
                self.datasource_default = PublishedContent(self.luid, u"datasource", self.t_rest_api,
                                                           default=True, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"project"]

    def lock_permissions(self):
        self.start_log_block()
        if self.api_version != u"2.0":
            self.t_rest_api.lock_project_permissions(self.luid)
        else:
            self.log(u"Permissions cannot be locked in 9.1 and previous")
        self.end_log_block()

    def unlock_permissions(self):
        self.start_log_block()
        if self.api_version != u"2.0":
            self.t_rest_api.unlock_project_permissions(self.luid)
        else:
            self.log(u"Permissions cannot be locked in 9.1 and previous")
        self.end_log_block()


class Workbook(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version=u"9.2", default=False, logger_obj=None):
        PublishedContent.__init__(self, luid, u"workbook", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"workbook"]


class Datasource(PublishedContent):
    def __init__(self, luid, tableau_rest_api_obj, tableau_server_version=u"9.2", default=False, logger_obj=None):
        PublishedContent.__init__(self, luid, u"datasource", tableau_rest_api_obj, tableau_server_version,
                                  default=default, logger_obj=logger_obj)
        self.__available_capabilities = self.available_capabilities[self.api_version][u"datasource"]
