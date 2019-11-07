from .rest_api_base import *
class WorkbookMethods(TableauRestApiBase):
    #
    # Start Workbook Querying Methods
    #

    # Filtering implemented for workbooks in 2.2
    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid=None, project_name_or_luid=None, all_fields=True, created_at_filter=None, updated_at_filter=None,
                        owner_name_filter=None, tags_filter=None, sorts=None, fields=None):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: List[Sort]
        :type fields: List[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource("users/{}/workbooks".format(user_luid))
        else:
            wbs = self.query_resource("workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields)

        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            wbs_in_project = wbs.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            wbs = etree.Element(self.ns_prefix + 'workbooks')
            for wb in wbs_in_project:
                wbs.append(wb)
        self.end_log_block()
        return wbs

    def query_workbooks_for_user(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        wbs = self.query_workbooks(username_or_luid)
        self.end_log_block()
        return wbs

    def query_workbooks_json(self, username_or_luid=None, project_name_or_luid=None, all_fields=True,
                             created_at_filter=None, updated_at_filter=None, owner_name_filter=None,
                             tags_filter=None, sorts=None, fields=None, page_number=None):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: List[Sort]
        :type fields: List[unicode]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource_json("users/{}/workbooks".format(user_luid), sorts=sorts, filters=filters,
                                           fields=fields, page_number=page_number)
        else:
            wbs = self.query_resource_json("workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields,
                                           page_number=page_number)

        self.end_log_block()
        return wbs

    # Because a workbook can have the same pretty name in two projects, requires more logic
    def query_workbook(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        workbooks = self.query_workbooks(username_or_luid)
        if self.is_luid(wb_name_or_luid):
            workbooks_with_name = self.query_resource("workbooks/{}".format(wb_name_or_luid))
        else:
            workbooks_with_name = workbooks.findall('.//t:workbook[@name="{}"]'.format(wb_name_or_luid), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException("No workbook found for username '{}' named {}".format(username_or_luid, wb_name_or_luid))
        elif proj_name_or_luid is None:
            if len(workbooks_with_name) == 1:
                wb_luid = workbooks_with_name[0].get("id")
                wb = self.query_resource("workbooks/{}".format(wb_luid))
                self.end_log_block()
                return wb
            else:
                self.end_log_block()
                raise MultipleMatchesFoundException('More than one workbook found by name {} without a project specified').format(wb_name_or_luid)
        else:
            if self.is_luid(proj_name_or_luid):
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException('No workbook found with name {} in project {}'.format(wb_name_or_luid, proj_name_or_luid))
            wb_luid = wb_in_proj[0].get("id")
            wb = self.query_resource("workbooks/{}".format(wb_luid))
            self.end_log_block()
            return wb

    def query_workbook_luid(self, wb_name, proj_name_or_luid=None, username_or_luid=None):
        """
        :type username_or_luid: unicode
        :type wb_name: unicode
        :type proj_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if username_or_luid is None:
            username_or_luid = self.user_luid
        workbooks = self.query_workbooks(username_or_luid)
        workbooks_with_name = workbooks.findall('.//t:workbook[@name="{}"]'.format(wb_name), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException("No workbook found for username '{}' named {}".format(username_or_luid, wb_name))
        elif len(workbooks_with_name) == 1:
            wb_luid = workbooks_with_name[0].get("id")
            self.end_log_block()
            return wb_luid
        elif len(workbooks_with_name) > 1 and proj_name_or_luid is not None:
            if self.is_luid(proj_name_or_luid):
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@id="{}"]/..'.format(wb_name, proj_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name, proj_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException('No workbook found with name {} in project {}').format(wb_name, proj_name_or_luid)
            wb_luid = wb_in_proj[0].get("id")
            self.end_log_block()
            return wb_luid
        else:
            self.end_log_block()
            raise MultipleMatchesFoundException('More than one workbook found by name {} without a project specified').format(wb_name)

    def query_workbooks_in_project(self, project_name_or_luid, username_or_luid=None):
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            project_luid = project_name_or_luid
        else:
            project_luid = self.query_project_luid(project_name_or_luid)
        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        workbooks = self.query_workbooks(user_luid)
        # This brings back the workbook itself
        wbs_in_project = workbooks.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
        wbs = etree.Element(self.ns_prefix + 'workbooks')
        for wb in wbs_in_project:
            wbs.append(wb)
        self.end_log_block()
        return wbs

    def update_workbook(self, workbook_name_or_luid, workbook_project_name_or_luid, new_project_luid=None,
                        new_owner_luid=None, show_tabs=True):
        """
        :type workbook_name_or_luid: unicode
        :type workbook_project_name_or_luid: unicode
        :type new_project_luid: unicode
        :type new_owner_luid: unicode
        :type show_tabs: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(workbook_name_or_luid):
            workbook_luid = workbook_name_or_luid
        else:
            workbook_luid = self.query_workbook_luid(workbook_name_or_luid, workbook_project_name_or_luid,
                                                     self.username)
        tsr = etree.Element("tsRequest")
        w = etree.Element("workbook")
        w.set('showTabs', str(show_tabs).lower())
        if new_project_luid is not None:
            p = etree.Element('project')
            p.set('id', new_project_luid)
            w.append(p)

        if new_owner_luid is not None:
            o = etree.Element('owner')
            o.set('id', new_owner_luid)
            w.append(o)
        tsr.append(w)

        url = self.build_api_url("workbooks/{}".format(workbook_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # To do this, you need the workbook's connection_luid. Seems to only come from "Query Workbook Connections",
    # which does not return any names, just types and LUIDs
    def update_workbook_connection_by_luid(self, wb_luid, connection_luid, new_server_address=None,
                                           new_server_port=None,
                                           new_connection_username=None, new_connection_password=None):
        """
        :type wb_luid: unicode
        :type connection_luid: unicode
        :type new_server_address: unicode
        :type new_server_port: unicode
        :type new_connection_username: unicode
        :type new_connection_password: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.__build_connection_update_xml(new_server_address, new_server_port, new_connection_username,
                                                 new_connection_password)
        url = self.build_api_url("workbooks/{}/connections/{}".format(wb_luid, connection_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # Can take collection or luid_string
    def delete_workbooks(self, wb_name_or_luid_s):
        """
        :type wb_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        wbs = self.to_list(wb_name_or_luid_s)
        for wb in wbs:
            # Check if workbook_luid exists
            if self.is_luid(wb):
                wb_luid = wb
            else:
                wb_luid = self.query_workbook_luid(wb)
            url = self.build_api_url("workbooks/{}".format(wb_luid))
            self.send_delete_request(url)
        self.end_log_block()

