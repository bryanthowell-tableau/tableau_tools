from .tableau_rest_api_connection_27 import *


class TableauRestApiConnection28(TableauRestApiConnection27):
    def __init__(self, server, username, password, site_content_url=""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection27.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version("10.5")

    def get_published_project_object(self, project_name_or_luid, project_xml_obj=None):
        """
        :type project_name_or_luid: unicode
        :type project_xml_obj: project_xml_obj
        :rtype: Project28
        """
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)

        parent_project_luid = None
        if project_xml_obj.get('parentProjectId'):
            parent_project_luid = project_xml_obj.get('parentProjectId')

        proj_obj = Project28(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj,
                             parent_project_luid=parent_project_luid)
        return proj_obj

    def create_project(self, project_name, parent_project_name_or_luid=None, project_desc=None, locked_permissions=True,
                       publish_samples=False, no_return=False):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :type no_return: bool
        :type parent_project_name_or_luid: unicode
        :rtype: Project21
        """
        self.start_log_block()

        tsr = etree.Element("tsRequest")
        p = etree.Element("project")
        p.set("name", project_name)

        if project_desc is not None:
            p.set('description', project_desc)
        if locked_permissions is not False:
            p.set('contentPermissions', "LockedToProject")

        if parent_project_name_or_luid is not None:
            if self.is_luid(parent_project_name_or_luid):
                parent_project_luid = parent_project_name_or_luid
            else:
                parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        tsr.append(p)

        url = self.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                proj_obj = self.get_published_project_object(project_luid, new_project)

                return proj_obj
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid, parent_project_name_or_luid=None, new_project_name=None,
                       new_project_description=None, locked_permissions=None, publish_samples=False):
        """
        :type name_or_luid: unicode
        :type parent_project_name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :rtype: Project28
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            project_luid = name_or_luid
        else:
            project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element("tsRequest")
        p = etree.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if parent_project_name_or_luid is not None:
            if self.is_luid(parent_project_name_or_luid):
                parent_project_luid = parent_project_name_or_luid
            else:
                parent_project_luid = self.query_project_luid(parent_project_name_or_luid)
            p.set('parentProjectId', parent_project_luid)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def query_project(self, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :rtype: Project28
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint_with_filter('project',
                                                                                               project_name_or_luid))

        self.end_log_block()
        return proj

    def add_workbook_to_schedule(self, wb_name_or_luid, schedule_name_or_luid, proj_name_or_luid):
        """
        :type wb_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)

        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        t = etree.Element('task')
        er = etree.Element('extractRefresh')
        w = etree.Element('workbook')
        w.set('id', wb_luid)
        er.append(w)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/workbooks".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()

    def add_datasource_to_schedule(self, ds_name_or_luid, schedule_name_or_luid, proj_name_or_luid):
        """
        :type ds_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)

        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        t = etree.Element('task')
        er = etree.Element('extractRefresh')
        d = etree.Element('datasource')
        d.set('id', ds_luid)
        er.append(d)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/datasources".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()

    # Do not include file extension
    def save_view_pdf(self, wb_name_or_luid, view_name_or_luid, filename_no_extension,
                                         proj_name_or_luid=None, view_filter_map=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type view_filter_map: dict
        :rtype:
        """
        self.start_log_block()

        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            if wb_name_or_luid is None:
                raise InvalidOptionException('If looking up view by name, must include workbook')
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)
        try:
            if filename_no_extension.find('.pdf') == -1:
                filename_no_extension += '.pdf'
            save_file = open(filename_no_extension, 'wb')
            if view_filter_map is not None:
                final_filter_map = {}
                for key in view_filter_map:
                    new_key = "vf_{}".format(key)
                    final_filter_map[new_key] = view_filter_map[key]

                additional_url_params = "?" + urllib.parse.urlencode(final_filter_map)
            else:
                additional_url_params = ""
            url = self.build_api_url("views/{}/pdf{}".format(view_luid, additional_url_params))
            image = self.send_binary_get_request(url)
            save_file.write(image)
            save_file.close()
            self.end_log_block()

        # You might be requesting something that doesn't exist
        except RecoverableHTTPException as e:
            self.log("Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.end_log_block()
            raise

    def save_view_data_as_csv(self, wb_name_or_luid, view_name_or_luid, filename_no_extension=None,
                              proj_name_or_luid=None, view_filter_map=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type view_filter_map: dict
        :rtype:
        """
        self.start_log_block()

        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            if wb_name_or_luid is None:
                raise InvalidOptionException('If looking up view by name, must include workbook')
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)
        try:
            if view_filter_map is not None:
                final_filter_map = {}
                for key in view_filter_map:
                    new_key = "vf_{}".format(key)
                    final_filter_map[new_key] = view_filter_map[key]

                additional_url_params = "?" + urllib.parse.urlencode(final_filter_map)
            else:
                additional_url_params = ""
            url = self.build_api_url("views/{}/data{}".format(view_luid, additional_url_params))
            data = self.send_binary_get_request(url)
            if filename_no_extension is not None:
                if filename_no_extension.find('.csv') == -1:
                    filename_no_extension += '.csv'
                save_file = open(filename_no_extension, 'wb')
                save_file.write(data)
                save_file.close()
                self.end_log_block()
            else:
                self.end_log_block()
                # Do we need to do a codec conversion to make this unicode text?
                return data

        # You might be requesting something that doesn't exist
        except RecoverableHTTPException as e:
            self.log("Attempt to request data results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.end_log_block()
            raise

    def update_datasource_now(self, ds_name_or_luid, project_name_or_luid=False):
        """
        :type ds_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid) is False:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, project_name_or_luid=project_name_or_luid)
        else:
            ds_luid = ds_name_or_luid

        # Has an empty request but is POST because it makes a
        tsr = etree.Element('tsRequest')

        url = self.build_api_url('datasources/{}/refresh'.format(ds_luid))
        response = self.send_add_request(url, tsr)

        self.end_log_block()
        return response

    def update_workbook_now(self, wb_name_or_luid, project_name_or_luid=False):
        """
        :type wb_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid) is False:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid=project_name_or_luid)
        else:
            wb_luid = wb_name_or_luid

        # Has an empty request but is POST because it makes a
        tsr = etree.Element('tsRequest')

        url = self.build_api_url('workbooks/{}/refresh'.format(wb_luid))
        response = self.send_add_request(url, tsr)

        self.end_log_block()
        return response

    def run_extract_refresh_for_workbook(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :return:
        """
        return self.update_workbook_now(wb_name_or_luid, proj_name_or_luid)

    # Use the specific refresh rather than the schedule task in 2.8
    def run_extract_refresh_for_datasource(self, ds_name_or_luid, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :rtype: etree.Element
        """
        return self.update_datasource_now(ds_name_or_luid, proj_name_or_luid)
