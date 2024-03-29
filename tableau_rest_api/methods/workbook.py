from .rest_api_base import *
from ..published_content import Workbook


class WorkbookMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def get_published_workbook_object(self, workbook_name_or_luid: str,
                                      project_name_or_luid: Optional[str] = None) -> Workbook:
        luid = self.rest.query_workbook_luid(workbook_name_or_luid, project_name_or_luid)
        wb_obj = Workbook(luid=luid, tableau_rest_api_obj=self.rest,
                          default=False, logger_obj=self.logger)
        return wb_obj

    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid: Optional[str] = None, project_name_or_luid: Optional[str] = None,
                        all_fields: bool = True, filters: Optional[List[UrlFilter]] = None, sorts: Optional[List[Sort]] = None,
                        fields: Optional[List[str]] = None) -> ET.Element:
        self.rest.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is not None:
            user_luid = self.rest.query_user_luid(username_or_luid)
            wbs = self.rest.query_resource("users/{}/workbooks".format(user_luid))
        else:
            wbs = self.rest.query_resource("workbooks", sorts=sorts, filters=filters, fields=fields)

        if project_name_or_luid is not None:
            project_luid = self.rest.query_project_luid(project_name_or_luid)
            wbs_in_project = wbs.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.rest.ns_map)
            wbs = ET.Element(self.rest.ns_prefix + 'workbooks')
            for wb in wbs_in_project:
                wbs.append(wb)
        self.rest.end_log_block()
        return wbs

    def query_workbooks_for_user(self, username_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        wbs = self.query_workbooks(username_or_luid)
        self.rest.end_log_block()
        return wbs

    def query_workbooks_json(self, username_or_luid: Optional[str] = None, all_fields: bool = True,
                             filters: Optional[List[UrlFilter]] = None, sorts: Optional[List[Sort]] = None,
                             fields: Optional[List[str]] = None, page_number: Optional[int] = None) -> Dict:
        self.rest.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is not None:
            user_luid = self.rest.query_user_luid(username_or_luid)
            wbs = self.rest.query_resource_json("users/{}/workbooks".format(user_luid), sorts=sorts, filters=filters,
                                           fields=fields, page_number=page_number)
        else:
            wbs = self.rest.query_resource_json("workbooks", sorts=sorts, filters=filters, fields=fields,
                                           page_number=page_number)

        self.rest.end_log_block()
        return wbs

    # Because a workbook can have the same pretty name in two projects, requires more logic
    def query_workbook(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None,
                       username_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        workbooks = self.rest.query_workbooks(username_or_luid)
        if self.rest.is_luid(wb_name_or_luid):
            workbooks_with_name = self.rest.query_resource("workbooks/{}".format(wb_name_or_luid))
        else:
            workbooks_with_name = workbooks.findall('.//t:workbook[@name="{}"]'.format(wb_name_or_luid), self.rest.ns_map)
        if len(workbooks_with_name) == 0:
            self.rest.end_log_block()
            raise NoMatchFoundException("No workbook found for username '{}' named {}".format(username_or_luid, wb_name_or_luid))
        elif proj_name_or_luid is None:
            if len(workbooks_with_name) == 1:
                wb_luid = workbooks_with_name[0].get("id")
                wb = self.rest.query_resource("workbooks/{}".format(wb_luid))
                self.rest.end_log_block()
                return wb
            else:
                self.rest.end_log_block()
                raise MultipleMatchesFoundException('More than one workbook found by name {} without a project specified').format(wb_name_or_luid)
        else:
            if self.rest.is_luid(proj_name_or_luid):
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.rest.ns_map)
            else:
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.rest.ns_map)
            if len(wb_in_proj) == 0:
                self.rest.end_log_block()
                raise NoMatchFoundException('No workbook found with name {} in project {}'.format(wb_name_or_luid, proj_name_or_luid))
            wb_luid = wb_in_proj[0].get("id")
            wb = self.rest.query_resource("workbooks/{}".format(wb_luid))
            self.rest.end_log_block()
            return wb

    def query_workbooks_in_project(self, project_name_or_luid: str, username_or_luid: Optional[str] = None):
        self.rest.start_log_block()

        project_luid = self.rest.query_project_luid(project_name_or_luid)
        if username_or_luid is not None:
            user_luid = self.rest.query_user_luid(username_or_luid)
            workbooks = self.query_workbooks(user_luid)
        else:
            workbooks = self.query_workbooks()
        # This brings back the workbook itself
        wbs_in_project = workbooks.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.rest.ns_map)
        wbs = ET.Element(self.rest.ns_prefix + 'workbooks')
        for wb in wbs_in_project:
            wbs.append(wb)
        self.rest.end_log_block()
        return wbs

    def update_workbook(self, workbook_name_or_luid: str, workbook_project_name_or_luid: Optional[str] = None,
                        new_project_name_or_luid: Optional[str] = None, new_owner_username_or_luid: Optional[str] = None,
                        show_tabs: bool = True) -> ET.Element:
        self.rest.start_log_block()
        workbook_luid = self.rest.query_workbook_luid(workbook_name_or_luid, workbook_project_name_or_luid)

        tsr = ET.Element("tsRequest")
        w = ET.Element("workbook")
        w.set('showTabs', str(show_tabs).lower())

        if new_project_name_or_luid is not None:
            new_project_luid = self.rest.query_project_luid(new_project_name_or_luid)
            p = ET.Element('project')
            p.set('id', new_project_luid)
            w.append(p)

        if new_owner_username_or_luid is not None:
            new_owner_luid = self.rest.query_user_luid(new_owner_username_or_luid)
            o = ET.Element('owner')
            o.set('id', new_owner_luid)
            w.append(o)
        tsr.append(w)

        url = self.rest.build_api_url("workbooks/{}".format(workbook_luid))
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response

    # To do this, you need the workbook's connection_luid. Seems to only come from "Query Workbook Connections",
    # which does not return any names, just types and LUIDs
    def update_workbook_connection_by_luid(self, wb_luid: str, connection_luid: str,
                                           new_server_address: Optional[str] = None,
                                           new_server_port: Optional[str] = None,
                                           new_connection_username: Optional[str] = None,
                                           new_connection_password: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        tsr = self.rest.__build_connection_update_xml(new_server_address, new_server_port, new_connection_username,
                                                 new_connection_password)
        url = self.rest.build_api_url("workbooks/{}/connections/{}".format(wb_luid, connection_luid))
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response

    def query_view_image(self, view_name_or_luid: Optional[str] = None,
                         high_resolution: bool = False, view_filter_map: Optional[Dict] = None,
                         wb_name_or_luid: Optional[str] = None, proj_name_or_luid: Optional[str] = None) -> bytes:
        self.rest.start_log_block()
        image = self.rest._query_data_file('image', view_name_or_luid=view_name_or_luid, high_resolution=high_resolution,
                                      view_filter_map=view_filter_map, wb_name_or_luid=wb_name_or_luid,
                                      proj_name_or_luid=proj_name_or_luid)
        self.rest.end_log_block()
        return image

    def save_view_image(self, wb_name_or_luid: Optional[str] = None, view_name_or_luid: Optional[str] = None,
                        filename_no_extension: Optional[str] = None,
                        proj_name_or_luid: Optional[str] = None, view_filter_map: Optional[Dict] = None) -> str:

        self.rest.start_log_block()
        data = self.query_view_image(wb_name_or_luid=wb_name_or_luid, view_name_or_luid=view_name_or_luid,
                                     proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)

        if filename_no_extension is not None:
            if filename_no_extension.find('.png') == -1:
                filename_no_extension += '.png'
            try:
                save_file = open(filename_no_extension, 'wb')
                save_file.write(data)
                save_file.close()
                self.rest.end_log_block()
                return filename_no_extension
            except IOError:
                self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
                self.rest.end_log_block()
                raise
        else:
            raise InvalidOptionException(
                'This method is for saving response to file. Must include filename_no_extension parameter')


    def query_workbook_views(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None,
                             username_or_luid: Optional[str] = None, usage: bool = False) -> ET.Element:
        self.rest.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        vws = self.rest.query_resource("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        self.rest.end_log_block()
        return vws

    def query_workbook_views_json(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None,
                                  username_or_luid: Optional[str] = None, usage: bool = False,
                                  page_number: Optional[int] = None) -> Dict:
        self.rest.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        url_params = self.rest.build_url_parameter_string(map_dict={'includeUsageStatistics': str(usage).lower()})
        vws = self.rest.query_resource_json("workbooks/{}/views".format(wb_luid),
                                       additional_url_ending=url_params, page_number=page_number)
        self.rest.end_log_block()
        return vws

    def query_workbook_view(self, wb_name_or_luid, view_name_or_luid: Optional[str] = None,
                            view_content_url: Optional[str] = None, proj_name_or_luid: Optional[str] = None,
                            username_or_luid: Optional[str] = None, usage: bool = False) -> List[ET.Element]:

        self.rest.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        vws = self.rest.query_resource("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))

        if view_content_url is not None:
            views_with_name = vws.findall('.//t:view[@contentUrl="{}"]'.format(view_content_url), self.rest.ns_map)
        elif self.rest.is_luid(view_name_or_luid):
            views_with_name = vws.findall('.//t:view[@id="{}"]'.format(view_name_or_luid), self.rest.ns_map)
        else:
            views_with_name = vws.findall('.//t:view[@name="{}"]'.format(view_name_or_luid), self.rest.ns_map)
        if len(views_with_name) == 0:
            self.rest.end_log_block()
            raise NoMatchFoundException('No view found with name {} in workbook {}'.format(view_name_or_luid, wb_name_or_luid))
        elif len(views_with_name) > 1:
            self.rest.end_log_block()
            raise MultipleMatchesFoundException(
                'More than one view found by name {} in workbook {}. Use view_content_url parameter'.format(view_name_or_luid, wb_name_or_luid))
        self.rest.end_log_block()
        return views_with_name

    # This should be the key to updating the connections in a workbook. Seems to return
    # LUIDs for connections and the datatypes, but no way to distinguish them
    def query_workbook_connections(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None,
                                   username_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        conns = self.rest.query_resource("workbooks/{}/connections".format(wb_luid))
        self.rest.end_log_block()
        return conns

    def query_views(self, all_fields: bool = True, usage: bool = False,
                         filters: Optional[List[UrlFilter]] = None, sorts: Optional[Sort] = None,
                         fields: Optional[List[str]] = None) -> ET.Element:
        self.rest.start_log_block()

        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')

        vws = self.rest.query_resource("views", filters=filters, sorts=sorts, fields=fields,
                                  additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()))
        self.rest.end_log_block()
        return vws

    def query_views_json(self, all_fields: bool = True, usage: bool = False,
                         filters: Optional[List[UrlFilter]] = None, sorts: Optional[Sort] = None,
                         fields: Optional[List[str]] = None, page_number: Optional[int] = None) -> Dict:
        self.rest.start_log_block()

        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')

        vws = self.rest.query_resource_json("views", filters=filters, sorts=sorts, fields=fields,
                                       additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()),
                                       page_number=page_number)
        self.rest.end_log_block()
        return vws

    def query_view(self, vw_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        vw = self.rest.query_single_element_from_endpoint_with_filter('view', vw_name_or_luid)
        self.rest.end_log_block()
        return vw

    # Can take collection or luid_string
    def delete_workbooks(self, wb_name_or_luid_s: Union[List[str], str]):
        self.rest.start_log_block()
        wbs = self.rest.to_list(wb_name_or_luid_s)
        for wb in wbs:
            wb_luid = self.rest.query_workbook_luid(wb)
            url = self.rest.build_api_url("workbooks/{}".format(wb_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_workbook(self, wb_name_or_luid: str, filename_no_extension: str,
                          proj_name_or_luid: Optional[str] = None, include_extract: bool = True) -> str:
        self.rest.start_log_block()

        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.rest.build_api_url("workbooks/{}/content?includeExtract=False".format(wb_luid))
            else:
                url = self.rest.build_api_url("workbooks/{}/content".format(wb_luid))
            wb = self.rest.send_binary_get_request(url)
            extension = None
            if self.rest._last_response_content_type.find('application/xml') != -1:
                extension = '.twb'
            elif self.rest._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.twbx'
            if extension is None:
                raise IOError('File extension could not be determined')
            self.rest.log(
                'Response type was {} so extension will be {}'.format(self.rest._last_response_content_type, extension))
        except RecoverableHTTPException as e:
            self.rest.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.rest.end_log_block()
            raise
        except:
            self.rest.end_log_block()
            raise
        try:

            save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(wb)
            save_file.close()

        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.rest.end_log_block()
        return save_filename

    # In 3.2, you can hide views from publishing
    def publish_workbook(self, workbook_filename: str, workbook_name: str, project_obj: Project,
                         overwrite: bool = False, async_publish: bool = False,
                         connection_username: Optional[str] = None,
                         connection_password: Optional[str] = None, save_credentials: bool = True,
                         show_tabs: bool = True, check_published_ds: bool = True,
                         oauth_flag: bool = False, views_to_hide_list: Optional[List[str]] = None) -> str:

        project_luid = project_obj.luid
        xml = self.rest._publish_content(content_type='workbook', content_filename=workbook_filename,
                                   content_name=workbook_name, project_luid=project_luid,
                                   url_params={"overwrite": overwrite, "asJob": async_publish},
                                   connection_username=connection_username,
                                   connection_password=connection_password,
                                   save_credentials=save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag,
                                   views_to_hide_list=views_to_hide_list)
        if async_publish is True:
            job = xml.findall('.//t:job', self.rest.ns_map)
            return job[0].get('id')
        else:
            workbook = xml.findall('.//t:workbook', self.ns_map)
            return workbook[0].get('id')

    #
    # Image and PDF endpoints
    #

    # You must pass in the wb name because the endpoint needs it (although, you could potentially look up the
    # workbook LUID from the view LUID
    def query_view_preview_image(self, wb_name_or_luid: str, view_name_or_luid: str,
                                         proj_name_or_luid: Optional[str] = None) -> bytes:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        view_luid = self.rest.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)
        try:
            url = self.rest.build_api_url("workbooks/{}/views/{}/previewImage".format(wb_luid, view_luid))
            image = self.rest.send_binary_get_request(url)

            self.rest.end_log_block()
            return image

        # You might be requesting something that doesn't exist
        except RecoverableHTTPException as e:
            self.rest.log("Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                                          e.tableau_error_code))
            self.rest.end_log_block()
            raise


    # Do not include file extension
    # Just an alias but it matches the naming of the current reference guide (2019.1)
    def save_view_preview_image(self, wb_name_or_luid: str, view_name_or_luid: str, filename_no_extension: str,
                                         proj_name_or_luid: Optional[str] = None):

        self.save_workbook_view_preview_image(wb_name_or_luid, view_name_or_luid, filename_no_extension,
                                         proj_name_or_luid)

    def save_workbook_view_preview_image(self, wb_name_or_luid: str, view_name_or_luid: str, filename_no_extension: str,
                                         proj_name_or_luid: Optional[str] = None):

        self.rest.start_log_block()
        image = self.query_view_preview_image(wb_name_or_luid=wb_name_or_luid, view_name_or_luid=view_name_or_luid,
                                              proj_name_or_luid=proj_name_or_luid)
        if filename_no_extension.find('.png') == -1:
            filename_no_extension += '.png'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(image)
            save_file.close()
            self.rest.end_log_block()

        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.rest.end_log_block()
            raise

    def query_workbook_preview_image(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None) -> bytes:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            url = self.rest.build_api_url("workbooks/{}/previewImage".format(wb_luid))
            image = self.rest.send_binary_get_request(url)
            self.rest.end_log_block()
            return image

        # You might be requesting something that doesn't exist, but unlikely
        except RecoverableHTTPException as e:
            self.rest.log("Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.rest.end_log_block()
            raise

    # Do not include file extension
    def save_workbook_preview_image(self, wb_name_or_luid: str, filename_no_extension: str,
                                    proj_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()
        image = self.query_workbook_preview_image(wb_name_or_luid=wb_name_or_luid, proj_name_or_luid=proj_name_or_luid)
        if filename_no_extension.find('.png') == -1:
            filename_no_extension += '.png'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(image)
            save_file.close()
            self.rest.end_log_block()

        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.rest.end_log_block()
            raise

    #
    # Tags
    #

    # Tags can be scalar string or list
    def add_tags_to_workbook(self, wb_name_or_luid: str, tag_s: List[str],
                             proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        url = self.rest.build_api_url("workbooks/{}/tags".format(wb_luid))

        tsr = ET.Element("tsRequest")
        ts = ET.Element("tags")
        tags = self.rest.to_list(tag_s)
        for tag in tags:
            t = ET.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return tag_response

    def delete_tags_from_workbook(self, wb_name_or_luid: str, tag_s: Union[List[str], str]) -> int:
        self.rest.start_log_block()
        tags = self.rest.to_list(tag_s)
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.rest.build_api_url("workbooks/{}/tags/{}".format(wb_luid, tag))
            deleted_count += self.rest.send_delete_request(url)
        self.rest.end_log_block()
        return deleted_count

    # Tags can be scalar string or list
    def add_tags_to_view(self, view_name_or_luid: str, workbook_name_or_luid: str, tag_s: List[str],
                         proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        vw_luid = self.rest.query_workbook_view_luid(workbook_name_or_luid, view_name_or_luid, proj_name_or_luid)
        url = self.rest.build_api_url("views/{}/tags".format(vw_luid))

        tsr = ET.Element("tsRequest")
        ts = ET.Element("tags")
        tags = self.rest.to_list(tag_s)
        for tag in tags:
            t = ET.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return tag_response

    def delete_tags_from_view(self, view_name_or_luid: str, workbook_name_or_luid: str, tag_s: Union[List[str], str],
                              proj_name_or_luid: Optional[str] = None) -> int:

        self.rest.start_log_block()
        tags = self.rest.to_list(tag_s)
        if self.rest.is_luid(view_name_or_luid):
            vw_luid = view_name_or_luid
        else:
            vw_luid = self.rest.query_workbook_view_luid(view_name_or_luid, workbook_name_or_luid, proj_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.rest.build_api_url("views/{}/tags/{}".format(vw_luid, tag))
            deleted_count += self.rest.send_delete_request(url)
        self.rest.end_log_block()
        return deleted_count

    def query_view_pdf(self, wb_name_or_luid: str, view_name_or_luid: str, proj_name_or_luid=None,
                       view_filter_map=None):
        self.rest.start_log_block()
        pdf = self.rest._query_data_file('pdf', view_name_or_luid=view_name_or_luid, wb_name_or_luid=wb_name_or_luid,
                                    proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)
        self.rest.end_log_block()
        return pdf

    # Do not include file extension
    def save_view_pdf(self, wb_name_or_luid: str, view_name_or_luid: str, filename_no_extension: str,
                      proj_name_or_luid: Optional[str] = None, view_filter_map: Optional[Dict] = None) -> str:
        self.rest.start_log_block()
        pdf = self.query_view_pdf(view_name_or_luid=view_name_or_luid, wb_name_or_luid=wb_name_or_luid,
                                  proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)

        if filename_no_extension.find('.pdf') == -1:
            filename_no_extension += '.pdf'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(pdf)
            save_file.close()
            self.rest.end_log_block()
            return filename_no_extension
        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.rest.end_log_block()
            raise

    def query_view_data(self, wb_name_or_luid: Optional[str] = None, view_name_or_luid: Optional[str] = None,
                        proj_name_or_luid: Optional[str] = None, view_filter_map: Optional[Dict] = None) -> bytes:
        self.rest.start_log_block()
        csv = self.rest._query_data_file('data', view_name_or_luid=view_name_or_luid, wb_name_or_luid=wb_name_or_luid,
                                    proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)
        self.rest.end_log_block()
        return csv

    def save_view_data_as_csv(self, wb_name_or_luid: Optional[str] = None, view_name_or_luid: Optional[str] = None,
                              filename_no_extension: Optional[str] = None, proj_name_or_luid: Optional[str] = None,
                              view_filter_map: Optional[Dict] = None) -> str:

        self.rest.start_log_block()
        data = self.query_view_data(wb_name_or_luid=wb_name_or_luid, view_name_or_luid=view_name_or_luid,
                                    proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)

        if filename_no_extension is not None:
            if filename_no_extension.find('.csv') == -1:
                filename_no_extension += '.csv'
            try:
                save_file = open(filename_no_extension, 'wb')
                save_file.write(data)
                save_file.close()
                self.rest.end_log_block()
                return filename_no_extension
            except IOError:
                self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
                self.rest.end_log_block()
                raise
        else:
            raise InvalidOptionException(
                'This method is for saving response to file. Must include filename_no_extension parameter')



class WorkbookMethods34(WorkbookMethods):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest = rest_api_base

    def query_view_image(self, view_name_or_luid: Optional[str] = None, high_resolution: bool = False,
                         view_filter_map: Optional[Dict] = None, wb_name_or_luid: Optional[str] = None,
                         proj_name_or_luid: Optional[str] = None,  max_age_minutes: Optional[int] = None) -> bytes:
        self.rest.start_log_block()
        image = self.rest._query_data_file('image', view_name_or_luid=view_name_or_luid, high_resolution=high_resolution,
                                      view_filter_map=view_filter_map, wb_name_or_luid=wb_name_or_luid,
                                      proj_name_or_luid=proj_name_or_luid, max_age_minutes=max_age_minutes)
        self.rest.end_log_block()
        return image

    def query_workbook_pdf(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None,
                       page_orientation: str = 'Portrait', page_type: str = 'Legal'):
        self.rest.start_log_block()

        pdf = self.rest._query_data_file('pdf', wb_name_or_luid=wb_name_or_luid,
                                    proj_name_or_luid=proj_name_or_luid, page_type=page_type,
                                    page_orientation=page_orientation)
        self.rest.end_log_block()
        return pdf

    def save_workbook_pdf(self, wb_name_or_luid: str, filename_no_extension: str,
                          proj_name_or_luid: Optional[str] = None, page_orientation: str = 'Portrait',
                          page_type: str = 'Legal') -> str:
        self.rest.start_log_block()
        pdf = self.query_workbook_pdf(wb_name_or_luid=wb_name_or_luid,
                                  proj_name_or_luid=proj_name_or_luid, page_type=page_type,
                                    page_orientation=page_orientation )

        if filename_no_extension.find('.pdf') == -1:
            filename_no_extension += '.pdf'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(pdf)
            save_file.close()
            self.rest.end_log_block()
            return filename_no_extension
        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.rest.end_log_block()
            raise

    def publish_workbook(self, workbook_filename: str, workbook_name: str, project_obj: Project,
                         overwrite: bool = False, async_publish: bool = False,
                         connection_username: Optional[str] = None,
                         connection_password: Optional[str] = None, save_credentials: bool = True,
                         show_tabs: bool = True, check_published_ds: bool = True,
                         oauth_flag: bool = False, views_to_hide_list: Optional[List[str]] = None,
                         generate_thumbnails_as_username_or_luid: Optional[str] = None):
        project_luid = project_obj.luid
        xml = self.rest._publish_content(content_type='workbook', content_filename=workbook_filename,
                                   content_name=workbook_name, project_luid=project_luid,
                                   url_params={"overwrite": overwrite, "asJob": async_publish},
                                   connection_username=connection_username,
                                   connection_password=connection_password,
                                   save_credentials=save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag,
                                   views_to_hide_list=views_to_hide_list,
                                   generate_thumbnails_as_username_or_luid=generate_thumbnails_as_username_or_luid)
        if async_publish is True:
            job = xml.findall('.//t:job', self.rest.ns_map)
            return job[0].get('id')
        else:
            workbook = xml.findall('.//t:workbook', self.rest.ns_map)
            return workbook[0].get('id')


class WorkbookMethods37(WorkbookMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase37):
        self.rest = rest_api_base

    # Recommendations Methods


class WorkbookMethods38(WorkbookMethods37):
    def __init__(self, rest_api_base: TableauRestApiBase38):
        self.rest = rest_api_base

    # Download Workbook Powerpoint
    def query_workbook_powerpoint(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()

        powerpoint = self.rest._query_data_file('powerpoint', wb_name_or_luid=wb_name_or_luid,
                                    proj_name_or_luid=proj_name_or_luid)
        self.rest.end_log_block()
        return powerpoint

    def save_workbook_powerpoint(self, wb_name_or_luid: str, filename_no_extension: str,
                          proj_name_or_luid: Optional[str] = None) -> str:
        self.rest.start_log_block()
        powerpoint = self.query_workbook_powerpoint(wb_name_or_luid=wb_name_or_luid,
                                  proj_name_or_luid=proj_name_or_luid)

        if filename_no_extension.find('.pptx') == -1:
            filename_no_extension += '.pptx'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(powerpoint)
            save_file.close()
            self.rest.end_log_block()
            return filename_no_extension
        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.rest.end_log_block()
            raise

    def update_workbook(self, workbook_name_or_luid: str, workbook_project_name_or_luid: Optional[str] = None,
                        new_project_name_or_luid: Optional[str] = None, new_owner_username_or_luid: Optional[str] = None,
                        show_tabs: bool = True, data_acceleration_enabled: Optional[bool] = None,
                        accelerate_now: Optional[bool] = None) -> ET.Element:
        self.rest.start_log_block()
        workbook_luid = self.rest.query_workbook_luid(workbook_name_or_luid, workbook_project_name_or_luid)

        tsr = ET.Element("tsRequest")
        w = ET.Element("workbook")
        w.set('showTabs', str(show_tabs).lower())

        if new_project_name_or_luid is not None:
            new_project_luid = self.rest.query_project_luid(new_project_name_or_luid)
            p = ET.Element('project')
            p.set('id', new_project_luid)
            w.append(p)

        if new_owner_username_or_luid is not None:
            new_owner_luid = self.rest.query_user_luid(new_owner_username_or_luid)
            o = ET.Element('owner')
            o.set('id', new_owner_luid)
            w.append(o)

        if data_acceleration_enabled is not None:
            dac = ET.Element('dataAccelerationConfig')
            dac.set('accelerationEnabled', str(data_acceleration_enabled).lower())
            if accelerate_now is not None:
                dac.set('accelerateNow', str(accelerate_now).lower())
            w.append(dac)

        tsr.append(w)

        url = self.rest.build_api_url("workbooks/{}".format(workbook_luid))
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response