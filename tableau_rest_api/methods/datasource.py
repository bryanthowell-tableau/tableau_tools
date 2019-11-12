from .rest_api_base import *
class DatasourceMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_datasources(self, project_name_or_luid: Optional[str] = None, all_fields: Optional[bool] = True,
                          updated_at_filter: Optional[UrlFilter] = None, created_at_filter: Optional[UrlFilter] = None,
                          tags_filter: Optional[UrlFilter] = None, datasource_type_filter: Optional[UrlFilter] = None,
                          sorts: Optional[List[Sort]] = None, fields: Optional[List[str]] = None) -> etree.Element:

        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource('datasources', filters=filters, sorts=sorts, fields=fields)

        # If there is a project filter
        if project_name_or_luid is not None:
            project_luid = self.query_project_luid(project_name_or_luid)
            dses_in_project = datasources.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            dses = etree.Element(self.ns_prefix + 'datasources')
            for ds in dses_in_project:
                dses.append(ds)
        else:
            dses = datasources

        self.end_log_block()
        return dses

    def query_datasources_json(self, all_fields: Optional[bool] = True, updated_at_filter: Optional[UrlFilter] = None,
                               created_at_filter: Optional[UrlFilter] = None, tags_filter: Optional[UrlFilter] = None,
                               datasource_type_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                               fields: Optional[List[str]] = None, page_number: Optional[int] = None) -> str:

        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource_json('datasources', filters=filters, sorts=sorts, fields=fields,
                                               page_number=page_number)

        self.end_log_block()
        return datasources

    # Tries to guess name or LUID, hope there is only one
    def query_datasource(self, ds_name_or_luid: str, proj_name_or_luid: Optional[str] = None) -> etree.Element:
        self.start_log_block()

        ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        ds = self.query_resource("datasources/{}".format(ds_luid))
        self.end_log_block()
        return ds

    # Filtering implemented in 2.2
    # query_workbook and query_workbook_luid can't be improved because filtering doesn't take a Project Name/LUID



    def query_datasource_content_url(self, datasource_name_or_luid: str,
                                     project_name_or_luid: Optional[str] = None) -> str:
        self.start_log_block()
        ds = self.query_datasource(datasource_name_or_luid, project_name_or_luid)
        content_url = ds.get('contentUrl')
        self.end_log_block()
        return content_url

    # query_datasource and query_datasource_luid can't be improved because filtering doesn't take a Project Name/LUID

    #
    # End Datasource Query Methods
    #

    # Can take collection or luid_string
    def delete_datasources(self, datasource_name_or_luid_s: Union[List[str], str]):
        self.start_log_block()
        datasources = self.to_list(datasource_name_or_luid_s)
        for datasource_name_or_luid in datasources:
            datasource_luid = self.query_datasource_luid(datasource_name_or_luid, None)
            url = self.build_api_url("datasources/{}".format(datasource_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def update_datasource(self, datasource_name_or_luid: str, datasource_project_name_or_luid: Optional[str] = None,
                          new_datasource_name: Optional[str] = None, new_project_luid: Optional[str] = None,
                          new_owner_luid: Optional[str] = None) -> etree.Element:
        self.start_log_block()
        datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = etree.Element("tsRequest")
        d = etree.Element("datasource")
        if new_datasource_name is not None:
            d.set('name', new_datasource_name)
        if new_project_luid is not None:
            p = etree.Element('project')
            p.set('id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = etree.Element('owner')
            o.set('id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url("datasources/{}".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def update_datasource_connection_by_luid(self, datasource_luid: str, new_server_address: Optional[str] = None,
                                             new_server_port: Optional[str] = None,
                                             new_connection_username: Optional[str] = None,
                                             new_connection_password: Optional[str] = None) -> etree.Element:
        self.start_log_block()
        tsr = self.__build_connection_update_xml(new_server_address, new_server_port,
                                                            new_connection_username,
                                                            new_connection_password)
        url = self.build_api_url("datasources/{}/connection".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # Do not include file extension. Without filename, only returns the response
    def download_datasource(self, ds_name_or_luid: str, filename_no_extension: str,
                            proj_name_or_luid: Optional[str] = None,
                            include_extract: Optional[bool] = True) -> str:
        self.start_log_block()

        ds_luid = self.query_datasource_luid(ds_name_or_luid, project_name_or_luid=proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.build_api_url("datasources/{}/content?includeExtract=False".format(ds_luid))
            else:
                url = self.build_api_url("datasources/{}/content".format(ds_luid))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.tds'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tdsx'
            self.log('Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:

            save_filename = filename_no_extension + extension
            save_file = open(save_filename, 'wb')
            save_file.write(ds)
            save_file.close()

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    #
    # Tags
    #

    # Tags can be scalar string or list
    def add_tags_to_datasource(self, ds_name_or_luid: str, tag_s: Union[List[str], str],
                               proj_name_or_luid: Optional[str] = None) -> etree.Element:
        self.start_log_block()

        ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)
        url = self.build_api_url("datasources/{}/tags".format(ds_luid))

        tsr = etree.Element("tsRequest")
        ts = etree.Element("tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def delete_tags_from_datasource(self, ds_name_or_luid: str, tag_s: Union[List[str], str],
                                    proj_name_or_luid: Optional[str] = None) -> int:
        self.start_log_block()
        tags = self.to_list(tag_s)
        ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.build_api_url("datasources/{}/tags/{}".format(ds_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count
    

class DatasourceMethods27(DatasourceMethods):
    def update_datasource(self, datasource_name_or_luid: str, datasource_project_name_or_luid: Optional[str] = None,
                          new_datasource_name: Optional[str] = None, new_project_luid: Optional[str] = None,
                          new_owner_luid: Optional[str] = None, certification_status: Optional[str] = None,
                          certification_note: Optional[str] = None) -> etree.Element:
        self.start_log_block()
        if certification_status not in [None, False, True]:
            raise InvalidOptionException('certification_status must be None, False, or True')

        datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = etree.Element("tsRequest")
        d = etree.Element("datasource")
        if new_datasource_name is not None:
            d.set('name', new_datasource_name)
        if certification_status is not None:
            d.set('isCertified', '{}'.format(str(certification_status).lower()))
        if certification_note is not None:
            d.set('certificationNote', certification_note)
        if new_project_luid is not None:
            p = etree.Element('project')
            p.set('id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = etree.Element('owner')
            o.set('id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url("datasources/{}".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

class DatasourceMethods28(DatasourceMethods27):
    pass


class DatasourceMethods30(DatasourceMethods28):
    pass

class DatasourceMethods31(DatasourceMethods30):
    pass

class DatasourceMethods32(DatasourceMethods31):
    pass

class DatasourceMethods33(DatasourceMethods32):
    pass

class DatasourceMethods34(DatasourceMethods33):
    pass

class DatasourceMethods35(DatasourceMethods34):
    pass

class DatasourceMethods36(DatasourceMethods35):
    pass