from .rest_api_base import *
from ..published_content import Datasource, Datasource28
from ...tableau_rest_xml import TableauRestXml

class DatasourceMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def get_published_datasource_object(self, datasource_name_or_luid: str,
                                        project_name_or_luid: Optional[str] = None) -> Datasource:
        luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        ds_obj = Datasource(luid=luid, tableau_rest_api_obj=self,
                            default=False, logger_obj=self.logger)
        return ds_obj

    def query_datasources(self, project_name_or_luid: Optional[str] = None, all_fields: Optional[bool] = True,
                          updated_at_filter: Optional[UrlFilter] = None, created_at_filter: Optional[UrlFilter] = None,
                          tags_filter: Optional[UrlFilter] = None, datasource_type_filter: Optional[UrlFilter] = None,
                          sorts: Optional[List[Sort]] = None, fields: Optional[List[str]] = None) -> ET.Element:

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
            dses_in_project = datasources.findall('.//t:project[@id="{}"]/..'.format(project_luid), TableauRestXml.ns_map)
            dses = ET.Element(self.ns_prefix + 'datasources')
            for ds in dses_in_project:
                dses.append(ds)
        else:
            dses = datasources

        self.end_log_block()
        return dses

    def query_datasources_json(self, all_fields: Optional[bool] = True, updated_at_filter: Optional[UrlFilter] = None,
                               created_at_filter: Optional[UrlFilter] = None, tags_filter: Optional[UrlFilter] = None,
                               datasource_type_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                               fields: Optional[List[str]] = None, page_number: Optional[int] = None) -> Dict:

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
    def query_datasource(self, ds_name_or_luid: str, proj_name_or_luid: Optional[str] = None) -> ET.Element:
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

    def delete_datasources(self, datasource_name_or_luid_s: Union[List[str], str]):
        self.start_log_block()
        datasources = self.to_list(datasource_name_or_luid_s)
        for datasource_name_or_luid in datasources:
            datasource_luid = self.query_datasource_luid(datasource_name_or_luid, None)
            url = self.build_api_url("datasources/{}".format(datasource_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def update_datasource(self, datasource_name_or_luid: str, datasource_project_name_or_luid: Optional[str] = None,
                          new_datasource_name: Optional[str] = None, new_project_name_or_luid: Optional[str] = None,
                          new_owner_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = ET.Element("tsRequest")
        d = ET.Element("datasource")
        if new_datasource_name is not None:
            d.set('name', new_datasource_name)
        if new_project_name_or_luid is not None:
            new_project_luid = self.query_project_luid(new_project_name_or_luid)
            p = ET.Element('project')
            p.set('id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = ET.Element('owner')
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
                                             new_connection_password: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = self.rest_api_base.__build_connection_update_xml(new_server_address, new_server_port,
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

    def publish_datasource(self, ds_filename: str, ds_name: str, project_obj: Project,
                           overwrite: bool = False, connection_username: Optional[str] = None,
                           connection_password: Optional[str] = None, save_credentials: bool = True,
                           oauth_flag: bool = False) -> str:
        project_luid = project_obj.luid
        xml = self._publish_content('datasource', ds_filename, ds_name, project_luid, {"overwrite": overwrite},
                                   connection_username, connection_password, save_credentials, oauth_flag=oauth_flag)
        datasource = xml.findall('.//t:datasource', TableauRestXml.ns_map)
        return datasource[0].get('id')

    #
    # Tags
    #

    # Tags can be scalar string or list
    def add_tags_to_datasource(self, ds_name_or_luid: str, tag_s: Union[List[str], str],
                               proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()

        ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)
        url = self.build_api_url("datasources/{}/tags".format(ds_luid))

        tsr = ET.Element("tsRequest")
        ts = ET.Element("tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = ET.Element("tag")
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
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

    def update_datasource(self, datasource_name_or_luid: str, datasource_project_name_or_luid: Optional[str] = None,
                          new_datasource_name: Optional[str] = None, new_project_name_or_luid: Optional[str] = None,
                          new_owner_luid: Optional[str] = None, certification_status: Optional[bool] = None,
                          certification_note: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        if certification_status not in [None, False, True]:
            raise InvalidOptionException('certification_status must be None, False, or True')

        datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = ET.Element("tsRequest")
        d = ET.Element("datasource")
        if new_datasource_name is not None:
            d.set('name', new_datasource_name)
        if certification_status is not None:
            d.set('isCertified', '{}'.format(str(certification_status).lower()))
        if certification_note is not None:
            d.set('certificationNote', certification_note)
        if new_project_name_or_luid is not None:
            new_project_luid = self.query_project_luid(new_project_name_or_luid)
            p = ET.Element('project')
            p.set('id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = ET.Element('owner')
            o.set('id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url("datasources/{}".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

class DatasourceMethods28(DatasourceMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

    def get_published_datasource_object(self, datasource_name_or_luid: str,
                                        project_name_or_luid: Optional[str] = None) -> Datasource28:
        luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        ds_obj = Datasource28(luid=luid, tableau_rest_api_obj=self,
                            default=False, logger_obj=self.logger)
        return ds_obj

class DatasourceMethods30(DatasourceMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class DatasourceMethods31(DatasourceMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

class DatasourceMethods32(DatasourceMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class DatasourceMethods33(DatasourceMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class DatasourceMethods34(DatasourceMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class DatasourceMethods35(DatasourceMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class DatasourceMethods36(DatasourceMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base