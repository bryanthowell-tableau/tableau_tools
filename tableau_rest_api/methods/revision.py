from .rest_api_base import *

class RevisionMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def get_workbook_revisions(self, workbook_name_or_luid: str,
                               project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(workbook_name_or_luid, project_name_or_luid)
        wb_revisions = self.rest.query_resource('workbooks/{}/revisions'.format(wb_luid))
        self.rest.end_log_block()
        return wb_revisions

    def get_datasource_revisions(self, datasource_name_or_luid: str,
                                 project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        ds_luid = self.rest.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        wb_revisions = self.rest.query_resource('datasources/{}/revisions'.format(ds_luid))
        self.rest.end_log_block()
        return wb_revisions

    def remove_datasource_revision(self, datasource_name_or_luid: str, revision_number: int,
                                   project_name_or_luid: Optional[str] = None):
        self.rest.start_log_block()
        ds_luid = self.rest.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        url = self.rest.build_api_url("datasources/{}/revisions/{}".format(ds_luid, str(revision_number)))
        self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def remove_workbook_revision(self, wb_name_or_luid: str, revision_number: int,
                                 project_name_or_luid: Optional[str] = None):

        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, project_name_or_luid)
        url = self.rest.build_api_url("workbooks/{}/revisions/{}".format(wb_luid, str(revision_number)))
        self.rest.send_delete_request(url)
        self.rest.end_log_block()

    # Do not include file extension. Without filename, only returns the response
    # Rewrite to have a Download and a Save, with one giving object in memory
    def download_datasource_revision(self, ds_name_or_luid: str, revision_number: int, filename_no_extension: str,
                                     proj_name_or_luid: Optional[str] = None,
                                     include_extract: bool = True) -> str:
        self.rest.start_log_block()

        ds_luid = self.rest.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        try:

            if include_extract is False:
                url = self.rest.build_api_url("datasources/{}/revisions/{}/content?includeExtract=False".format(ds_luid,
                                                                                                            str(revision_number)))
            else:
                url = self.rest.build_api_url(
                    "datasources/{}/revisions/{}/content".format(ds_luid, str(revision_number)))
            ds = self.rest.send_binary_get_request(url)
            extension = None
            if self.rest._last_response_content_type.find('application/xml') != -1:
                extension = '.tds'
            elif self.rest._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tdsx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.rest.log("download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                              e.tableau_error_code))
            self.rest.end_log_block()
            raise
        except:
            self.rest.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_ds' + extension
            else:
                save_filename = filename_no_extension + extension
            save_file = open(save_filename, 'wb')
            save_file.write(ds)
            save_file.close()
            self.rest.end_log_block()
            return save_filename
        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.rest.end_log_block()
            raise

    # Do not include file extension, added automatically. Without filename, only returns the response

    def download_workbook_revision(self, wb_name_or_luid: str, revision_number: int, filename_no_extension: str,
                                   proj_name_or_luid: Optional[str] = None,
                                   include_extract: bool = True) -> str:
        self.rest.start_log_block()

        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.rest.build_api_url("workbooks/{}/revisions/{}/content?includeExtract=False".format(wb_luid,
                                                                                                          str(revision_number)))
            else:
                url = self.rest.build_api_url("workbooks/{}/revisions/{}/content".format(wb_luid, str(revision_number)))
            wb = self.rest.send_binary_get_request(url)
            extension = None
            if self.rest._last_response_content_type.find('application/xml') != -1:
                extension = '.twb'
            elif self.rest._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.twbx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.rest.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                            e.tableau_error_code))
            self.rest.end_log_block()
            raise
        except:
            self.rest.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_wb' + extension
            else:
                save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(wb)
            save_file.close()
            self.rest.end_log_block()
            return save_filename

        except IOError:
            self.rest.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.rest.end_log_block()
            raise
