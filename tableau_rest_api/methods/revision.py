from .rest_api_base import *

class RevisionMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def get_workbook_revisions(self, workbook_name_or_luid: str,
                               project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        wb_luid = self.query_workbook_luid(workbook_name_or_luid, project_name_or_luid)
        wb_revisions = self.query_resource('workbooks/{}/revisions'.format(wb_luid))
        self.end_log_block()
        return wb_revisions

    def get_datasource_revisions(self, datasource_name_or_luid: str,
                                 project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        ds_luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        wb_revisions = self.query_resource('datasources/{}/revisions'.format(ds_luid))
        self.end_log_block()
        return wb_revisions

    def remove_datasource_revision(self, datasource_name_or_luid: str, revision_number: int,
                                   project_name_or_luid: Optional[str] = None):
        self.start_log_block()
        ds_luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        url = self.build_api_url("datasources/{}/revisions/{}".format(ds_luid, str(revision_number)))
        self.send_delete_request(url)
        self.end_log_block()

    def remove_workbook_revision(self, wb_name_or_luid: str, revision_number: int,
                                 project_name_or_luid: Optional[str] = None):

        self.start_log_block()
        wb_luid = self.query_workbook_luid(wb_name_or_luid, project_name_or_luid)
        url = self.build_api_url("workbooks/{}/revisions/{}".format(wb_luid, str(revision_number)))
        self.send_delete_request(url)
        self.end_log_block()

    # Do not include file extension. Without filename, only returns the response
    # Rewrite to have a Download and a Save, with one giving object in memory
    def download_datasource_revision(self, ds_name_or_luid: str, revision_number: int, filename_no_extension: str,
                                     proj_name_or_luid: Optional[str] = None,
                                     include_extract: bool = True) -> str:
        self.start_log_block()

        ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        try:

            if include_extract is False:
                url = self.build_api_url("datasources/{}/revisions/{}/content?includeExtract=False".format(ds_luid,
                                                                                                            str(revision_number)))
            else:
                url = self.build_api_url(
                    "datasources/{}/revisions/{}/content".format(ds_luid, str(revision_number)))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.tds'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tdsx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                              e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_ds' + extension
            else:
                save_filename = filename_no_extension + extension
            save_file = open(save_filename, 'wb')
            save_file.write(ds)
            save_file.close()
            self.end_log_block()
            return save_filename
        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.end_log_block()
            raise

    # Do not include file extension, added automatically. Without filename, only returns the response

    def download_workbook_revision(self, wb_name_or_luid: str, revision_number: int, filename_no_extension: str,
                                   proj_name_or_luid: Optional[str] = None,
                                   include_extract: bool = True) -> str:
        self.start_log_block()

        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.build_api_url("workbooks/{}/revisions/{}/content?includeExtract=False".format(wb_luid,
                                                                                                          str(revision_number)))
            else:
                url = self.build_api_url("workbooks/{}/revisions/{}/content".format(wb_luid, str(revision_number)))
            wb = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.twb'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.twbx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                            e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_wb' + extension
            else:
                save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(wb)
            save_file.close()
            self.end_log_block()
            return save_filename

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.end_log_block()
            raise

class RevisionMethods27(RevisionMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

class RevisionMethods28(RevisionMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

class RevisionMethods30(RevisionMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class RevisionMethods31(RevisionMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

class RevisionMethods32(RevisionMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class RevisionMethods33(RevisionMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class RevisionMethods34(RevisionMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class RevisionMethods35(RevisionMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class RevisionMethods36(RevisionMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base