from .rest_api_base import *
from ..permissions import DatabasePermissions35, TablePermissions35
import json


# First Metadata Methods appear in API 3.5
class MetadataMethods35():
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_databases(self) -> ET.Element:
        self.start_log_block()
        response = self.query_resource("databases")
        self.end_log_block()
        return response

    def query_database(self, database_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        # Implement the search mechanism eventually using XPath similar to old workbooks / datasources lookup
        response = self.query_single_element_from_endpoint("database", name_or_luid=database_name_or_luid)
        self.end_log_block()
        return response

    def update_database(self, database_name_or_luid: str, certification_status: Optional[bool] = None,
                        certification_note: Optional[str] = None, description: Optional[str] = None,
                        contact_username_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        d = ET.Element('database')
        if certification_status is not None:
            d.set('isCertified', str(certification_status).lower())
        if certification_note is not None:
            d.set('certificationNote', certification_note)
        if description is not None:
            d.set('description', description)

        if contact_username_or_luid is not None:
            user_luid = self.rest_api_base.query_user_luid(username=contact_username_or_luid)
            c = ET.Element('contact')
            c.set('id', user_luid)
            d.append(c)
        tsr.append(d)
        database_luid = self.query_database_luid(database_name=database_name_or_luid)
        url = self.build_api_url("databases/{}".format(database_luid))
        response = self.send_update_request(url=url, request=tsr)
        self.end_log_block()
        return response

    def remove_database(self, database_name_or_luid: str):
        self.start_log_block()
        database_luid = self.query_database_luid(database_name=database_name_or_luid)
        url = self.build_api_url("databases/{}".format(database_luid))
        self.rest_api_base.send_delete_request(url)
        self.end_log_block()

    def query_tables(self) -> ET.Element:
        self.start_log_block()
        response = self.query_resource("tables")
        self.end_log_block()
        return response

    def query_table(self, table_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        # Implement the search mechanism eventually using XPath similar to old workbooks / datasources lookup
        response = self.query_single_element_from_endpoint("table", name_or_luid=table_name_or_luid)
        self.end_log_block()
        return response

    def update_table(self, table_name_or_luid: str, certification_status: Optional[bool] = None,
                        certification_note: Optional[str] = None, description: Optional[str] = None,
                        contact_username_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        t = ET.Element('table')
        if certification_status is not None:
            t.set('isCertified', str(certification_status).lower())
        if certification_note is not None:
            t.set('certificationNote', certification_note)
        if description is not None:
            t.set('description', description)

        if contact_username_or_luid is not None:
            user_luid = self.rest_api_base.query_user_luid(username=contact_username_or_luid)
            c = ET.Element('contact')
            c.set('id', user_luid)
            t.append(c)
        tsr.append(t)
        table_luid = self.query_database_luid(database_name=table_name_or_luid)
        url = self.build_api_url("tables/{}".format(table_luid))
        response = self.send_update_request(url=url, request=tsr)
        self.end_log_block()
        return response

    def remove_table(self, table_name_or_luid: str):
        self.start_log_block()
        table_luid = self.query_table_luid(table_name=table_name_or_luid)
        url = self.build_api_url("tables/{}".format(table_luid))
        self.rest_api_base.send_delete_request(url)
        self.end_log_block()

    def query_columns_in_a_table(self, table_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        table_luid = self.query_table_luid(table_name_or_luid)
        response = self.query_resource("tables/{}/columns".format(table_luid))
        self.end_log_block()
        return response

    def update_column(self, table_name_or_luid: str, column_name_or_luid: str,
                      description: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        c = ET.Element('column')
        if description is not None:
            c.set('description', description)
        tsr.append(c)
        table_luid = self.query_table_luid(table_name=table_name_or_luid)
        column_luid = self.query_column_luid(database_name=column_name_or_luid)
        url = self.build_api_url("tables/{}/columns/{}".format(table_luid, column_luid))
        response = self.send_update_request(url=url, request=tsr)
        self.end_log_block()
        return response

    def remove_column(self, table_name_or_luid: str, column_name_or_luid: str):
        self.start_log_block()
        table_luid = self.query_table_luid(table_name=table_name_or_luid)
        column_luid = self.query_column_luid(database_name=column_name_or_luid)
        url = self.build_api_url("tables/{}/columns/{}".format(table_luid, column_luid))
        self.send_delete_request(url)
        self.end_log_block()

    # There does not appear to be a plural Data Quality Warnings endpoint

    def add_data_quality_warning(self, content_type: str, content_luid: str) -> ET.Element:
        self.start_log_block()
        content_type = content_type.lower()
        if content_type not in ['database', 'table', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be one of: 'database', 'table', 'datasource', 'flow'")
        url = self.build_api_url("dataQualityWarnings/{}/{}".format(content_type, content_luid))
        response = self.send_post_request(url)
        self.end_log_block()
        return response

    def query_data_quality_warning_by_id(self, data_quality_warning_luid: str) -> ET.Element:
        self.start_log_block()
        url = self.build_api_url("dataQualityWarnings/{}".format(data_quality_warning_luid))
        response = self.query_resource(url)
        self.end_log_block()
        return response

    def query_data_quality_warning_by_asset(self, content_type: str, content_luid: str) -> ET.Element:
        self.start_log_block()
        content_type = content_type.lower()
        if content_type not in ['database', 'table', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be one of: 'database', 'table', 'datasource', 'flow'")
        url = self.build_api_url("dataQualityWarnings/{}/{}".format(content_type, content_luid))
        response = self.query_resource(url)
        self.end_log_block()
        return response

    def update_data_quality_warning(self, data_quality_warning_luid: str, type: Optional[str] = None,
                                    is_active: Optional[bool] = None, message: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        d = ET.Element('dataQualityWarning')
        if type is not None:
            if type not in ['Deprecated', 'Warning', 'Stale data', 'Under maintenance']:
                raise InvalidOptionException("The following are the allowed types: 'Deprecated', 'Warning', 'Stale data', 'Under maintenance'")
            d.set('type', type)
        if is_active is not None:
            d.set('isActive', str(is_active).lower())
        if message is not None:
            d.set('message', message)
        tsr.append(d)

        url = self.build_api_url("dataQualityWarnings/{}".format(data_quality_warning_luid))
        response = self.send_update_request(url=url, request=tsr)
        self.end_log_block()
        return response

    def delete_data_quality_warning(self, data_quality_warning_luid: str):
        self.start_log_block()
        url = self.build_api_url("dataQualityWarnings/{}".format(data_quality_warning_luid))
        self.send_delete_request(url)
        self.end_log_block()

    # This is called "Delete Data Quality Warning by Content" in the reference, but Query is called "by Asset"
    # So this method is implemented with a parallel naming (By Asset is better IMHO)
    def delete_data_quality_warning_by_asset(self, content_type: str, content_luid: str):
        self.start_log_block()
        content_type = content_type.lower()
        if content_type not in ['database', 'table', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be one of: 'database', 'table', 'datasource', 'flow'")
        url = self.build_api_url("dataQualityWarnings/{}/{}".format(content_type, content_luid))
        self.send_delete_request(url)
        self.end_log_block()

    # This is the naming in the reference, but Query is called "by asset" which is better IMHO
    def delete_data_quality_warning_by_content(self, content_type: str, content_luid: str):
        self.delete_data_quality_warning_by_asset(content_type=content_type, content_luid=content_luid)

    # Described here https://help.tableau.com/current/api/metadata_api/en-us/index.html
    # Uses json.loads() to build the JSON object to send
    # Need to implement POST on the RestJsonRequest object to be able to do this
    def graphql(self, graphql_query: str) -> Dict:
        self.start_log_block()
        graphql_json = {"query": graphql_query}
        url = self.rest_api_base.build_api_url("metadata")
        try:
            response = self.rest_api_base.send_add_request_json(url, graphql_json)
            self.end_log_block()
            return response
        except RecoverableHTTPException as e:
            if e.tableau_error_code == '404003':
                self.end_log_block()
                raise InvalidOptionException("The metadata API is not turned on for this server at this time")



    # Database and Table Permissions are implemented in the Permissions and PublishedContent classes

class MetadataMethods36(MetadataMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base