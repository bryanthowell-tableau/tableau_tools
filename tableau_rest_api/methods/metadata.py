from .rest_api_base import *
from ..permissions import DatabasePermissions35, TablePermissions35

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
        response = self.query_single_element_from_endpoint("databases", name_or_luid=database_name_or_luid)
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
        response = self.rest_api_base.send_update_request(url=url, request=tsr)
        self.end_log_block()
        return response

    # UNFINISHED
    def query_database_permissions(self, database_name_or_luid: str) -> DatabasePermissions35:
        pass



    # Described here https://help.tableau.com/current/api/metadata_api/en-us/index.html
    # Uses json.loads() to build the JSON object to send
    # Need to implement POST on the RestJsonRequest object to be able to do this
    def graphql(self, graphql_query: str) -> str:
        self.start_log_block()

        self.end_log_block()