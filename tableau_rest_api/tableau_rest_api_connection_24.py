from tableau_rest_api_connection_23 import *


class TableauRestApiConnection24(TableauRestApiConnection23):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection23.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.1")

    def query_server_info(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        server_info = self.query_resource(u"serverinfo", server_level=True)
        self.end_log_block()
        return server_info

    def query_server_version(self):
        """
        :rtype:
        """
        self.start_log_block()
        server_info = self.query_server_info()
        # grab the server number

    def query_api_version(self):
        self.start_log_block()
        server_info = self.query_server_info()
        # grab api version number

    def query_views(self, usage=False, created_at_filter=None, updated_at_filter=None, tags_filter=None, sorts=None):
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource(u"views", filters=filters, sorts=sorts,
                                  additional_url_ending=u"includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    def query_view(self, vw_name_or_luid):
        """
        :type vw_name_or_luid:
        :rtype: etree.Element
        """
        self.start_log_block()
        vw = self.query_single_element_from_endpoint_with_filter(u'view', vw_name_or_luid)
        self.end_log_block()
        return vw

    def query_datasources(self, project_name_or_luid=None, updated_at_filter=None, created_at_filter=None,
                          tags_filter=None, datasource_type_filter=None, sorts=None):
        """
        :type project_name_or_luid: unicode
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type datasource_type_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter,
                         u'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        ds = self.query_resource(u'datasources', filters=filters, sorts=sorts)
        self.end_log_block()
        return ds

    # query_datasource and query_datasource_luid can't be improved because filtering doesn't take a Project Name/LUID