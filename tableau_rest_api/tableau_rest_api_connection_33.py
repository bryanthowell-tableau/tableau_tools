from tableau_rest_api_connection_32 import *

class TableauRestApiConnection32(TableauRestApiConnection31):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection31.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"2019.1")

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, async_publish=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False, generate_thumbnails_as_username_or_luid=None):
        """
        :type workbook_filename: unicode
        :type workbook_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type show_tabs: bool
        :param check_published_ds: Set to False to improve publish speed if you KNOW there are no published data sources
        :type check_published_ds: bool
        :type oauth_flag: bool:
        :type generate_thumbnails_as_username_or_luid: unicode
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content(u'workbook', workbook_filename, workbook_name, project_luid,
                                   {u"overwrite": overwrite, u"asJob": async_publish}, connection_username,
                                   connection_password, save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag,
                                   generate_thumbnails_as_username_or_luid=generate_thumbnails_as_username_or_luid)
        if async_publish is True:
            job = xml.findall(u'.//t:job', self.ns_map)
            return job[0].get(u'id')
        else:
            workbook = xml.findall(u'.//t:workbook', self.ns_map)
            return workbook[0].get(u'id')


    # Flow Methods Start


    def query_flow_luid(self, flow_name):
        pass

    def query_flows(self, sorts=None):
        self.start_log_block()
        flows = self.query_resource('flows', sorts=sorts)

        self.end_log_block()
        return flows

    # Just an alias for the method
    def query_flows_for_a_site(self):
        return self.query_flows()

    def query_flows_for_a_user(self, username_or_luid):
        pass

    def run_flow_now(self):
        pass
    # Flow Methods End