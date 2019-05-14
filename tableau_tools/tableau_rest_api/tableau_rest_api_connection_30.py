from tableau_rest_api_connection_28 import *


class TableauRestApiConnection30(TableauRestApiConnection28):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection28.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"2018.1")

    @staticmethod
    def build_site_request_xml(site_name=None, content_url=None, admin_mode=None, tier_creator_capacity=None,
                               tier_explorer_capacity=None, tier_viewer_capacity=None, storage_quota=None,
                               disable_subscriptions=None, state=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type tier_creator_capacity: unicode
        :type tier_explorer_capacity: unicode
        :type tier_viewer_capacity: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :rtype: unicode
        """
        tsr = etree.Element(u"tsRequest")
        s = etree.Element(u'site')

        if site_name is not None:
            s.set(u'name', site_name)
        if content_url is not None:
            s.set(u'contentUrl', content_url)
        if admin_mode is not None:
            s.set(u'adminMode', admin_mode)
        if tier_creator_capacity is not None:
            s.set(u'tierCreatorCapacity', unicode(tier_creator_capacity))
        if tier_explorer_capacity is not None:
            s.set(u'tierExplorerCapacity', unicode(tier_explorer_capacity))
        if tier_viewer_capacity is not None:
            s.set(u'tierViewerCapacity', unicode(tier_viewer_capacity))
        if state is not None:
            s.set(u'state', state)
        if storage_quota is not None:
            s.set(u'storageQuota', unicode(storage_quota))
        if disable_subscriptions is not None:
            s.set(u'disableSubscriptions', unicode(disable_subscriptions).lower())

        tsr.append(s)
        return tsr

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name, new_content_url, admin_mode=None, tier_creator_capacity=None,
                    tier_explorer_capacity=None, tier_viewer_capacity=None, storage_quota=None,
                    disable_subscriptions=None):
        """
        :type new_site_name: unicode
        :type new_content_url: unicode
        :type admin_mode: unicode
        :type tier_creator_capacity: unicode
        :type tier_explorer_capacity: unicode
        :type tier_viewer_capacity: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :rtype: unicode
        """

        add_request = self.build_site_request_xml(new_site_name, new_content_url, admin_mode, tier_creator_capacity,
                                                  tier_explorer_capacity, tier_viewer_capacity, storage_quota,
                                                  disable_subscriptions)
        url = self.build_api_url(u"sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.send_add_request(url, add_request)
            return new_site.findall(u'.//t:site', self.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u"Site with content_url {} already exists".format(new_content_url))
                self.end_log_block()
                raise AlreadyExistsException(u"Site with content_url {} already exists".format(new_content_url),
                                             new_content_url)

    # Can only update the site you are signed into, so take site_luid from the object
    def update_site(self, site_name=None, content_url=None, admin_mode=None, tier_creator_capacity=None,
                    tier_explorer_capacity=None, tier_viewer_capacity=None, storage_quota=None,
                    disable_subscriptions=None, state=None, revision_history_enabled=None, revision_limit=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type tier_creator_capacity: unicode
        :type tier_explorer_capacity: unicode
        :type tier_viewer_capacity: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :type revision_history_enabled: bool
        :type revision_limit: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.build_site_request_xml(site_name, content_url, admin_mode, tier_creator_capacity,
                                          tier_explorer_capacity, tier_viewer_capacity, storage_quota,
                                          disable_subscriptions, state)
        url = self.build_api_url(u"")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, async_publish=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False):
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
        :type oauth_flag: bool
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content(u'workbook', workbook_filename, workbook_name, project_luid,
                                   {u"overwrite": overwrite, u"asJob": async_publish}, connection_username,
                                   connection_password, save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag)
        if async_publish is True:
            job = xml.findall(u'.//t:job', self.ns_map)
            return job[0].get(u'id')
        else:
            workbook = xml.findall(u'.//t:workbook', self.ns_map)
            return workbook[0].get(u'id')
