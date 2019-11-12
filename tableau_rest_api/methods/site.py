from .rest_api_base import *
class SiteMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)
    #
    # Start Site Querying Methods
    #

    # Site queries don't have the site portion of the URL, so login option gets correct format
    def query_sites(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        sites = self.query_resource("sites", server_level=True)
        self.end_log_block()
        return sites

    def query_sites_json(self, page_number=None):
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        sites = self.query_resource_json("sites", server_level=True, page_number=page_number)
        self.end_log_block()
        return sites

    # Methods for getting info about the sites, since you can only query a site when you are signed into it

    # Return list of all site contentUrls
    def query_all_site_content_urls(self):
        """
        :rtype: List[unicode]
        """
        self.start_log_block()
        sites = self.query_sites()
        site_content_urls = []
        for site in sites:
            site_content_urls.append(site.get("contentUrl"))
        self.end_log_block()
        return site_content_urls

    # You can only query a site you have logged into this way. Better to use methods that run through query_sites
    def query_current_site(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        site = self.query_resource("sites/{}".format(self.site_luid), server_level=True)
        self.end_log_block()
        return site

    #
    # End Site Querying Methods
    #
    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name, new_content_url, admin_mode=None, user_quota=None, storage_quota=None,
                    disable_subscriptions=None, direct_xml_request=None):
        """
        :type new_site_name: unicode
        :type new_content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        if direct_xml_request is not None:
            add_request = direct_xml_request
        else:
            add_request = self.build_site_request_xml(new_site_name, new_content_url, admin_mode, user_quota,
                                                      storage_quota, disable_subscriptions)
        url = self.build_api_url("sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.send_add_request(url, add_request)
            return new_site.findall('.//t:site', self.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log("Site with content_url {} already exists".format(new_content_url))
                self.end_log_block()
                raise AlreadyExistsException("Site with content_url {} already exists".format(new_content_url),
                                             new_content_url)

    # Can only update the site you are signed into, so take site_luid from the object
    def update_site(self, site_name=None, content_url=None, admin_mode=None, user_quota=None,
                    storage_quota=None, disable_subscriptions=None, state=None, revision_history_enabled=None,
                    revision_limit=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :type revision_history_enabled: bool
        :type revision_limit: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.build_site_request_xml(site_name, content_url, admin_mode, user_quota, storage_quota,
                                          disable_subscriptions, state)
        url = self.build_api_url("")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # Can only delete a site that you have signed into
    def delete_current_site(self):
        """
        :rtype:
        """
        self.start_log_block()
        url = self.build_api_url("sites/{}".format(self.site_luid), server_level=True)
        self.send_delete_request(url)
        self.end_log_block()

class SiteMethods27(SiteMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

class SiteMethods28(SiteMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

class SiteMethods30(SiteMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

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
        url = self.build_api_url("sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.send_add_request(url, add_request)
            return new_site.findall('.//t:site', self.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log("Site with content_url {} already exists".format(new_content_url))
                self.end_log_block()
                raise AlreadyExistsException("Site with content_url {} already exists".format(new_content_url),
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
        url = self.build_api_url("")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

class SiteMethods31(SiteMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

class SiteMethods32(SiteMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class SiteMethods33(SiteMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class SiteMethods34(SiteMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class SiteMethods35(SiteMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class SiteMethods36(SiteMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base