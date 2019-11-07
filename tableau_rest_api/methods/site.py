from .rest_api_base import *
class SiteMethods(TableauRestApiBase):
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