from .rest_api_base import *


class SiteMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    #
    # Internal REST API Helpers (mostly XML definitions that are reused between methods)
    #
    @staticmethod
    def build_site_request_xml(site_name: Optional[str] = None, content_url: Optional[str] = None,
                               options_dict: Optional[Dict] = None):
        tsr = ET.Element("tsRequest")
        s = ET.Element('site')

        if site_name is not None:
            s.set('name', site_name)
        if content_url is not None:
            s.set('contentUrl', content_url)
        if options_dict is not None:
            for key in options_dict:
                if str(options_dict[key]).lower() in ['true', 'false']:
                    s.set(key, str(options_dict[key]).lower())
                else:
                    s.set(key, str(options_dict[key]))

        tsr.append(s)
        return tsr

    #
    # Start Site Querying Methods
    #

    # Site queries don't have the site portion of the URL, so login option gets correct format
    def query_sites(self) -> ET.Element:
        self.rest.start_log_block()
        sites = self.rest.query_resource("sites", server_level=True)
        self.rest.end_log_block()
        return sites

    def query_sites_json(self, page_number: Optional[int] = None) -> Dict:
        self.rest.start_log_block()
        sites = self.rest.query_resource_json("sites", server_level=True, page_number=page_number)
        self.rest.end_log_block()
        return sites

    # Methods for getting info about the sites, since you can only query a site when you are signed into it

    # Return list of all site contentUrls
    def query_all_site_content_urls(self) -> List[str]:
        self.rest.start_log_block()
        sites = self.query_sites()
        site_content_urls = []
        for site in sites:
            site_content_urls.append(site.get("contentUrl"))
        self.rest.end_log_block()
        return site_content_urls

    # You can only query a site you have logged into this way. Better to use methods that run through query_sites
    def query_current_site(self) -> ET.Element:
        self.rest.start_log_block()
        site = self.rest.query_resource("sites/{}".format(self.rest.site_luid), server_level=True)
        self.rest.end_log_block()
        return site

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name: str, new_content_url: str, options_dict: Optional[Dict] = None,
                    direct_xml_request: Optional[ET.Element] = None) -> str:

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = self.build_site_request_xml(site_name=new_site_name, content_url=new_content_url,
                                                      options_dict=options_dict)
        url = self.rest.build_api_url("sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.rest.send_add_request(url, tsr)
            return new_site.findall('.//t:site', self.rest.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.rest.log("Site with content_url {} already exists".format(new_content_url))
                self.rest.end_log_block()
                raise AlreadyExistsException("Site with content_url {} already exists".format(new_content_url),
                                             new_content_url)

    # Can only update the site you are signed into, so take site_luid from the object
    def update_site(self, site_name: Optional[str] = None, content_url: Optional[str] = None,
                    options_dict: Optional[Dict] = None, direct_xml_request: Optional[ET.Element] = None) -> ET.Element:
        self.rest.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = self.build_site_request_xml(site_name=site_name, content_url=content_url, options_dict=options_dict)
        url = self.rest.build_api_url("")
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response

    # Can only delete a site that you have signed into
    def delete_current_site(self):
        self.rest.start_log_block()
        url = self.rest.build_api_url("sites/{}".format(self.rest.site_luid), server_level=True)
        self.rest.send_delete_request(url)
        self.rest.end_log_block()

