from .rest_api_base import *


class SiteMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    #
    # Internal REST API Helpers (mostly XML definitions that are reused between methods)
    #
    @staticmethod
    def build_site_request_xml(site_name: Optional[str] = None, content_url: Optional[str] = None,
                               admin_mode: Optional[str] = None, user_quota: Optional[int] = None,
                               storage_quota: Optional[str] = None, disable_subscriptions: Optional[bool] = None,
                               state: Optional[str] = None,
                               revision_history_enabled: Optional[bool] = None, revision_limit: Optional[str] = None):
        tsr = ET.Element("tsRequest")
        s = ET.Element('site')

        if site_name is not None:
            s.set('name', site_name)
        if content_url is not None:
            s.set('contentUrl', content_url)
        if admin_mode is not None:
            s.set('adminMode', admin_mode)
        if user_quota is not None:
            s.set('userQuota', str(user_quota))
        if state is not None:
            s.set('state', state)
        if storage_quota is not None:
            s.set('storageQuota', str(storage_quota))
        if disable_subscriptions is not None:
            s.set('disableSubscriptions', str(disable_subscriptions).lower())
        if revision_history_enabled is not None:
            s.set('revisionHistoryEnabled', str(revision_history_enabled).lower())
        if revision_limit is not None:
            s.set('revisionLimit', str(revision_limit))

        tsr.append(s)
        return tsr

    #
    # Start Site Querying Methods
    #

    # Site queries don't have the site portion of the URL, so login option gets correct format
    def query_sites(self) -> ET.Element:
        self.start_log_block()
        sites = self.query_resource("sites", server_level=True)
        self.end_log_block()
        return sites

    def query_sites_json(self, page_number: Optional[int] = None) -> Dict:
        self.start_log_block()
        sites = self.query_resource_json("sites", server_level=True, page_number=page_number)
        self.end_log_block()
        return sites

    # Methods for getting info about the sites, since you can only query a site when you are signed into it

    # Return list of all site contentUrls
    def query_all_site_content_urls(self) -> List[str]:
        self.start_log_block()
        sites = self.query_sites()
        site_content_urls = []
        for site in sites:
            site_content_urls.append(site.get("contentUrl"))
        self.end_log_block()
        return site_content_urls

    # You can only query a site you have logged into this way. Better to use methods that run through query_sites
    def query_current_site(self) -> ET.Element:
        self.start_log_block()
        site = self.query_resource("sites/{}".format(self.site_luid), server_level=True)
        self.end_log_block()
        return site

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name: str, new_content_url: str, admin_mode: Optional[str] = None,
                    user_quota: Optional[int] = None, storage_quota: Optional[str] = None,
                    disable_subscriptions: Optional[bool] = None, revision_history_enabled: Optional[bool] = None,
                    revision_limit: Optional[str] = None,
                    direct_xml_request: Optional[ET.Element] = None) -> str:

        if direct_xml_request is not None:
            add_request = direct_xml_request
        else:
            add_request = self.build_site_request_xml(site_name=new_site_name, content_url=new_content_url,
                                                      admin_mode=admin_mode,
                                                      user_quota=user_quota, storage_quota=storage_quota,
                                                      disable_subscriptions=disable_subscriptions,
                                                      revision_history_enabled=revision_history_enabled,
                                                      revision_limit=revision_limit)
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
    def update_site(self, site_name: Optional[str] = None, content_url: Optional[str] = None,
                    admin_mode: Optional[str] = None, user_quota: Optional[int] = None,
                    storage_quota: Optional[str] = None, disable_subscriptions: Optional[bool] = None,
                    state: Optional[str] = None, revision_history_enabled: Optional[bool] = None,
                    revision_limit: Optional[str] = None,
                    direct_xml_request: Optional[ET.Element] = None) -> ET.Element:
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = self.build_site_request_xml(site_name=site_name, content_url=content_url,
                                              admin_mode=admin_mode,
                                              user_quota=user_quota, storage_quota=storage_quota,
                                              disable_subscriptions=disable_subscriptions,
                                              revision_history_enabled=revision_history_enabled,
                                              revision_limit=revision_limit)
        url = self.build_api_url("")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # Can only delete a site that you have signed into
    def delete_current_site(self):
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

    @staticmethod
    def build_site_request_xml(site_name: Optional[str] = None, content_url: Optional[str] = None,
                               admin_mode: Optional[str] = None, user_quota: Optional[int] = None,
                               storage_quota: Optional[str] = None,
                               disable_subscriptions: Optional[bool] = None,
                               flows_enabled: Optional[bool] = None,
                               allow_subscription_attachments: Optional[bool] = None,
                               guest_access_enabled: Optional[bool] = None,
                               cache_warmup_enabled: Optional[bool] = None,
                               commenting_enabled: Optional[bool] = None,
                               revision_history_enabled: Optional[bool] = None,
                               revision_limit: Optional[str] = None,
                               subscribe_others_enabled: Optional[bool] = None,
                               extract_encryption_mode: Optional[str] = None,
                               request_access_enabled: Optional[bool] = None,
                               state: Optional[str] = None) -> ET.Element:
        tsr = ET.Element("tsRequest")
        s = ET.Element('site')

        if site_name is not None:
            s.set('name', site_name)
        if content_url is not None:
            s.set('contentUrl', content_url)
        if admin_mode is not None:
            s.set('adminMode', admin_mode)
        if user_quota is not None:
            s.set('userQuota', str(user_quota))
        if state is not None:
            s.set('state', state)
        if storage_quota is not None:
            s.set('storageQuota', str(storage_quota))
        if disable_subscriptions is not None:
            s.set('disableSubscriptions', str(disable_subscriptions).lower())
        if flows_enabled is not None:
            s.set('flowsEnabled', str(flows_enabled).lower())
        if allow_subscription_attachments is not None:
            s.set('allowSubscriptionAttachments', str(allow_subscription_attachments).lower())
        if guest_access_enabled is not None:
            s.set('guestAccessEnabled', str(guest_access_enabled).lower())
        if cache_warmup_enabled is not None:
            s.set('cacheWarmupEnabled', str(cache_warmup_enabled).lower())
        if commenting_enabled is not None:
            s.set('commentingEnabled', str(commenting_enabled).lower())
        if revision_history_enabled is not None:
            s.set('revisionHistoryEnabled', str(revision_history_enabled).lower())
        if revision_limit is not None:
            s.set('revisionLimit', str(revision_limit))
        if subscribe_others_enabled is not None:
            s.set('extractEncryptionMode', extract_encryption_mode)
        if extract_encryption_mode is not None:
            s.set('subscribeOthersEnabled', str(subscribe_others_enabled).lower())
        if request_access_enabled is not None:
            s.set('requestAccessEnabled', str(request_access_enabled).lower())

        tsr.append(s)
        return tsr

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name: str, new_content_url: str, admin_mode: Optional[str] = None,
                    user_quota: Optional[int] = None,
                    storage_quota: Optional[str] = None,
                    disable_subscriptions: Optional[bool] = None,
                    flows_enabled: Optional[bool] = None,
                    allow_subscription_attachments: Optional[bool] = None,
                    guest_access_enabled: Optional[bool] = None,
                    cache_warmup_enabled: Optional[bool] = None,
                    commenting_enabled: Optional[bool] = None,
                    revision_history_enabled: Optional[bool] = None,
                    revision_limit: Optional[str] = None,
                    subscribe_others_enabled: Optional[bool] = None,
                    extract_encryption_mode: Optional[str] = None,
                    request_access_enabled: Optional[bool] = None,
                    direct_xml_request: Optional[ET.Element] = None) -> str:
        self.start_log_block()
        if extract_encryption_mode is not None:
            if extract_encryption_mode not in ['enforced', 'enabled', 'disabled']:
                raise InvalidOptionException('extract_encryption_mode must be one of: enforced, enabled, disabled')

        if direct_xml_request is None:
            add_request = self.build_site_request_xml(site_name=new_site_name, content_url=new_content_url, admin_mode=admin_mode,
                                              user_quota=user_quota, storage_quota=storage_quota,
                                              disable_subscriptions=disable_subscriptions,
                                              flows_enabled=flows_enabled,
                                              allow_subscription_attachments=allow_subscription_attachments,
                                              guest_access_enabled=guest_access_enabled,
                                              cache_warmup_enabled=cache_warmup_enabled,
                                              commenting_enabled=commenting_enabled,
                                              revision_history_enabled=revision_history_enabled,
                                              revision_limit=revision_limit,
                                              subscribe_others_enabled=subscribe_others_enabled,
                                              extract_encryption_mode=extract_encryption_mode,
                                              request_access_enabled=request_access_enabled)
        else:
            add_request = direct_xml_request

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
    def update_site(self, site_name: Optional[str] = None, content_url: Optional[str] = None,
                    admin_mode: Optional[str] = None,
                    user_quota: Optional[int] = None,
                    storage_quota: Optional[str] = None,
                    disable_subscriptions: Optional[bool] = None,
                    flows_enabled: Optional[bool] = None,
                    allow_subscription_attachments: Optional[bool] = None,
                    guest_access_enabled: Optional[bool] = None,
                    cache_warmup_enabled: Optional[bool] = None,
                    commenting_enabled: Optional[bool] = None,
                    revision_history_enabled: Optional[bool] = None,
                    revision_limit: Optional[str] = None,
                    subscribe_others_enabled: Optional[bool] = None,
                    extract_encryption_mode: Optional[str] = None,
                    request_access_enabled: Optional[bool] = None,
                    state: Optional[str] = None,
                    direct_xml_request: Optional[ET.Element] = None) -> ET.Element:
        self.start_log_block()
        if extract_encryption_mode is not None:
            if extract_encryption_mode not in ['enforced', 'enabled', 'disabled']:
                raise InvalidOptionException('extract_encryption_mode must be one of: enforced, enabled, disabled')
        if direct_xml_request is None:
            tsr = self.build_site_request_xml(site_name=site_name, content_url=content_url, admin_mode=admin_mode,
                                              user_quota=user_quota, storage_quota=storage_quota,
                                              disable_subscriptions=disable_subscriptions, state=state,
                                              flows_enabled=flows_enabled,
                                              allow_subscription_attachments=allow_subscription_attachments,
                                              guest_access_enabled=guest_access_enabled,
                                              cache_warmup_enabled=cache_warmup_enabled,
                                              commenting_enabled=commenting_enabled,
                                              revision_history_enabled=revision_history_enabled,
                                              revision_limit=revision_limit,
                                              subscribe_others_enabled=subscribe_others_enabled,
                                              extract_encryption_mode=extract_encryption_mode,
                                              request_access_enabled=request_access_enabled)
        else:
            tsr = direct_xml_request

        url = self.build_api_url("")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

class SiteMethods36(SiteMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base