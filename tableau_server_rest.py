from .tableau_rest_api import *
#
# This is equivalent to TableauRestApiConnection but with an extra layer of organization, by attaching
# most of the functions as sub-objects
#
class TableauServerRest(TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.2"):
        TableauRestApiBase.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.datasources: DatasourceMethods = DatasourceMethods(self.rest_api_base)
        self.extracts: ExtractMethods = ExtractMethods(self.rest_api_base)
        self.favorites: FavoritesMethods = FavoritesMethods(self.rest_api_base)
        self.groups: GroupMethods = GroupMethods(self.rest_api_base)
        self.projects: ProjectMethods = ProjectMethods(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
        self.subscriptions: SubscriptionMethods = SubscriptionMethods(self.rest_api_base)
        self.users: UserMethods = UserMethods(self.rest_api_base)
        self.workbooks: WorkbookMethods = WorkbookMethods(self.rest_api_base)


class TableauServerRest33(TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.3"):
        TableauRestApiBase33.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods33 = AlertMethods33(self.rest_api_base)
        self.datasources: DatasourceMethods33 = DatasourceMethods33(self.rest_api_base)
        self.extracts: ExtractMethods33 = ExtractMethods33(self.rest_api_base)
        self.favorites: FavoritesMethods33 = FavoritesMethods33(self.rest_api_base)
        self.flows: FlowMethods33 = FlowMethods33(self.rest_api_base)
        self.groups: GroupMethods33 = GroupMethods33(self.rest_api_base)
        self.projects: ProjectMethods33 = ProjectMethods33(self.rest_api_base)
        self.revisions: RevisionMethods33 = RevisionMethods33(self.rest_api_base)
        self.schedules: ScheduleMethods33 = ScheduleMethods33(self.rest_api_base)
        self.sites: SiteMethods33 = SiteMethods33(self.rest_api_base)
        self.subscriptions: SubscriptionMethods33 = SubscriptionMethods33(self.rest_api_base)
        self.users: UserMethods33 = UserMethods33(self.rest_api_base)
        self.workbooks: WorkbookMethods33 = WorkbookMethods33(self.rest_api_base)


class TableauServerRest34(TableauRestApiBase34):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.4"):
        TableauRestApiBase34.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods34 = AlertMethods34(self.rest_api_base)
        self.datasources: DatasourceMethods34 = DatasourceMethods34(self.rest_api_base)
        self.extracts: ExtractMethods34 = ExtractMethods34(self.rest_api_base)
        self.favorites: FavoritesMethods34 = FavoritesMethods34(self.rest_api_base)
        self.flows: FlowMethods34 = FlowMethods34(self.rest_api_base)
        self.groups: GroupMethods34 = GroupMethods34(self.rest_api_base)
        self.projects: ProjectMethods34 = ProjectMethods34(self.rest_api_base)
        self.revisions: RevisionMethods34 = RevisionMethods34(self.rest_api_base)
        self.schedules: ScheduleMethods34 = ScheduleMethods34(self.rest_api_base)
        self.sites: SiteMethods34 = SiteMethods34(self.rest_api_base)
        self.subscriptions: SubscriptionMethods34 = SubscriptionMethods34(self.rest_api_base)
        self.users: UserMethods34 = UserMethods34(self.rest_api_base)
        self.workbooks: WorkbookMethods34 = WorkbookMethods34(self.rest_api_base)


class TableauServerRest35(TableauRestApiBase35):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.5"):
        TableauRestApiBase35.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods35 = AlertMethods35(self.rest_api_base)
        self.datasources: DatasourceMethods35 = DatasourceMethods35(self.rest_api_base)
        self.extracts: ExtractMethods35 = ExtractMethods35(self.rest_api_base)
        self.favorites: FavoritesMethods35 = FavoritesMethods35(self.rest_api_base)
        self.flows: FlowMethods35 = FlowMethods35(self.rest_api_base)
        self.groups: GroupMethods35 = GroupMethods35(self.rest_api_base)
        self.metadata: MetadataMethods35 = MetadataMethods35(self.rest_api_base)
        self.projects: ProjectMethods35 = ProjectMethods35(self.rest_api_base)
        self.revisions: RevisionMethods35 = RevisionMethods35(self.rest_api_base)
        self.schedules: ScheduleMethods35 = ScheduleMethods35(self.rest_api_base)
        self.sites: SiteMethods35 = SiteMethods35(self.rest_api_base)
        self.subscriptions: SubscriptionMethods35 = SubscriptionMethods35(self.rest_api_base)
        self.users: UserMethods35 = UserMethods35(self.rest_api_base)
        self.workbooks: WorkbookMethods35 = WorkbookMethods35(self.rest_api_base)


class TableauServerRest36(TableauRestApiBase36):
    def __init__(self, server: str, username: Optional[str] = None, password: Optional[str] = None,
                 site_content_url: Optional[str] = "", pat_name: Optional[str] = None,
                 pat_secret: Optional[str] = None, api_version: str = "3.6"):
        TableauRestApiBase36.__init__(self, server=server, username=username, password=password,
                                      site_content_url=site_content_url, pat_name=pat_name, pat_secret=pat_secret)
        self.rest_api_base = self

        self.alerts: AlertMethods36 = AlertMethods36(self.rest_api_base)
        self.datasources: DatasourceMethods36 = DatasourceMethods36(self.rest_api_base)
        self.extracts: ExtractMethods36 = ExtractMethods36(self.rest_api_base)
        self.favorites: FavoritesMethods36 = FavoritesMethods36(self.rest_api_base)
        self.flows: FlowMethods36 = FlowMethods36(self.rest_api_base)
        self.groups: GroupMethods36 = GroupMethods36(self.rest_api_base)
        self.metadata: MetadataMethods36 = MetadataMethods36(self.rest_api_base)
        self.projects: ProjectMethods36 = ProjectMethods36(self.rest_api_base)
        self.revisions: RevisionMethods36 = RevisionMethods36(self.rest_api_base)
        self.schedules: ScheduleMethods36 = ScheduleMethods36(self.rest_api_base)
        self.sites: SiteMethods36 = SiteMethods36(self.rest_api_base)
        self.subscriptions: SubscriptionMethods36 = SubscriptionMethods36(self.rest_api_base)
        self.users: UserMethods36 = UserMethods36(self.rest_api_base)
        self.webhooks: WebhooksMethods36 = WebhooksMethods36(self.rest_api_base)
        self.workbooks: WorkbookMethods36 = WorkbookMethods36(self.rest_api_base)