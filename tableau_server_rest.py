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

        self.alerts: AlertMethods = AlertMethods(self.rest_api_base)
        self.datasources: DatasourceMethods = DatasourceMethods(self.rest_api_base)
        self.extracts: ExtractMethods = ExtractMethods(self.rest_api_base)
        self.favorites: FavoritesMethods = FavoritesMethods(self.rest_api_base)
        self.flows: FlowMethods33 = FlowMethods33(self.rest_api_base)
        self.groups: GroupMethods = GroupMethods(self.rest_api_base)
        self.projects: ProjectMethods33 = ProjectMethods33(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
        self.subscriptions: SubscriptionMethods = SubscriptionMethods(self.rest_api_base)
        self.users: UserMethods = UserMethods(self.rest_api_base)
        self.workbooks: WorkbookMethods = WorkbookMethods(self.rest_api_base)


class TableauServerRest34(TableauRestApiBase34):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.4"):
        TableauRestApiBase34.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods = AlertMethods(self.rest_api_base)
        self.datasources: DatasourceMethods = DatasourceMethods(self.rest_api_base)
        self.extracts: ExtractMethods = ExtractMethods(self.rest_api_base)
        self.favorites: FavoritesMethods = FavoritesMethods(self.rest_api_base)
        self.flows: FlowMethods33 = FlowMethods33(self.rest_api_base)
        self.groups: GroupMethods = GroupMethods(self.rest_api_base)
        self.projects: ProjectMethods = ProjectMethods(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
        self.subscriptions: SubscriptionMethods = SubscriptionMethods(self.rest_api_base)
        self.users: UserMethods = UserMethods(self.rest_api_base)
        self.workbooks: WorkbookMethods34 = WorkbookMethods34(self.rest_api_base)


class TableauServerRest35(TableauRestApiBase35):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = "", api_version: str = "3.5"):
        TableauRestApiBase35.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods = AlertMethods(self.rest_api_base)
        self.datasources: DatasourceMethods = DatasourceMethods(self.rest_api_base)
        self.extracts: ExtractMethods35 = ExtractMethods35(self.rest_api_base)
        self.favorites: FavoritesMethods = FavoritesMethods(self.rest_api_base)
        self.flows: FlowMethods33 = FlowMethods33(self.rest_api_base)
        self.groups: GroupMethods = GroupMethods(self.rest_api_base)
        self.metadata: MetadataMethods35 = MetadataMethods35(self.rest_api_base)
        self.projects: ProjectMethods = ProjectMethods(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
        self.subscriptions: SubscriptionMethods35 = SubscriptionMethods35(self.rest_api_base)
        self.users: UserMethods = UserMethods(self.rest_api_base)
        self.workbooks: WorkbookMethods34 = WorkbookMethods34(self.rest_api_base)


class TableauServerRest36(TableauRestApiBase36):
    def __init__(self, server: str, username: Optional[str] = None, password: Optional[str] = None,
                 site_content_url: Optional[str] = "", pat_name: Optional[str] = None,
                 pat_secret: Optional[str] = None, api_version: str = "3.6"):
        TableauRestApiBase36.__init__(self, server=server, username=username, password=password,
                                      site_content_url=site_content_url, pat_name=pat_name, pat_secret=pat_secret)
        self.rest_api_base = self

        self.alerts: AlertMethods = AlertMethods(self.rest_api_base)
        self.datasources: DatasourceMethods = DatasourceMethods(self.rest_api_base)
        self.extracts: ExtractMethods35 = ExtractMethods35(self.rest_api_base)
        self.favorites: FavoritesMethods = FavoritesMethods(self.rest_api_base)
        self.flows: FlowMethods33 = FlowMethods33(self.rest_api_base)
        self.groups: GroupMethods = GroupMethods(self.rest_api_base)
        self.metadata: MetadataMethods35 = MetadataMethods35(self.rest_api_base)
        self.projects: ProjectMethods = ProjectMethods(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
        self.subscriptions: SubscriptionMethods35 = SubscriptionMethods35(self.rest_api_base)
        self.users: UserMethods = UserMethods(self.rest_api_base)
        self.webhooks: WebhooksMethods36 = WebhooksMethods36(self.rest_api_base)
        self.workbooks: WorkbookMethods34 = WorkbookMethods34(self.rest_api_base)