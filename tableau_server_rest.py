from .tableau_rest_api import *
#
# This is equivalent to TableauRestApiConnection but with an extra layer of organization, by attaching
# most of the functions as sub-objects
#
class TableauServerRest(TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
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


class TableauServerRest27(TableauRestApiBase27):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
        TableauRestApiBase27.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.datasources: DatasourceMethods27 = DatasourceMethods27(self.rest_api_base)
        self.extracts: ExtractMethods27 = ExtractMethods27(self.rest_api_base)
        self.favorites: FavoritesMethods27 = FavoritesMethods27(self.rest_api_base)
        self.groups: GroupMethods27 = GroupMethods27(self.rest_api_base)
        self.projects: ProjectMethods27 = ProjectMethods27(self.rest_api_base)
        self.revisions: RevisionMethods27 = RevisionMethods27(self.rest_api_base)
        self.schedules: ScheduleMethods27 = ScheduleMethods27(self.rest_api_base)
        self.sites: SiteMethods27 = SiteMethods27(self.rest_api_base)
        self.subscriptions: SubscriptionMethods27 = SubscriptionMethods27(self.rest_api_base)
        self.users: UserMethods27 = UserMethods27(self.rest_api_base)
        self.workbooks: WorkbookMethods27 = WorkbookMethods27(self.rest_api_base)


class TableauServerRest28(TableauRestApiBase28):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
        TableauRestApiBase28.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.datasources: DatasourceMethods28 = DatasourceMethods28(self.rest_api_base)
        self.extracts: ExtractMethods28 = ExtractMethods28(self.rest_api_base)
        self.favorites: FavoritesMethods28 = FavoritesMethods28(self.rest_api_base)
        self.groups: GroupMethods28 = GroupMethods28(self.rest_api_base)
        self.projects: ProjectMethods28 = ProjectMethods28(self.rest_api_base)
        self.revisions: RevisionMethods28 = RevisionMethods28(self.rest_api_base)
        self.schedules: ScheduleMethods28 = ScheduleMethods28(self.rest_api_base)
        self.sites: SiteMethods28 = SiteMethods28(self.rest_api_base)
        self.subscriptions: SubscriptionMethods28 = SubscriptionMethods28(self.rest_api_base)
        self.users: UserMethods28 = UserMethods28(self.rest_api_base)
        self.workbooks: WorkbookMethods28 = WorkbookMethods28(self.rest_api_base)


class TableauServerRest30(TableauRestApiBase30):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
        TableauRestApiBase30.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.datasources: DatasourceMethods30 = DatasourceMethods30(self.rest_api_base)
        self.extracts: ExtractMethods30 = ExtractMethods30(self.rest_api_base)
        self.favorites: FavoritesMethods30 = FavoritesMethods30(self.rest_api_base)
        self.groups: GroupMethods30 = GroupMethods30(self.rest_api_base)
        self.projects: ProjectMethods30 = ProjectMethods30(self.rest_api_base)
        self.revisions: RevisionMethods30 = RevisionMethods30(self.rest_api_base)
        self.schedules: ScheduleMethods30 = ScheduleMethods30(self.rest_api_base)
        self.sites: SiteMethods30 = SiteMethods30(self.rest_api_base)
        self.subscriptions: SubscriptionMethods30 = SubscriptionMethods30(self.rest_api_base)
        self.users: UserMethods30 = UserMethods30(self.rest_api_base)
        self.workbooks: WorkbookMethods30 = WorkbookMethods30(self.rest_api_base)


class TableauServerRest31(TableauRestApiBase31):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
        TableauRestApiBase31.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.datasources: DatasourceMethods31 = DatasourceMethods31(self.rest_api_base)
        self.extracts: ExtractMethods31 = ExtractMethods31(self.rest_api_base)
        self.favorites: FavoritesMethods31 = FavoritesMethods31(self.rest_api_base)
        self.groups: GroupMethods31 = GroupMethods31(self.rest_api_base)
        self.projects: ProjectMethods31 = ProjectMethods31(self.rest_api_base)
        self.revisions: RevisionMethods31 = RevisionMethods31(self.rest_api_base)
        self.schedules: ScheduleMethods31 = ScheduleMethods31(self.rest_api_base)
        self.sites: SiteMethods31 = SiteMethods31(self.rest_api_base)
        self.subscriptions: SubscriptionMethods31 = SubscriptionMethods31(self.rest_api_base)
        self.users: UserMethods31 = UserMethods31(self.rest_api_base)
        self.workbooks: WorkbookMethods31 = WorkbookMethods31(self.rest_api_base)


class TableauServerRest32(TableauRestApiBase32):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
        TableauRestApiBase32.__init__(self, server, username, password, site_content_url)
        self.rest_api_base = self

        self.alerts: AlertMethods32 = AlertMethods32(self.rest_api_base)
        self.datasources: DatasourceMethods32 = DatasourceMethods32(self.rest_api_base)
        self.extracts: ExtractMethods32 = ExtractMethods32(self.rest_api_base)
        self.favorites: FavoritesMethods32 = FavoritesMethods32(self.rest_api_base)
        self.groups: GroupMethods32 = GroupMethods32(self.rest_api_base)
        self.projects: ProjectMethods32 = ProjectMethods32(self.rest_api_base)
        self.revisions: RevisionMethods32 = RevisionMethods32(self.rest_api_base)
        self.schedules: ScheduleMethods32 = ScheduleMethods32(self.rest_api_base)
        self.sites: SiteMethods32 = SiteMethods32(self.rest_api_base)
        self.subscriptions: SubscriptionMethods32 = SubscriptionMethods32(self.rest_api_base)
        self.users: UserMethods32 = UserMethods32(self.rest_api_base)
        self.workbooks: WorkbookMethods32 = WorkbookMethods32(self.rest_api_base)


class TableauServerRest33(TableauRestApiBase33):
    def __init__(self, server: str, username: str, password: str,
                 site_content_url: Optional[str] = ""):
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
                 site_content_url: Optional[str] = ""):
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
                 site_content_url: Optional[str] = ""):
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
                 pat_secret: Optional[str] = None):
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