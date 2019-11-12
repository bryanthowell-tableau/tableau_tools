from .methods import *

# This is a now a composite class that brings together all the different methods under one roof, but compatible with
# older scripts (easy upgrade)
class TableauRestApiConnection(WorkbookMethods, UserMethods, SiteMethods,
                               ScheduleMethods, RevisionMethods, PublishingMethods,
                               ProjectMethods, GroupMethods, FavoritesMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self
        #self.user_methods = UserMethods(self.rest_api_base)

class TableauRestApiConnection27(WorkbookMethods27, UserMethods27, SiteMethods27,
                               ScheduleMethods27, RevisionMethods27, PublishingMethods27, ProjectMethods27, GroupMethods27, FavoritesMethods27,
                               ExtractMethods27, DatasourceMethods27, TableauRestApiBase27):
    pass

class TableauRestApiConnection28(WorkbookMethods28, UserMethods28, SiteMethods28,
                               ScheduleMethods28, RevisionMethods28, PublishingMethods28, ProjectMethods28,
                               GroupMethods28, FavoritesMethods28,
                               ExtractMethods28, DatasourceMethods28, TableauRestApiBase28):
    pass

class TableauRestApiConnection30(WorkbookMethods30, UserMethods30, SiteMethods30,
                               ScheduleMethods30, RevisionMethods30, PublishingMethods30, ProjectMethods30,
                                 GroupMethods30, FavoritesMethods30,
                               ExtractMethods30, DatasourceMethods30, TableauRestApiBase30):
    pass

class TableauRestApiConnection31(WorkbookMethods31, UserMethods31, SiteMethods31,
                               ScheduleMethods31, RevisionMethods31, PublishingMethods31, ProjectMethods31,
                                 GroupMethods31, FavoritesMethods31,
                               ExtractMethods31, DatasourceMethods31, TableauRestApiBase31):
    pass

class TableauRestApiConnection32(WorkbookMethods32, UserMethods32, SiteMethods32,
                                 ScheduleMethods32, RevisionMethods32, PublishingMethods32,
                                 ProjectMethods32, GroupMethods32,
                                 FavoritesMethods32, ExtractMethods32, DatasourceMethods32, AlertMethods32,
                                 TableauRestApiBase32):
    pass

class TableauRestApiConnection33(WorkbookMethods33, UserMethods33, SiteMethods33,
                                 ScheduleMethods33, RevisionMethods33, PublishingMethods33, ProjectMethods33,
                                 GroupMethods33, FlowMethods33,
                                 FavoritesMethods33, ExtractMethods33, DatasourceMethods33, AlertMethods33,
                                 TableauRestApiBase33):
    pass

class TableauRestApiConnection34(WorkbookMethods34, UserMethods34, SiteMethods34,
                                 ScheduleMethods34, RevisionMethods34, PublishingMethods34, ProjectMethods34,
                                 GroupMethods34, FlowMethods34,
                                 FavoritesMethods34, ExtractMethods34, DatasourceMethods34, AlertMethods34,
                                 TableauRestApiBase34):
    pass

class TableauRestApiConnection35(WorkbookMethods35, UserMethods35, SiteMethods35,
                                 ScheduleMethods35, RevisionMethods35, PublishingMethods35, ProjectMethods35,
                                 GroupMethods35, FlowMethods35,
                                 FavoritesMethods35, ExtractMethods35, DatasourceMethods35, AlertMethods35,
                                 TableauRestApiBase35):
    pass

class TableauRestApiConnection36(WorkbookMethods36, UserMethods36, SiteMethods36,
                                 ScheduleMethods36, RevisionMethods36, PublishingMethods36, ProjectMethods36,
                                 GroupMethods36, FlowMethods36,
                                 FavoritesMethods36, ExtractMethods36, DatasourceMethods36, AlertMethods36,
                                 TableauRestApiBase36):
    pass


#
# Reorg'd new classes
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
        self.publishing: PublishingMethods = PublishingMethods(self.rest_api_base)
        self.revisions: RevisionMethods = RevisionMethods(self.rest_api_base)
        self.schedules: ScheduleMethods = ScheduleMethods(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods(self.rest_api_base)
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
        self.publishing: PublishingMethods27 = PublishingMethods27(self.rest_api_base)
        self.revisions: RevisionMethods27 = RevisionMethods27(self.rest_api_base)
        self.schedules: ScheduleMethods27 = ScheduleMethods27(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods27(self.rest_api_base)
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
        self.publishing: PublishingMethods28 = PublishingMethods28(self.rest_api_base)
        self.revisions: RevisionMethods28 = RevisionMethods28(self.rest_api_base)
        self.schedules: ScheduleMethods28 = ScheduleMethods28(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods28(self.rest_api_base)
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
        self.publishing: PublishingMethods30 = PublishingMethods30(self.rest_api_base)
        self.revisions: RevisionMethods30 = RevisionMethods30(self.rest_api_base)
        self.schedules: ScheduleMethods30 = ScheduleMethods30(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods30(self.rest_api_base)
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
        self.publishing: PublishingMethods31 = PublishingMethods31(self.rest_api_base)
        self.revisions: RevisionMethods31 = RevisionMethods31(self.rest_api_base)
        self.schedules: ScheduleMethods31 = ScheduleMethods31(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods31(self.rest_api_base)
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
        self.publishing: PublishingMethods32 = PublishingMethods32(self.rest_api_base)
        self.revisions: RevisionMethods32 = RevisionMethods32(self.rest_api_base)
        self.schedules: ScheduleMethods32 = ScheduleMethods32(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods32(self.rest_api_base)
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
        self.publishing: PublishingMethods33 = PublishingMethods33(self.rest_api_base)
        self.revisions: RevisionMethods33 = RevisionMethods33(self.rest_api_base)
        self.schedules: ScheduleMethods33 = ScheduleMethods33(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods33(self.rest_api_base)
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
        self.publishing: PublishingMethods34 = PublishingMethods34(self.rest_api_base)
        self.revisions: RevisionMethods34 = RevisionMethods34(self.rest_api_base)
        self.schedules: ScheduleMethods34 = ScheduleMethods34(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods34(self.rest_api_base)
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
        self.projects: ProjectMethods35 = ProjectMethods35(self.rest_api_base)
        self.publishing: PublishingMethods35 = PublishingMethods35(self.rest_api_base)
        self.revisions: RevisionMethods35 = RevisionMethods35(self.rest_api_base)
        self.schedules: ScheduleMethods35 = ScheduleMethods35(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods35(self.rest_api_base)
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
        self.projects: ProjectMethods36 = ProjectMethods36(self.rest_api_base)
        self.publishing: PublishingMethods36 = PublishingMethods36(self.rest_api_base)
        self.revisions: RevisionMethods36 = RevisionMethods36(self.rest_api_base)
        self.schedules: ScheduleMethods36 = ScheduleMethods36(self.rest_api_base)
        self.sites: SiteMethods = SiteMethods36(self.rest_api_base)
        self.users: UserMethods36 = UserMethods36(self.rest_api_base)
        self.workbooks: WorkbookMethods36 = WorkbookMethods36(self.rest_api_base)
