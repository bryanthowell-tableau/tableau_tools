from .methods import *

# This is a now a composite class that brings together all the different methods under one roof, but compatible with
# older scripts (easy upgrade)
class TableauRestApiConnection(WorkbookMethods, UserMethods, SubscriptionMethods, SiteMethods,
                               ScheduleMethods, RevisionMethods, PublishingMethods,
                               ProjectMethods, GroupMethods, FavoritesMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self
        #self.user_methods = UserMethods(self.rest_api_base)

class TableauRestApiConnection27(WorkbookMethods27, UserMethods27, SubscriptionMethods27, SiteMethods27,
                               ScheduleMethods27, RevisionMethods27, ProjectMethods27, GroupMethods27, FavoritesMethods27,
                               ExtractMethods27, DatasourceMethods27, TableauRestApiBase27):
    pass

class TableauRestApiConnection28(WorkbookMethods28, UserMethods28, SubscriptionMethods28, SiteMethods28,
                               ScheduleMethods28, RevisionMethods28, ProjectMethods28,
                               GroupMethods28, FavoritesMethods28,
                               ExtractMethods28, DatasourceMethods28, TableauRestApiBase28):
    pass

class TableauRestApiConnection30(WorkbookMethods30, UserMethods30, SubscriptionMethods30, SiteMethods30,
                               ScheduleMethods30, RevisionMethods30, ProjectMethods30,
                                 GroupMethods30, FavoritesMethods30,
                               ExtractMethods30, DatasourceMethods30, TableauRestApiBase30):
    pass

class TableauRestApiConnection31(WorkbookMethods31, UserMethods31, SubscriptionMethods31, SiteMethods31,
                               ScheduleMethods31, RevisionMethods31, ProjectMethods31,
                                 GroupMethods31, FavoritesMethods31,
                               ExtractMethods31, DatasourceMethods31, TableauRestApiBase31):
    pass

class TableauRestApiConnection32(WorkbookMethods32, UserMethods32, SubscriptionMethods32, SiteMethods32,
                                 ScheduleMethods32, RevisionMethods32,
                                 ProjectMethods32, GroupMethods32,
                                 FavoritesMethods32, ExtractMethods32, DatasourceMethods32, AlertMethods32,
                                 TableauRestApiBase32):
    pass

class TableauRestApiConnection33(WorkbookMethods33, UserMethods33, SubscriptionMethods33, SiteMethods33,
                                 ScheduleMethods33, RevisionMethods33, ProjectMethods33,
                                 GroupMethods33, FlowMethods33,
                                 FavoritesMethods33, ExtractMethods33, DatasourceMethods33, AlertMethods33,
                                 TableauRestApiBase33):
    pass

class TableauRestApiConnection34(WorkbookMethods34, UserMethods34, SubscriptionMethods34, SiteMethods34,
                                 ScheduleMethods34, RevisionMethods34, ProjectMethods34,
                                 GroupMethods34, FlowMethods34,
                                 FavoritesMethods34, ExtractMethods34, DatasourceMethods34, AlertMethods34,
                                 TableauRestApiBase34):
    pass

class TableauRestApiConnection35(WorkbookMethods35, UserMethods35, SubscriptionMethods35, SiteMethods35,
                                 ScheduleMethods35, RevisionMethods35, ProjectMethods35,
                                 GroupMethods35, FlowMethods35,
                                 FavoritesMethods35, ExtractMethods35, DatasourceMethods35, AlertMethods35,
                                 TableauRestApiBase35):
    pass

class TableauRestApiConnection36(WorkbookMethods36, UserMethods36, SubscriptionMethods36, SiteMethods36,
                                 ScheduleMethods36, RevisionMethods36, ProjectMethods36,
                                 GroupMethods36, FlowMethods36,
                                 FavoritesMethods36, ExtractMethods36, DatasourceMethods36, AlertMethods36,
                                 TableauRestApiBase36):
    pass
