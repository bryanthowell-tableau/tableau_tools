from .tableau_rest_api.methods.workbook import *
from .tableau_rest_api.methods.user import *
from .tableau_rest_api.methods.subscription import *
from .tableau_rest_api.methods.site import *
from .tableau_rest_api.methods.schedule import *
from .tableau_rest_api.methods.revision import *
from .tableau_rest_api.methods.project import *
from .tableau_rest_api.methods.group import *
from .tableau_rest_api.methods.flow import *
from .tableau_rest_api.methods.favorites import *
from .tableau_rest_api.methods.extract import *
from .tableau_rest_api.methods.datasource import *
from .tableau_rest_api.methods.alert import *
from .tableau_rest_api.methods.metadata import *
from .tableau_rest_api.methods.webhooks import *
from .tableau_rest_api.methods.rest_api_base import *

# This is a now a composite class that brings together all the different methods under one roof, but compatible with
# older scripts (easy upgrade)
class TableauRestApiConnection(WorkbookMethods, UserMethods, SubscriptionMethods, SiteMethods,
                               ScheduleMethods, RevisionMethods,
                               ProjectMethods, GroupMethods, FavoritesMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self
        #self.user_methods = UserMethods(self.rest_api_base)

class TableauRestApiConnection27(WorkbookMethods27, UserMethods27, SubscriptionMethods27, SiteMethods27,
                               ScheduleMethods27, RevisionMethods27, ProjectMethods27, GroupMethods27, FavoritesMethods27,
                               ExtractMethods27, DatasourceMethods27, TableauRestApiConnection, TableauRestApiBase27):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase27.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection28(WorkbookMethods28, UserMethods28, SubscriptionMethods28, SiteMethods28,
                               ScheduleMethods28, RevisionMethods28, ProjectMethods28,
                               GroupMethods28, FavoritesMethods28,
                               ExtractMethods28, DatasourceMethods28, TableauRestApiConnection27, TableauRestApiBase28):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase28.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection30(WorkbookMethods30, UserMethods30, SubscriptionMethods30, SiteMethods30,
                               ScheduleMethods30, RevisionMethods30, ProjectMethods30,
                                 GroupMethods30, FavoritesMethods30,
                               ExtractMethods30, DatasourceMethods30, TableauRestApiConnection28, TableauRestApiBase30):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase30.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection31(WorkbookMethods31, UserMethods31, SubscriptionMethods31, SiteMethods31,
                               ScheduleMethods31, RevisionMethods31, ProjectMethods31,
                                 GroupMethods31, FavoritesMethods31,
                               ExtractMethods31, DatasourceMethods31, TableauRestApiConnection30, TableauRestApiBase31):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase31.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection32(WorkbookMethods32, UserMethods32, SubscriptionMethods32, SiteMethods32,
                                 ScheduleMethods32, RevisionMethods32,
                                 ProjectMethods32, GroupMethods32,
                                 FavoritesMethods32, ExtractMethods32, DatasourceMethods32, AlertMethods32,
                                 TableauRestApiConnection31, TableauRestApiBase32):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase32.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection33(WorkbookMethods33, UserMethods33, SubscriptionMethods33, SiteMethods33,
                                 ScheduleMethods33, RevisionMethods33, ProjectMethods33,
                                 GroupMethods33, FlowMethods33,
                                 FavoritesMethods33, ExtractMethods33, DatasourceMethods33, AlertMethods33,
                                 TableauRestApiConnection32, TableauRestApiBase33):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase33.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection34(WorkbookMethods34, UserMethods34, SubscriptionMethods34, SiteMethods34,
                                 ScheduleMethods34, RevisionMethods34, ProjectMethods34,
                                 GroupMethods34, FlowMethods34,
                                 FavoritesMethods34, ExtractMethods34, DatasourceMethods34, AlertMethods34,
                                 TableauRestApiConnection33, TableauRestApiBase34,):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase34.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection35(WorkbookMethods35, UserMethods35, SubscriptionMethods35, SiteMethods35,
                                 ScheduleMethods35, RevisionMethods35, ProjectMethods35, MetadataMethods35,
                                 GroupMethods35, FlowMethods35,
                                 FavoritesMethods35, ExtractMethods35, DatasourceMethods35, AlertMethods35,
                                 TableauRestApiConnection34, TableauRestApiBase35):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase35.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self

class TableauRestApiConnection36(WorkbookMethods36, WebhooksMethods36, UserMethods36, SubscriptionMethods36,
                                 SiteMethods36,
                                 ScheduleMethods36, RevisionMethods36, ProjectMethods36, MetadataMethods36,
                                 GroupMethods36, FlowMethods36,
                                 FavoritesMethods36, ExtractMethods36, DatasourceMethods36, AlertMethods36,
                                 TableauRestApiConnection35, TableauRestApiBase36):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase36.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.rest_api_base = self
