from .methods import *

# This is a now a composite class that brings together all the different methods under one roof, but compatible with
# older scripts (easy upgrade)
class TableauRestApiConnection(WorkbookMethods, UserMethods, SiteMethods, ServerMethods,
                               ScheduleMethods, RevisionMethods, ProjectMethods, GroupMethods, FavoritesMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    pass

class TableauRestApiConnection27(WorkbookMethods27, UserMethods27, SiteMethods27, ServerMethods27,
                               ScheduleMethods27, RevisionMethods27, ProjectMethods27, GroupMethods27, FavoritesMethods27,
                               ExtractMethods27, DatasourceMethods27, TableauRestApiBase27):
    pass

class TableauRestApiConnection28(WorkbookMethods28, UserMethods28, SiteMethods28, ServerMethods28,
                               ScheduleMethods28, RevisionMethods28, ProjectMethods28, GroupMethods28, FavoritesMethods28,
                               ExtractMethods28, DatasourceMethods28, TableauRestApiBase28):
    pass

class TableauRestApiConnection30(WorkbookMethods30, UserMethods30, SiteMethods30, ServerMethods30,
                               ScheduleMethods30, RevisionMethods30, ProjectMethods30, GroupMethods30, FavoritesMethods30,
                               ExtractMethods30, DatasourceMethods30, TableauRestApiBase30):
    pass

class TableauRestApiConnection31(WorkbookMethods31, UserMethods31, SiteMethods31, ServerMethods31,
                               ScheduleMethods31, RevisionMethods31, ProjectMethods31, GroupMethods31, FavoritesMethods31,
                               ExtractMethods31, DatasourceMethods31, TableauRestApiBase31):
    pass

class TableauRestApiConnection32(WorkbookMethods32, UserMethods32, SiteMethods32, ServerMethods32,
                                 ScheduleMethods32, RevisionMethods32, ProjectMethods32, GroupMethods32,
                                 FavoritesMethods32, ExtractMethods32, DatasourceMethods32, AlertMethods32,
                                 TableauRestApiBase32):
    pass

class TableauRestApiConnection33(WorkbookMethods33, UserMethods33, SiteMethods33, ServerMethods33,
                                 ScheduleMethods33, RevisionMethods33, ProjectMethods33, GroupMethods33, FlowMethods33,
                                 FavoritesMethods33, ExtractMethods33, DatasourceMethods33, AlertMethods33,
                                 TableauRestApiBase33):
    pass

class TableauRestApiConnection34(WorkbookMethods34, UserMethods34, SiteMethods34, ServerMethods34,
                                 ScheduleMethods34, RevisionMethods34, ProjectMethods34, GroupMethods34, FlowtMethods34,
                                 FavoritesMethods34, ExtractMethods34, DatasourceMethods34, AlertMethods34,
                                 TableauRestApiBase34):
    pass

