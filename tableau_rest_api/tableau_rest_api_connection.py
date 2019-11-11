from .methods import *

# This is a now a composite class that brings together all the different methods under one roof
class TableauRestApiConnection(WorkbookMethods, UserMethods, SiteMethods, ServerMethods,
                               ScheduleMethods, RevisionMethods, ProjectMethods, GroupMethods, FavoritesMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    pass

