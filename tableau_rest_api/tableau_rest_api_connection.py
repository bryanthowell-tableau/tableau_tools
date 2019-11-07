from .methods import *

# This is a now a composite class that brings together all the different methods under one roof
class TableauRestApiConnection(WorkbookMethods, UserMethods, SiteMethods, ScheduleMethods, ProjectMethods, GroupMethods,
                               ExtractMethods, DatasourceMethods, TableauRestApiBase):
    pass
