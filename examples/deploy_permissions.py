# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tableau_rest_api import *
import time

logger = Logger(u'scratch.log')

server = 'http://127.0.0.1'
username = ''
password = ''
d = TableauRestApiConnection(server, username, password)
d.signin()
d.enable_logging(logger)

sites_to_create = {'site_1': 'Site 1', 'site_2': 'Site 2', 'site_3': 'Site 3'}
# Create each of the sites
for site in sites_to_create:
    d.create_site(sites_to_create[site], site)
    t = TableauRestApiConnection(server, username, password, site)
    t.signin()
    t.enable_logging(logger)
    groups_to_create = ['Group 1', 'Group 2', 'Group 3']
    projects_to_create = ['Project 1', 'Project 2', 'Project 3']
    groups_dict = {}
    for group in groups_to_create:
        # When you create, a LUID is returned. We store them here to use for further commands
        groups_dict[group] = t.create_group(group)
    projects_dict = {}

    # When a project is created, it takes the settings of the Default project
    # Set All Users to Undefined
    time.sleep(2)
    all_users_luid = t.query_group_luid_by_name(u'All Users')
    default_project_luid = t.query_project_luid_by_name(u'Default')
    default_proj_obj = t.get_project_object_by_luid(default_project_luid)

    gcap_obj_p = t.get_grantee_capabilities_object(u'group', all_users_luid, u'project')
    gcap_obj_p.set_all_to_unspecified()
    default_proj_obj.set_permissions_by_gcap_obj(gcap_obj_p)

    gcap_obj_ds = t.get_grantee_capabilities_object(u'group', all_users_luid, u'workbook')
    gcap_obj_ds.set_all_to_unspecified()
    default_proj_obj.datasource_default.set_permissions_by_gcap_obj(gcap_obj_ds)

    gcap_obj_wb = t.get_grantee_capabilities_object(u'group', all_users_luid, u'workbook')
    gcap_obj_wb.set_all_to_unspecified()
    default_proj_obj.workbook_default.set_permissions_by_gcap_obj(gcap_obj_wb)

    for project in projects_to_create:
        # Deploy them locked
        # When you create, a LUID is returned. We store them here to use for further commands
        projects_dict[project] = t.create_project(project, locked_permissions=True)

        time.sleep(2)
        # Project Object represents the settings of the publish project
        proj_obj = t.get_project_object_by_luid(projects_dict[project])

        # Each set of permissions is represented by one GranteeCapabilities object
        # Project class can do testing and comparison if send them one at a time
        g1_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 1'], u'project')
        g1_gcap_obj.set_capabilities_to_match_role(u'Viewer')
        proj_obj.set_permissions_by_gcap_obj(g1_gcap_obj)

        g2_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 2'], u'project')
        g2_gcap_obj.set_capabilities_to_match_role(u'Viewer')
        proj_obj.set_permissions_by_gcap_obj(g2_gcap_obj)

        g3_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 3'], u'project')
        g3_gcap_obj.set_capabilities_to_match_role(u'Publisher')
        proj_obj.set_permissions_by_gcap_obj(g3_gcap_obj)

        # Also need to set the Default Workbook permissions
        g1_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 1'], u'workbook')
        g1_gcap_obj.set_capabilities_to_match_role(u'Editor')
        proj_obj.workbook_default.set_permissions_by_gcap_obj(g1_gcap_obj)

        g2_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 2'], u'workbook')
        g2_gcap_obj.set_capabilities_to_match_role(u'Interactor')
        proj_obj.workbook_default.set_permissions_by_gcap_obj(g2_gcap_obj)

        g3_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 3'], u'workbook')
        g3_gcap_obj.set_capabilities_to_match_role(u'Viewer')
        proj_obj.workbook_default.set_permissions_by_gcap_obj(g3_gcap_obj)

        # Also need to set the Default Data Source permissions
        g1_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 1'], u'datasource')
        g1_gcap_obj.set_capabilities_to_match_role(u'Connector')
        proj_obj.datasource_default.set_permissions_by_gcap_obj(g1_gcap_obj)

        g2_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 2'], u'datasource')
        g2_gcap_obj.set_capabilities_to_match_role(u'Connector')
        proj_obj.datasource_default.set_permissions_by_gcap_obj(g2_gcap_obj)

        g3_gcap_obj = t.get_grantee_capabilities_object(u'group', groups_dict['Group 3'], u'datasource')
        g3_gcap_obj.set_capabilities_to_match_role(u'Editor')
        proj_obj.datasource_default.set_permissions_by_gcap_obj(g3_gcap_obj)
