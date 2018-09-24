# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *

capabilities_to_set = {u"Download Full Data": u"Deny"}
tableau_group_name = u'All Users'

server = u'http://'
username = u'username'
password = u'secure_password'
site = u'a_site'

logger = Logger(u'Permissions.log')

t = TableauRestApiConnection25(server, username, password, site)
t.signin()
t.enable_logging(logger)

projects = t.query_projects()
projects_dict = t.convert_xml_list_to_name_id_dict(projects)


# Determine the identifer (LUID) of the Group
try:
    all_users_group_luid = t.query_group_luid(tableau_group_name)
except NoMatchFoundException:
    print(u"No group found using the name provided")
    exit()


def update_workbook_permissions(project_obj, published_workbook_object, group_luid, capabilities_dict):
    """
    :type project_obj: Project
    :type published_workbook_object: Workbook
    :type group_luid: unicode
    :type capabilities_dict: dict
    :return:
    """
    # Query the permissions objects (comes as a list)
    permissions = published_workbook_object.get_permissions_obj_list()
    print(u"Retrieved Permissions")
    # Get the permissions object for the group_luid
    # Have to check for group_luid not being set at all
    does_group_have_any_permissions = False
    for perm_obj in permissions:

        if perm_obj.luid == group_luid:
            does_group_have_any_permissions = True
            for cap in capabilities_dict:
                perm_obj.set_capability(cap, capabilities_dict[cap])

    # Send back the whole of the original list of permissions, with the one modified.
    if does_group_have_any_permissions is True:
        print(u'Updating Existing Permissions for Group')
        published_workbook_object.set_permissions_by_permissions_obj_list(permissions)

    # If there are no permissions at all, create Permissions object for it
    elif does_group_have_any_permissions is False:
        new_perm_obj = project_obj.create_workbook_permissions_object_for_group(all_users_group_luid)
        for cap in capabilities_dict:
            new_perm_obj.set_capability(cap, capabilities_dict[cap])
        print(u'No permissions found for group, adding new permissions')
        published_workbook_object.set_permissions_by_permissions_obj_list([new_perm_obj, ])


for project in projects_dict:
    # List of projects we want to search in. Uncomment if you want to limit which projects are affected:
    # projects_to_change = [u'Project A', u'Project C']
    # if project not in projects_to_change:
    #    continue

    # Update the workbook_defaults in the project

    # Get the project as an object so permissions are available
    try:
        project_object = t.query_project(project)
    except NoMatchFoundException:
        print(u"No project found with the given name, check the log")
        exit()

    workbook_defaults_obj = project_object.workbook_defaults
    print(u"Updating the Project's Workbook Defaults")
    update_workbook_permissions(project_object, workbook_defaults_obj, all_users_group_luid, capabilities_to_set)

    # Update the workbooks themselves (if the permissions aren't locked, because this would be a waste of time)
    if project_object.are_permissions_locked() is False:
        wbs_in_project = t.query_workbooks_in_project(project)
        wbs_dict = t.convert_xml_list_to_name_id_dict(wbs_in_project)
        for wb in wbs_dict:
            # Second parameter project_name is unecessary when passing a LUID
            # That is why you reference wbs_dict[wb], rather than wb directly, which is just the name
            wb_obj = t.get_published_workbook_object(wbs_dict[wb], u"")
            print(u'Updating workbook with LUID {}'.format(wbs_dict[wb]))
            update_workbook_permissions(project_object, wb_obj, all_users_group_luid, capabilities_to_set)
