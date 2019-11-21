# -*- coding: utf-8 -*-
from typing import Optional, List, Dict
import time, datetime
import json

from ...tableau_tools import *

# This is meant to test all non-querying functionality of the tableau_tools library.
# It creates a new site on whatever Tableau Server it has been pointed to
# Please run as a Server Admin level user

# For testing the Add, Update, Delete and Publishing functionality, please use ..._querying_tests.py

# Other than printing messages before and afte each test, logging of what actually happens going into the log file
# rather than to the console.

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
    # "2019.3 Windows": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""},

    # "2019.4 Windows": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""},
    # "2019.4 Linux": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""}
}

log_obj = Logger('tableau_rest_api_connection_querying_tests.log')
rest_request_log_obj = Logger('tableau_rest_api_connection_querying_tests_rest.log')

# Set of Words in all sorts of characters to make sure everything works right
words = ['ASCII', 'Οὐχὶ ταὐτὰ', 'γιγνώσκειν', 'რეგისტრაცია', 'Международную', 'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา',
         'አይተዳደርም።', '晚飯', '晩ご飯', '저녁밥', 'bữa ăn tối', 'Señor']

tdsx_to_publish = 'Some Datasource.tdsx'
twbx_to_publish = 'Some Workbook.twbx'

# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):
    # Create a default connection, test pulling server information
    d = TableauServerRest35(server=server_url, username=username, password=password,
                            site_content_url=site_content_url)
    d.signin()
    d.enable_logging(rest_request_log_obj)

    t = create_test_site(tableau_server_default_connection=d, server_url=server_url, username=username,
                         password=password, logger=rest_request_log_obj)
    project_tests(t)
    # Create users first
    user_tests(t)
    # Then drop them into groups
    group_tests(t)

    workbooks_tests(t)
    datasources_tests(t)

    favorites_tests(t)

    # Gotta Create Schedules first
    schedule_tests(t)
    # then can subscribe
    subscription_tests(t)
    # schedule_tests(t)
    revision_tests(t)
    extract_tests(t)
    alerts_tests(t) # 2018.3+
    flow_tests(t) # 2019.2+
    metadata_tests(t)  # 2019.3+ only
    # webhook_test(t)  # 2019.4+ only

def create_test_site(tableau_server_default_connection: TableauServerRest, server_url: str, username: str,
                     password: str, logger: Logger) -> TableauServerRest:
    print("Creating a test site")
    logger.log('Creating test site')
    default_site = tableau_server_default_connection
    # Assign this however you'd like
    new_site_content_url = 'tableau_tools_test_site'
    new_site_name = 'Test Site 1'
    new_site_name_to_change_to = 'Test Site - tableau_tools'

    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        logger.log('Received content_url to delete {}'.format(new_site_content_url))
        test_site = TableauServerRest(server_url, username, password, new_site_content_url)
        test_site.signin()
        test_site.enable_logging(logger)
        logger.log('Signed in successfully to {}'.format(new_site_content_url))

        site_xml = test_site.query_current_site()
        logger.log('Attempting to delete current site')
        test_site.sites.delete_current_site()
        logger.log("Deleted site {}".format(new_site_name))
    except RecoverableHTTPException as e:
        logger.log(e.tableau_error_code)
        logger.log("Cannot delete site that does not exist, assuming it already exists and continuing")

    try:
        # Create the new site
        logger.log('Now going into the create site')
        default_site.log('Logging with the log function')
        new_site_id = default_site.create_site(new_site_name, new_site_content_url)
        logger.log('Created new site ' + new_site_id)
    # This shouldn't happen if the existing check and delete happened earlier, but might as well protect
    except AlreadyExistsException as e:
        print((e.msg))
        print("Cannot create new site due to error, exiting")
        exit()

    # Once we've created the site, we need to sign into it to do anything else
    test_site = TableauServerRest(server=server_url, username=username,
                                         password=password, site_content_url=new_site_content_url)
    test_site.signin()
    test_site.enable_logging(logger)
    logger.log('Signed in successfully to ' + new_site_content_url)

    # Update the site name
    logger.log('Updating site name')
    test_site.sites.update_site(site_name=new_site_name_to_change_to)

    logger.log('Updating everything about site')
    if isinstance(test_site, TableauRestApiConnection):
        # If e-mail subscriptions are disabled for the Server, this comes back with an error
        #test_site.update_site(content_url=new_site_content_url, admin_mode=u'ContentAndUsers', user_quota=u'30',
        #                      storage_quota=u'400', disable_subscriptions=False, state=u'Active',
        #                      revision_history_enabled=True, revision_limit=u'15')

        # THis could be different depending on the version / if Named Users
        test_site.sites.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers', user_quota='30',
                              storage_quota='400', state='Active',
                              revision_history_enabled=True, revision_limit='15')
    else:
        test_site.sites.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers', user_quota='30',
                              storage_quota='400', disable_subscriptions=False, state='Active')

    print('Finished creating new site')
    return test_site

def project_tests(t: TableauServerRest33):
    print('Testing project methods')

    new_proj_obj = t.projects.create_project(project_name="A Great Project",
                                             project_desc="Literally the greatest project imaginable")

    new_child_proj_obj = t.projects.create_project(project_name="Little baby child project",
                                                   parent_project_name_or_luid=new_proj_obj.luid)

    two_proj_obj = t.projects.create_project(project_name="Inferior Project Slated for Removal",
                                             project_desc="This one won't last long")

    t.projects.update_project(new_proj_obj.luid, new_project_name="An Even Greater Project")

    t.projects.delete_projects(two_proj_obj.luid)
    del two_proj_obj

    new_proj_obj.lock_permissions()
    new_proj_obj.unlock_permissions()
    new_proj_obj.clear_all_permissions()


    # Permissions setting
    all_users_groups_luid = t.query_group_luid(group_name="All Users")

    all_users_perms = new_proj_obj.create_project_permissions_object_for_group(group_name_or_luid=all_users_groups_luid)
    all_users_perms.set_capability_to_allow("View")
    new_proj_obj.set_permissions_by_permissions_obj_list([all_users_perms, ])

    all_users_wb_perms = new_proj_obj.create_workbook_permissions_object_for_group(group_name_or_luid=all_users_groups_luid,
                                                                                   role="Interactor")
    new_proj_obj.workbook_defaults.set_permissions_by_permissions_obj_list([all_users_wb_perms, ])

    all_users_ds_perms = new_proj_obj.create_datasource_permissions_object_for_group(group_name_or_luid="All Users")
    all_users_ds_perms.set_all_to_deny()
    new_proj_obj.datasource_defaults.set_permissions_by_permissions_obj_list([all_users_ds_perms, ])

    # Create other projects with all the character set tests
    for word in words:
        t.projects.create_project(project_name=word, no_return=True)

    print("Finished testing project methods")


def group_tests(t: TableauServerRest):
    print("Starting group tests")

    group_luids = []
    for word in words:
        group_luid = t.groups.create_group(group_name=word)
        group_luids.append(group_luid)

        # Alternatively, if you had a set of AD groups to add:
        # t.groups.create_group_from_ad_group()

    all_users = t.users.query_users()
    all_users_dict = t.convert_xml_list_to_name_id_dict(all_users)
    all_user_luids = list(all_users_dict.values())  # .values() returns a ValuesView, which needs to be cast to list
    # Just picking one Group at random here basically
    t.groups.add_users_to_group(username_or_luid_s=all_user_luids, group_name_or_luid=group_luids[0])

    # Randomly removing a few users
    t.groups.remove_users_from_group(username_or_luid_s=[all_user_luids[0], all_user_luids[2]])

    # Deleting second group randomly
    t.groups.delete_groups(group_name_or_luid_s=group_luids[1])

    t.groups.update_group(name_or_luid=group_luids[2], new_group_name='Updated Group Name')

    # If you needed to sync an AD group
    # t.groups.sync_ad_group()

    print('Finished group tests')


def user_tests(t: TableauServerRest):
    print('Starting User tests')
    user_luids = []
    for word in words:
        # This sends both a CREATE and an UPDATE, to set everything with one command
        user_luid  = t.users.add_user(username=word, fullname=word.upper(), site_role='Interactor',
                         email="{}@nowhere.com".format(word))
        user_luids.append(user_luid)

    # This sends only the basic ADd command, without the update for additional properties. Faster if you really needed it
    t.users.add_user_by_username(username="A New User", site_role="Interactor")

    t.users.update_user(username_or_luid=user_luids[0], password="AVerySecurePasswordForYou", site_role="Explorer")

    # Example of using Full Name property for additional context in RLS calculations, rather than a name itself
    t.users.update_user(username_or_luid=user_luids[3], full_name="GroupA||Region17||Other Stuff")

    # Unnlicense a user
    t.users.unlicense_users(username_or_luid_s=user_luids[3])

    # This deletes them completely
    t.users.remove_users_from_site(username_or_luid_s=[user_luids[1], user_luids[2]])


    print('Finished User tests')


def workbooks_tests(t: TableauServerRest):
    print("Starting Workbook tests")

    wb_name = "My Published Workbook"
    default_project = t.projects.query_project('Default')
    # log_obj.log('Publishing workbook as {}'.format(twbx_to_publish)
    new_wb_luid = t.workbooks.publish_workbook(workbook_filename=twbx_to_publish, workbook_name=wb_name,
                                               project_obj=default_project, overwrite=True)

    # Repeat Multiple times to creates some revisions
    time.sleep(3)
    new_wb_luid = t.workbooks.publish_workbook(workbook_filename=twbx_to_publish, workbook_name=wb_name,
                                               project_obj=default_project, overwrite=True)

    time.sleep(3)
    new_wb_luid = t.workbooks.publish_workbook(workbook_filename=twbx_to_publish, workbook_name=wb_name,
                                               project_obj=default_project, overwrite=True)

    time.sleep(3)

    # Publish second one to be deleted
    new_wb_luid_2 = t.workbooks.publish_workbook(workbook_filename=twbx_to_publish, workbook_name="{} - 2".format(wb_name),
                                                 project_obj=default_project, overwrite=True)
    time.sleep(3)

    projects = t.projects.query_projects()
    projects_dict = t.convert_xml_list_to_name_id_dict(projects)
    projects_list = list(projects_dict.keys())

    log_obj.log('Moving workbook to {} project'.format(projects_list[0]))
    t.workbooks.update_workbook(workbook_name_or_luid=new_wb_luid,
                                new_project_name_or_luid=projects_dict[projects_list[0]], show_tabs=True)

    log_obj.log("Querying workbook as Published Object")
    wb_obj = t.get_published_workbook_object(workbook_name_or_luid=new_wb_luid)

    # Permissions type stuff on it
    wb_permissions = wb_obj.get_permissions_obj_list()
    wb_obj.clear_all_permissions()
    wb_obj.
    wb_obj.set_permissions_by_permissions_obj_list()


    wb_views = t.query_workbook_views(new_wb_luid)
    wb_views_dict = t.convert_xml_list_to_name_id_dict(wb_views)

    t.log(str(wb_views_dict))

    for wb_view in wb_views_dict:
        t.log("Adding {} to favorites for me".format(wb_view))
        t.add_view_to_user_favorites('Fav - {}'.format(wb_view), t.username, wb_view, wb_name_or_luid=new_wb_luid)

    for wb_view in wb_views_dict:
        t.log("Deleting {} from favorites for me".format(wb_view))
        t.delete_views_from_user_favorites(wb_views_dict.get(wb_view), t.username, new_wb_luid)

    t.log('Adding tags to workbook')
    t.add_tags_to_workbook(new_wb_luid, ['workbooks', 'flights', 'cool', '晚飯'])

    t.log('Deleting a tag from workbook')
    t.delete_tags_from_workbook(new_wb_luid, 'flights')

    t.log("Add workbook to favorites for me")
    t.add_workbook_to_user_favorites('My favorite workbook', new_wb_luid, t.username)

    t.log("Deleting workbook from favorites for me")
    t.delete_workbooks_from_user_favorites(new_wb_luid, t.username)

    #    # Saving view as file
    #    for wb_view in wb_views_dict:
    #        t_site.log(u"Saving a png for {}".format(wb_view)
    #        t_site.save_workbook_view_preview_image(wb_luid, wb_views_dict.get(wb_view), '{}_preview'.format(wb_view))

    t.log('Deleting workbook')
    t.delete_workbooks(new_wb_luid_2)


def datasources_tests(t: TableauServerRest):
    print("Starting Datasources tests")
    dses_xml = t.datasources.query_datasources(sorts=[SortAscending('name'), ])
    dses_json = t.datasources.query_datasources_json()
    for ds in dses_xml:
        ds_luid = ds.get('id')
        ds_content_url = t.datasources.query_datasource_content_url(datasource_name_or_luid=ds_luid)
        pub_ds_obj = t.get_published_datasource_object(datasource_name_or_luid=ds_luid)
        permissions_obj_list = pub_ds_obj.get_permissions_obj_list()
        break

    print('Finished Datasource Tests')

def favorites_tests(t: TableauServerRest):
    print("Starting Favorites tests")
    user_favorites_xml = t.favorites.query_user_favorites(username_or_luid=t.user_luid)
    user_favorites_json = t.favorites.query_user_favorites_json(username_or_luid=t.user_luid)
    print("Finished Favorites tests")

def subscription_tests(t: TableauServerRest):
    print('Starting Subscription tests')

    all_subscriptions = t.subscriptions.query_subscriptions()
    sub_dict = t.convert_xml_list_to_name_id_dict(all_subscriptions)

    # Subscriptions for one user
    users = t.users.query_users()
    for user in users:
        user_luid = user.get('id')
        one_user_subscriptions = t.subscriptions.query_subscriptions(username_or_luid=user_luid)
        break

    print('Finished subscription tests')

def schedules_tests(t: TableauServerRest):
    print("Starting Schedules tests")
    schedules = t.schedules.query_schedules()
    schedules_json = t.schedules.query_schedules_json()

    sub_schedules = t.schedules.query_subscription_schedules()

    print("Finished Schedules tests")

def revision_tests(t: TableauServerRest):
    print('Starting revision tests')
    wbs = t.workbooks.query_workbooks()
    for wb in wbs:
        revisions = t.revisions.get_workbook_revisions(workbook_name_or_luid=wb.get('id'))
        log_obj.log('There are {} revisions of workbook {}'.format(len(revisions), wb.get('name')))
        log_obj.log_xml_response(revisions)
        break
    dses = t.datasources.query_datasources()
    for ds in dses:
        revisions = t.revisions.get_datasource_revisions(datasource_name_or_luid=ds.get('id'))
        log_obj.log('There are {} revisions of datasource {}'.format(len(revisions), ds.get('name')))
        log_obj.log_xml_response(revisions)
        break

    print('Finished revision tests')


def extract_tests(t: TableauServerRest):
    print('Starting Extract tests')
    response = t.extracts.get_extract_refresh_tasks()
    log_obj.log_xml_response(response)
    for tasks in response:
        for task in tasks:
            # This is obviously unnecessary but it's just to test the functioning of the method
            try:
                t.extracts.get_extract_refresh_task(task_luid=task.get('id'))
            except NoMatchFoundException as e:
                log_obj.log(e.msg)
            break

    refresh_schedules = t.schedules.query_extract_schedules()
    for sched in refresh_schedules:
        try:
            refresh_tasks = t.extracts.get_extract_refresh_tasks_on_schedule(schedule_name_or_luid=sched.get('id'))
        except NoMatchFoundException as e:
            log_obj.log(e.msg)
        break

    # Only possible in 2019.3+, also risky to run as tests, so commented out
    # t.extracts.encrypt_extracts()
    # t.extracts.decrypt_extracts()
    # t.extracts.reencrypt_extracts()

    # See the Add / Update / Delete test suite for the commands to run the extract refreshes or encrypt them

    print('Finished Extract Refresh tests')

def alerts_tests(t: TableauServerRest32):
    print("Starting Alerts tests")
    alerts = t.alerts.query_data_driven_alerts()
    for alert in alerts:
        t.alerts.query_data_driven_alert_details(data_alert_luid=alert.get('id'))
        break
    vws = t.workbooks.query_views()
    for vw in vws:
        view_alerts = t.alerts.query_data_driven_alerts_for_view(view_luid=vw.get('id'))
        break
    print("Finished Alerts tests")

def flow_tests(t: TableauServerRest33):
    print("Starting Flows tests")
    response = t.flows.get_flow_run_tasks()
    for flow_tasks in response:
        for flow_task in flow_tasks:
            t.flows.get_flow_run_task(task_luid=flow_task.get('id'))
    print("Finished Flows tests")

def metadata_tests(t: TableauServerRest35):
    print("Starting Metadata tests")
    dbs = t.metadata.query_databases()
    for db in dbs:
        t.metadata.query_database(database_name_or_luid=db.get('id'))

        break
    tables = t.metadata.query_tables()
    for table in tables:
        t.metadata.query_table(table_name_or_luid=table.get('id'))
        t.metadata.query_columns_in_a_table(table_name_or_luid=table.get('id'))
        break

    # GraphQL queries
    # Does this need to be wrapped further?
    my_query = """
query ShowMeOneTableCalledOrders{
  tables ("filter": {"name": "Orders"}){
    name
    columns {
      name
    }
  }
}
"""
    try:
        graphql_response = t.metadata.graphql(graphql_query=my_query)
        log_obj.log(json.dumps(graphql_response))
    except InvalidOptionException as e:
        log_obj.log(e.msg)
    print("Finished Metadata tests")

for server in servers:
    print("Logging in to {}".format(servers[server]['server']))
    run_tests(servers[server]['server'], servers[server]['username'], servers[server]['password'])