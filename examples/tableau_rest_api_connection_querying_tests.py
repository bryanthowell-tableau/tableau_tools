# -*- coding: utf-8 -*-

from ...tableau_tools.tableau_rest_api import *
from ...tableau_tools import *
import time
# This is meant to test all querying functionality of the tableau_tools library.
# It is intended to be pointed at existing sites on existing Tableau Servers, with enough content for
# the methods to be tested in a serious way

# For testing the Add, Update, Delete and Publishing functionality, please use ..._add_update_delete_tests.py

# Other than printing messages before and afte each test, logging of what actually happens going into the log file
# rather than to the console.

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
           "2019.3 Windows": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""},
           "2019.3 Linux": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""},
           "2019.4 Windows": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""},
           "2019.4 Linux": {"server": "http://127.0.0.1", "username": "", "password": "", "site_content_url": ""}
           }


# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):

    log_obj = Logger('tableau_rest_api_connection_querying_tests.log')

    # Create a default connection, test pulling server information
    t = TableauServerRest35(server=server_url, username=username, password=password,
                                       site_content_url=site_content_url)
    t.signin()
    t.enable_logging(log_obj)

    # Server info and methods
    server_info = t.query_server_info()
    server_version = t.query_server_version()
    api_version = t.query_api_version()
    build_version = t.query_build_number()
    log_obj.log_xml_response(server_info)
    log_obj.log("Server Version {} with API Version {}, build {}".format(server_version, api_version, build_version))

    # What can you know about the Sites
    t.sites.query_sites()
    t.sites.query_all_site_content_urls()
    t.sites.query_sites_json()


    # Step 2: Project tests
    project_tests(t)

    # Step 3: Group tests
    group_tests(t)

    # Step 4: Project Permissions tests
    project_permissions_tests(t)

    # Step 5: User Tests
    user_tests(t)

    # Step 6: Publishing Workbook Tests
    #workbooks_test(test_site, twbx_filename, twbx_content_name)

    # Step 7: Subscription tests
    #if isinstance(test_site, TableauRestApiConnection23):
    #    subscription_tests(test_site)

    # Step 8: Publishing Datasource tests
    #publishing_datasources_test(test_site, tdsx_filename, tdsx_content_name)

    # These capabilities are only available in later API versions
    # Step 9: Scheduling tests
    #if isinstance(test_site, TableauRestApiConnection23):
    #    schedule_test(test_site)

    # Step 10: Extract Refresh tests

def project_tests(t: TableauServerRest33):

    print('Testing project methods')
    all_projects = t.projects.query_projects()
    all_projects_dict = t.convert_xml_list_to_name_id_dict(all_projects)

    # Grab one project randomly
    for project_name in all_projects_dict:
        project_obj = t.projects.query_project(project_name_or_luid=project_name)
        project_xml = t.projects.query_project_xml_object(project_name_or_luid=project_name)
        permissions_locked = project_obj.are_permissions_locked()
        # Permissions querying
        project_permissions = project_obj.get_permissions_obj_list()
        project_permissions_xml = project_obj.get_permissions_xml()

        wb_default_permissions = project_obj.workbook_defaults.get_permissions_obj_list()
        wb_default_permissions_xml = project_obj.workbook_defaults.get_permissions_xml()

        ds_default_permissions = project_obj.datasource_defaults.get_permissions_obj_list()
        ds_default_permissions_xml = project_obj.workbook_defaults.get_permissions_xml()

        # Make conditional on API 3./3
        flow_default_permissions = project_obj.flow_defaults.get_permissions_obj_list()
        flow_default_permissions_xml = project_obj.flow_defaults.get_permissions_xml()
        
        break


    print("Finished testing project methods")


def group_tests(t: TableauServerRest):
    print("Starting group tests")

    t.log("Querying all the groups")
    groups_on_site = t.groups.query_groups()

    # Convert the list to a dict {name : luid}
    groups_dict = t.convert_xml_list_to_name_id_dict(groups_on_site)
    t.log(str(groups_dict))

    t.groups.query_groups_json()

    t.groups.query_group(group_name_or_luid="All Users")
    all_users_luid = t.query_group_luid("All Users")
    all_users_name = t.groups.query_group_name(group_luid=all_users_luid)


    print('Finished group tests')
    return groups_dict

def user_tests(t: TableauServerRest):
    print('Starting User tests')
    users = t.users.query_users()
    t.users.get_users()

    t.users.query_users_json()
    t.users.get_users_json()

    t.users.query_users_in_group(group_name_or_luid="All Users")

    users_dict = t.convert_xml_list_to_name_id_dict(users)
    t.log(str(list(users_dict.keys())))

    # Filtering and Sorting

    print('Finished User tests')


def workbooks_test(t: TableauRestApiConnection, twbx_filename:str, twbx_content_name:str,
                   twb_filename: Optional[str] = None, twb_content_name: Optional[str] = None):
    print("Starting Workbook tests")

    default_project = t.query_project('Default')
    t.log('Publishing workbook as {}'.format(twbx_content_name))
    new_wb_luid = t.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    # Repeat Multiple times to creates some revisions
    time.sleep(3)
    new_wb_luid = t.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    time.sleep(3)
    new_wb_luid = t.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    time.sleep(3)

    # Publish second one to be deleted
    new_wb_luid_2 = t.publish_workbook(twbx_filename, "{} - 2".format(twbx_content_name),
                                       default_project, overwrite=True)
    time.sleep(3)

    projects = t.query_projects()
    projects_dict = t.convert_xml_list_to_name_id_dict(projects)
    projects_list = list(projects_dict.keys())

    t.log('Moving workbook to {} project'.format(projects_list[0]))
    t.update_workbook(new_wb_luid, default_project.luid, new_project_luid=projects_dict[projects_list[0]], show_tabs=True)

    t.log("Querying workbook")
    t.query_workbook(new_wb_luid)

    # Save workbook preview image
    t.log("Saving workbook preview image")
    t.save_workbook_preview_image(new_wb_luid, 'Workbook preview')

    t.log("Downloading workbook file")
    t.download_workbook(new_wb_luid, 'saved workbook')

    t.log("Query workbook connections")
    t.query_workbook_connections(new_wb_luid)

    t.log("Querying workbook views")
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
    print('Finished Workbook tests')


def publishing_datasources_test(t: TableauRestApiConnection, tdsx_file: str, tdsx_content_name: str):
    print("Starting Datasource tests")
    default_project = t.query_project('Default')

    t.log("Publishing as {}".format(tdsx_content_name))
    new_ds_luid = t.publish_datasource(tdsx_file, tdsx_content_name, default_project, overwrite=True)

    time.sleep(3)

    projects = t.query_projects()
    projects_dict = t.convert_xml_list_to_name_id_dict(projects)
    projects_list = list(projects_dict.keys())

    t.log('Moving datasource to {} project'.format(projects_list[1]))
    t.update_datasource(new_ds_luid, default_project.luid, new_project_luid=projects_dict[projects_list[1]])

    t.log("Querying datasource")
    t.query_workbook(new_ds_luid)

    t.log('Downloading and saving datasource')
    t.download_datasource(new_ds_luid, 'saved_datasource')

    # Can't add to favorites until API version 2.3

    t.log('Adding to Favorites')
    t.add_datasource_to_user_favorites('The Greatest Datasource', new_ds_luid, t.username)

    t.log('Removing from Favorites')
    t.delete_datasources_from_user_favorites(new_ds_luid, t.username)

    # t_site.log("Publishing TDS with credentials -- reordered args")
    # tds_cred_luid = t_site.publish_datasource('TDS with Credentials.tds', 'TDS w Creds', project,
    # connection_username='postgres', overwrite=True, connection_password='')

    # t_site.log("Update Datasource connection")
    # t_site.update_datasource_connection(tds_cred_luid, 'localhost', '5432', db_username, db_password)

    # t_site.log("Deleting the published DS")
    # t_site.delete_datasources(new_ds_luid)

    print('Finished Datasource Tests')


def schedule_test(t: TableauRestApiConnection):
    print('Started Schedule tests')
    all_schedules = t.query_schedules()
    schedule_dict = t.convert_xml_list_to_name_id_dict(all_schedules)
    t.log('All schedules on Server: {}'.format(str(schedule_dict)))
    try:
        t.log('Creating a daily extract schedule')
        t.create_daily_extract_schedule('Afternoon Delight', start_time='13:00:00')
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    try:
        t.log('Creating a monthly subscription schedule')
        new_monthly_luid = t.create_monthly_subscription_schedule('First of the Month', '1',
                                                                  start_time='03:00:00', parallel_or_serial='Serial')
        t.log('Deleting monthly subscription schedule LUID {}'.format(new_monthly_luid))
        time.sleep(4)
        t.delete_schedule(new_monthly_luid)
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')
    try:
        t.log('Creating a monthly extract schedule')
        t.create_monthly_extract_schedule('Last Day of Month', 'LastDay', start_time='03:00:00', priority=25)
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    try:
        t.log('Creating a weekly extract schedule')
        weekly_luid = t.create_weekly_subscription_schedule('Mon Wed Fri', ['Monday', 'Wednesday', 'Friday'],
                                                            start_time='05:00:00')
        time.sleep(4)

        t.log('Updating schedule with LUID {}'.format(weekly_luid))
        t.update_schedule(weekly_luid, new_name='Wed Fri', interval_value_s=['Wednesday', 'Friday'])
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    print('Finished Schedule tests')


def subscription_tests(t: TableauRestApiConnection):
    print('Starting Subscription tests')
    # All users in a Group
    groups = t.query_groups()
    groups_dict = t.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    users_in_group = t.query_users_in_group(groups_dict[group_names[0]])
    users_dict = t.convert_xml_list_to_name_id_dict(users_in_group)
    usernames = list(users_dict.keys())

    wbs = t.query_workbooks()
    wbs_dict = t.convert_xml_list_to_name_id_dict(wbs)
    wb_names = list(wbs_dict.keys())

    # Grab first workbook
    wb_luid = wbs_dict[wb_names[0]]

    sub_schedules = t.query_subscription_schedules()
    sched_dict = t.convert_xml_list_to_name_id_dict(sub_schedules)
    sched_names = list(sched_dict.keys())

    # Grab first schedule
    sched_luid = sched_dict[sched_names[0]]

    # Subscribe them to the first workbook
    t.log('Adding subscription with subject Important weekly update to first workbook for all users in group 1')
    for user in users_dict:
        t.create_subscription_to_workbook('Important weekly update', wb_luid, sched_luid, users_dict[user])

    # Find the subscriptions for user 1, delete
    t.query_subscriptions()
    user_1_subs = t.query_subscriptions(username_or_luid=usernames[0])
    t.log('Deleting all subscriptions for user 1')
    for sub in user_1_subs:
        luid = sub.get('id')
        t.delete_subscriptions(luid)

    # Update user 2 subscriptions
    t.log('Updating user 2s subscriptions to second schedule')
    user_2_subs = t.query_subscriptions(username_or_luid=usernames[1])
    for sub in user_2_subs:
        luid = sub.get('id')
        t.update_subscription(luid, schedule_luid=sched_dict[sched_names[1]])

    print('Finished subscription tests')


def revision_tests(t: TableauRestApiConnection, workbook_name: str, project_name: str):
    print('Starting revision tests')
    revisions = t.get_workbook_revisions(workbook_name, project_name)
    t.log('There are {} revisions of workbook {}'.format(len(revisions), workbook_name))

    print('Finished revision tests')


def extract_refresh_test(t: TableauRestApiConnection):
    print('Starting Extract Refresh tests')
    tasks = t.get_extract_refresh_tasks()

    print('Finished Extract Refresh tests')


for server in servers:
    print("Logging in to {}".format(servers[server]['server']))
    run_tests(servers[server]['server'], servers[server]['username'], servers[server]['password'])
