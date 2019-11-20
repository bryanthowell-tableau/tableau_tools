# -*- coding: utf-8 -*-
from typing import Optional, List, Dict
import time, datetime
import json
# from tableau_tools.logger import Logger
# from tableau_tools.tableau_server_rest import TableauServerRest, TableauServerRest33, TableauServerRest35
# from tableau_tools.tableau_rest_api.url_filter import *
# from tableau_tools.tableau_rest_api.sort import *
from ...tableau_tools import *

# This is meant to test all querying functionality of the tableau_tools library.
# It is intended to be pointed at existing sites on existing Tableau Servers, with enough content for
# the methods to be tested in a serious way

# For testing the Add, Update, Delete and Publishing functionality, please use ..._add_update_delete_tests.py

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


# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):
    # Create a default connection, test pulling server information
    t = TableauServerRest35(server=server_url, username=username, password=password,
                            site_content_url=site_content_url)
    t.signin()
    t.enable_logging(rest_request_log_obj)

    # Server info and methods
    server_info = t.query_server_info()
    server_version = t.query_server_version()
    api_version = t.query_api_version()
    build_version = t.query_build_number()
    log_obj.log_xml_response(server_info)
    log_obj.log("Server Version {} with API Version {}, build {}".format(server_version, api_version, build_version))

    # What can you know about the Sites
    sites = t.sites.query_sites()
    log_obj.log_xml_response(sites)

    sites_json = t.sites.query_sites_json()
    log_obj.log(json.dumps(sites_json))

    content_urls = t.sites.query_all_site_content_urls()
    log_obj.log("{}".format(content_urls))

    # Step 2: Project tests
    project_tests(t)

    # Step 3: Group tests
    group_tests(t)

    # Step 5: User Tests
    user_tests(t)

    # Step 6: Publishing Workbook Tests
    workbooks_tests(t)

    #datasources_tests(t)
    # Step 7: Subscription tests
    # if isinstance(test_site, TableauRestApiConnection23):
    #    subscription_tests(test_site)

    # These capabilities are only available in later API versions
    # Step 9: Scheduling tests
    # if isinstance(test_site, TableauRestApiConnection23):
    #    schedule_test(test_site)

    # Step 10: Extract Refresh tests


def project_tests(t: TableauServerRest33):
    print('Testing project methods')
    all_projects = t.projects.query_projects()
    log_obj.log_xml_response(all_projects)
    all_projects_dict = t.convert_xml_list_to_name_id_dict(all_projects)
    log_obj.log("{}".format(all_projects_dict))

    all_projects_json = t.projects.query_projects_json()
    log_obj.log("{}".format(all_projects_json))
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
    explorer_filter = UrlFilter.create_site_role_filter(site_role="Explorer")

    # Create a filter that was last updated by
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=15)
    time_to_filter_by = today - offset_time
    # Tableau Time Filters require this format: YYYY-MM-DDTHH:MM:SSZ
    filter_time_string = time_to_filter_by.isoformat('T')[:19] + 'Z'

    last_login_filter = UrlFilter.create_last_login_filter(operator="gte", last_login_time=filter_time_string)

    filtered_users = t.users.query_users(site_role_filter=explorer_filter, last_login_filter=last_login_filter,
                                         sorts=[SortAscending("name"), ])

    print('Finished User tests')


def workbooks_tests(t: TableauServerRest):
    print("Starting Workbook tests")

    default_project = t.projects.query_project('Default')
    wbs_on_site = t.workbooks.query_workbooks()
    wbs_in_project = t.workbooks.query_workbooks_in_project(project_name_or_luid=default_project.luid)
    for wb in wbs_in_project:
        wb_luid_from_obj = wb.get('id')
        wb_luid_lookup = t.query_workbook_luid(wb_name=wb.get('name'), proj_name_or_luid=default_project.luid)
        vws_in_wb = t.workbooks.query_workbook_views(wb_name_or_luid=wb_luid_from_obj)
        vws_in_wb_json = t.workbooks.query_workbook_views_json(wb_name_or_luid=wb_luid_from_obj)
        t.workbooks.query_workbook_connections(wb_name_or_luid=wb_luid_from_obj)
        # Published Workbook Options
        wb_obj = t.get_published_workbook_object(workbook_name_or_luid=wb_luid_from_obj)
        permissions_obj_list = wb_obj.get_permissions_obj_list()
        break

    vws_on_site = t.workbooks.query_views()
    vws_json = t.workbooks.query_views_json()
    # .workbooks.query_view(vw_name_or_luid='GreatView')



def datasources_tests(t: TableauServerRest):
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


def schedule_tests(t: TableauServerRest):
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


def subscription_tests(t: TableauServerRest):
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


def revision_tests(t: TableauServerRest, workbook_name: str, project_name: str):
    print('Starting revision tests')
    revisions = t.get_workbook_revisions(workbook_name, project_name)
    t.log('There are {} revisions of workbook {}'.format(len(revisions), workbook_name))

    print('Finished revision tests')


def extract_refresh_tests(t: TableauServerRest):
    print('Starting Extract Refresh tests')
    tasks = t.get_extract_refresh_tasks()

    print('Finished Extract Refresh tests')


for server in servers:
    print("Logging in to {}".format(servers[server]['server']))
    run_tests(servers[server]['server'], servers[server]['username'], servers[server]['password'])