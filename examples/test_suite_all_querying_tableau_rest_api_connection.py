# -*- coding: utf-8 -*-
from typing import Optional, List, Dict
import time, datetime
import json

from tableau_tools import *

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
rest_request_log_obj = Logger('tableau_rest_api_connection_querying_tests_rest_requests.log')

# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):
    # Create a default connection, test pulling server information
    t = TableauRestApiConnection35(server=server_url, username=username, password=password,
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
    sites = t.query_sites()
    log_obj.log_xml_response(sites)

    sites_json = t.query_sites_json()
    log_obj.log(json.dumps(sites_json))

    content_urls = t.query_all_site_content_urls()
    log_obj.log("{}".format(content_urls))

    project_tests(t)
    group_tests(t)
    user_tests(t)
    favorites_tests(t)
    workbooks_tests(t)
    datasources_tests(t)
    subscription_tests(t)
    # schedule_tests(t)
    revision_tests(t)
    extract_tests(t)
    alerts_tests(t) # 2018.3+
    flow_tests(t) # 2019.2+
    metadata_tests(t)  # 2019.3+ only
    # webhook_test(t)  # 2019.4+ only


def project_tests(t: TableauRestApiConnection33):
    print('Testing project methods')
    all_projects = t.query_projects()
    log_obj.log_xml_response(all_projects)
    all_projects_dict = t.xml_list_to_dict(all_projects)
    log_obj.log("{}".format(all_projects_dict))

    all_projects_json = t.query_projects_json()
    log_obj.log("{}".format(all_projects_json))
    # Grab one project randomly
    for project_name in all_projects_dict:
        project_obj = t.query_project(project_name_or_luid=project_name)
        project_xml = t.query_project_xml_object(project_name_or_luid=project_name)
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


def group_tests(t: TableauRestApiConnection):
    print("Starting group tests")

    t.log("Querying all the groups")
    groups_on_site = t.query_groups()

    # Convert the list to a dict {name : luid}
    groups_dict = t.xml_list_to_dict(groups_on_site)
    t.log(str(groups_dict))

    t.query_groups_json()

    t.query_group(group_name_or_luid="All Users")
    all_users_luid = t.query_group_luid("All Users")
    all_users_name = t.query_group_name(group_luid=all_users_luid)

    users_in_group = t.query_users_in_group(group_name_or_luid="All Users")

    print('Finished group tests')
    return groups_dict


def user_tests(t: TableauRestApiConnection):
    print('Starting User tests')
    users = t.query_users()
    t.get_users()

    t.query_users_json()
    t.get_users_json()


    users_dict = t.xml_list_to_dict(users)
    t.log(str(list(users_dict.keys())))

    # Filtering and Sorting
    explorer_filter = t.url_filters.get_site_role_filter(site_role="Explorer")

    # Create a filter that was last updated by
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=15)
    time_to_filter_by = today - offset_time
    # Tableau Time Filters require this format: YYYY-MM-DDTHH:MM:SSZ
    filter_time_string = time_to_filter_by.isoformat('T')[:19] + 'Z'

    last_login_filter = t.url_filters.get_last_login_filter(operator="gte", last_login_time=filter_time_string)

    filtered_users = t.query_users(site_role_filter=explorer_filter, last_login_filter=last_login_filter,
                                         sorts=[t.sorts.Ascending("name"), ])

    print('Finished User tests')


def workbooks_tests(t: TableauRestApiConnection):
    print("Starting Workbook tests")

    default_project = t.query_project('Default')
    wbs_on_site = t.query_workbooks()
    wbs_in_project = t.query_workbooks_in_project(project_name_or_luid=default_project.luid)
    for wb in wbs_in_project:
        wb_luid_from_obj = wb.get('id')
        wb_luid_lookup = t.query_workbook_luid(wb_name=wb.get('name'), proj_name_or_luid=default_project.luid)
        vws_in_wb = t.query_workbook_views(wb_name_or_luid=wb_luid_from_obj)
        vws_in_wb_json = t.query_workbook_views_json(wb_name_or_luid=wb_luid_from_obj)
        t.query_workbook_connections(wb_name_or_luid=wb_luid_from_obj)
        # Published Workbook Options
        wb_obj = t.get_published_workbook_object(workbook_name_or_luid=wb_luid_from_obj)
        permissions_obj_list = wb_obj.get_permissions_obj_list()
        break

    vws_on_site = t.query_views()
    vws_json = t.query_views_json()
    # .workbooks.query_view(vw_name_or_luid='GreatView')


def datasources_tests(t: TableauRestApiConnection):
    print("Starting Datasources tests")
    dses_xml = t.query_datasources(sorts=[Sort.Ascending('name'), ])
    dses_json = t.query_datasources_json()
    for ds in dses_xml:
        ds_luid = ds.get('id')
        ds_content_url = t.query_datasource_content_url(datasource_name_or_luid=ds_luid)
        pub_ds_obj = t.get_published_datasource_object(datasource_name_or_luid=ds_luid)
        permissions_obj_list = pub_ds_obj.get_permissions_obj_list()
        break

    print('Finished Datasource Tests')

def favorites_tests(t: TableauRestApiConnection):
    print("Starting Favorites tests")
    user_favorites_xml = t.query_user_favorites(username_or_luid=t.user_luid)
    user_favorites_json = t.query_user_favorites_json(username_or_luid=t.user_luid)
    print("Finished Favorites tests")

def subscription_tests(t: TableauRestApiConnection):
    print('Starting Subscription tests')

    all_subscriptions = t.query_subscriptions()
    sub_dict = t.xml_list_to_dict(all_subscriptions)

    # Subscriptions for one user
    users = t.query_users()
    for user in users:
        user_luid = user.get('id')
        one_user_subscriptions = t.query_subscriptions(username_or_luid=user_luid)
        break

    print('Finished subscription tests')

def schedules_tests(t: TableauRestApiConnection):
    print("Starting Schedules tests")
    schedules = t.query_schedules()
    schedules_json = t.query_schedules_json()

    sub_schedules = t.query_subscription_schedules()

    print("Finished Schedules tests")

def revision_tests(t: TableauRestApiConnection):
    print('Starting revision tests')
    wbs = t.query_workbooks()
    for wb in wbs:
        revisions = t.get_workbook_revisions(workbook_name_or_luid=wb.get('id'))
        log_obj.log('There are {} revisions of workbook {}'.format(len(revisions), wb.get('name')))
        log_obj.log_xml_response(revisions)
        break
    dses = t.query_datasources()
    for ds in dses:
        revisions = t.get_datasource_revisions(datasource_name_or_luid=ds.get('id'))
        log_obj.log('There are {} revisions of datasource {}'.format(len(revisions), ds.get('name')))
        log_obj.log_xml_response(revisions)
        break

    print('Finished revision tests')


def extract_tests(t: TableauRestApiConnection):
    print('Starting Extract tests')
    response = t.get_extract_refresh_tasks()
    log_obj.log_xml_response(response)
    for tasks in response:
        for task in tasks:
            # This is obviously unnecessary but it's just to test the functioning of the method
            try:
                t.get_extract_refresh_task(task_luid=task.get('id'))
            except NoMatchFoundException as e:
                log_obj.log(e.msg)
            break

    refresh_schedules = t.query_extract_schedules()
    for sched in refresh_schedules:
        try:
            refresh_tasks = t.get_extract_refresh_tasks_on_schedule(schedule_name_or_luid=sched.get('id'))
        except NoMatchFoundException as e:
            log_obj.log(e.msg)
        break

    # Only possible in 2019.3+, also risky to run as tests, so commented out
    # t.extracts.encrypt_extracts()
    # t.extracts.decrypt_extracts()
    # t.extracts.reencrypt_extracts()

    # See the Add / Update / Delete test suite for the commands to run the extract refreshes or encrypt them

    print('Finished Extract Refresh tests')

def alerts_tests(t: TableauRestApiConnection32):
    print("Starting Alerts tests")
    alerts = t.query_data_driven_alerts()
    for alert in alerts:
        t.query_data_driven_alert_details(data_alert_luid=alert.get('id'))
        break
    vws = t.query_views()
    for vw in vws:
        view_alerts = t.query_data_driven_alerts_for_view(view_luid=vw.get('id'))
        break
    print("Finished Alerts tests")

def flow_tests(t: TableauRestApiConnection33):
    print("Starting Flows tests")
    response = t.get_flow_run_tasks()
    for flow_tasks in response:
        for flow_task in flow_tasks:
            t.get_flow_run_task(task_luid=flow_task.get('id'))
    print("Finished Flows tests")

def metadata_tests(t: TableauRestApiConnection35):
    print("Starting Metadata tests")
    dbs = t.query_databases()
    for db in dbs:
        t.query_database(database_name_or_luid=db.get('id'))

        break
    tables = t.query_tables()
    for table in tables:
        t.query_table(table_name_or_luid=table.get('id'))
        t.query_columns_in_a_table(table_name_or_luid=table.get('id'))
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
        graphql_response = t.graphql(graphql_query=my_query)
        log_obj.log(json.dumps(graphql_response))
    except InvalidOptionException as e:
        log_obj.log(e.msg)
    print("Finished Metadata tests")

for server in servers:
    print("Logging in to {}".format(servers[server]['server']))
    run_tests(servers[server]['server'], servers[server]['username'], servers[server]['password'])