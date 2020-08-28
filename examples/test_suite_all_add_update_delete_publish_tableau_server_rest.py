# -*- coding: utf-8 -*-
from typing import Optional, List, Dict
import time, datetime
import json

from tableau_tools import *

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

log_obj = Logger('tableau_rest_api_connection_other_tests.log')
rest_request_log_obj = Logger('tableau_rest_api_other_querying_tests_rest.log')

# Set of Words in all sorts of characters to make sure everything works right
words = ['ASCII', 'Οὐχὶ ταὐτὰ', 'γιγνώσκειν', 'რეგისტრაცია', 'Международную', 'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา',
         'አይተዳደርም።', '晚飯', '晩ご飯', '저녁밥', 'bữa ăn tối', 'Señor']

tdsx_to_publish = 'Basic Superstore.tdsx'
twbx_to_publish = 'Test Workbook - 10_3.twbx'
tdsx_with_extract_to_publish = ''
twbx_with_extract_to_publish = ''

# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):
    # Create a default connection, test pulling server information
    d = TableauServerRest35(server=server_url, username=username, password=password,
                            site_content_url=site_content_url)
    d.signin()
    d.enable_logging(rest_request_log_obj)

    t = create_test_site(tableau_server_default_connection=d, server_url=server_url, username=username,
                         password=password, logger=rest_request_log_obj)
    d.signout()
    project_tests(t)
    # Create users first
    user_tests(t)
    # Then drop them into groups
    group_tests(t)

    workbooks_tests(t)
    datasources_tests(t)

    favorites_tests(t)

    # Gotta Create Schedules first
    schedules_tests(t)
    # then can subscribe. Make sure the server is configured for subscriptions
    subscription_tests(t)

    revision_tests(t)
    # extract_tests(t)

    # flow_tests(t) # 2019.2+
    # metadata_tests(t)  # 2019.3+ only
    # webhook_test(t)  # 2019.4+ only

    # You need to do the alerts tests after the creation of the site, and then make an alert or two, for them to do anything

    # alerts_tests(t) # 2018.3+
    t.signout()


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
        test_site = TableauServerRest35(server_url, username, password, new_site_content_url)
        test_site.signin()
        test_site.enable_logging(logger)
        logger.log('Signed in successfully to {}'.format(new_site_content_url))

        site_xml = test_site.sites.query_current_site()
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
        new_site_id = default_site.sites.create_site(new_site_name, new_site_content_url)
        logger.log('Created new site ' + new_site_id)
    # This shouldn't happen if the existing check and delete happened earlier, but might as well protect
    except AlreadyExistsException as e:
        print((e.msg))
        print("Cannot create new site due to error, exiting")
        exit()

    # Once we've created the site, we need to sign into it to do anything else
    test_site = TableauServerRest35(server=server_url, username=username,
                                    password=password, site_content_url=new_site_content_url)
    test_site.signin()
    test_site.enable_logging(logger)
    logger.log('Signed in successfully to ' + new_site_content_url)

    # Update the site name
    logger.log('Updating site name')
    test_site.sites.update_site(site_name=new_site_name_to_change_to)

    logger.log('Updating everything about site')
    if isinstance(test_site, TableauServerRest):
        # If e-mail subscriptions are disabled for the Server, this comes back with an error
        # test_site.update_site(content_url=new_site_content_url, admin_mode=u'ContentAndUsers', user_quota=u'30',
        #                      storage_quota=u'400', disable_subscriptions=False, state=u'Active',
        #                      revision_history_enabled=True, revision_limit=u'15')

        # THis could be different depending on the version / if Named Users
        test_site.sites.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers',
                                    user_quota=100,
                                    storage_quota='400', state='Active',
                                    revision_history_enabled=True, revision_limit='15', disable_subscriptions=False)
    else:
        test_site.sites.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers',
                                    user_quota=100,
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

    all_users_perms = new_proj_obj.get_permissions_obj(group_name_or_luid=all_users_groups_luid)
    all_users_perms.set_capability_to_allow("View")
    new_proj_obj.set_permissions(permissions=[all_users_perms, ])

    all_users_wb_perms = new_proj_obj.workbook_defaults.get_permissions_obj(group_name_or_luid=all_users_groups_luid,
                                                                            role="Interactor")
    new_proj_obj.workbook_defaults.set_permissions(permissions=[all_users_wb_perms, ])

    all_users_ds_perms = new_proj_obj.datasource_defaults.get_permissions_obj(group_name_or_luid="All Users")
    all_users_ds_perms.set_all_to_deny()
    new_proj_obj.datasource_defaults.set_permissions(permissions=[all_users_ds_perms, ])

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
    all_users_dict = t.xml_list_to_dict(all_users)
    all_user_luids = list(all_users_dict.values())  # .values() returns a ValuesView, which needs to be cast to list
    # Just picking one Group at random here basically
    t.groups.add_users_to_group(username_or_luid_s=all_user_luids, group_name_or_luid=group_luids[0])

    # Randomly removing a few users
    t.groups.remove_users_from_group(group_name_or_luid=group_luids[0],
                                     username_or_luid_s=[all_user_luids[0], all_user_luids[2]])

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
        user_luid = t.users.add_user(username=word, fullname=word.upper(), site_role='Interactor',
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
    new_wb_luid_2 = t.workbooks.publish_workbook(workbook_filename=twbx_to_publish,
                                                 workbook_name="{} - 2".format(wb_name),
                                                 project_obj=default_project, overwrite=True)
    time.sleep(3)

    projects = t.projects.query_projects()
    projects_dict = t.xml_list_to_dict(projects)
    projects_list = list(projects_dict.keys())

    log_obj.log('Moving workbook to {} project'.format(projects_list[0]))
    t.workbooks.update_workbook(workbook_name_or_luid=new_wb_luid,
                                new_project_name_or_luid=projects_dict[projects_list[0]], show_tabs=True)

    log_obj.log("Querying workbook as Published Object")
    wb_obj = t.workbooks.get_published_workbook_object(workbook_name_or_luid=new_wb_luid)

    # Permissions type stuff on it
    wb_permissions = wb_obj.get_permissions_obj_list()
    wb_obj.clear_all_permissions()
    all_users_perms = wb_obj.get_permissions_obj(group_name_or_luid="All Users")
    all_users_perms.set_all_to_allow()
    all_users_perms.set_capability_to_deny(capability_name='Save')

    groups = t.groups.query_groups()
    groups_dict = t.xml_list_to_dict(groups)
    group_perms = wb_obj.get_permissions_obj(group_name_or_luid=list(groups_dict.keys())[1])
    group_perms.set_all_to_deny()

    wb_obj.set_permissions(permissions=[all_users_perms, group_perms])

    wb_views = t.workbooks.query_workbook_views(new_wb_luid)
    wb_views_dict = t.xml_list_to_dict(wb_views)

    t.log(str(wb_views_dict))

    t.log('Adding tags to workbook')
    t.workbooks.add_tags_to_workbook(wb_name_or_luid=new_wb_luid, tag_s=['workbooks', 'flights', 'cool', '晚飯'])

    t.log('Deleting a tag from workbook')
    t.workbooks.delete_tags_from_workbook(new_wb_luid, 'flights')

    t.log('Deleting workbook')
    t.workbooks.delete_workbooks(new_wb_luid_2)

    print("Finished Workbook tests")


def datasources_tests(t: TableauServerRest):
    print("Starting Datasources tests")
    ds_name = 'My Datasource'
    default_project = t.projects.query_project('Default')
    # log_obj.log('Publishing workbook as {}'.format(twbx_to_publish)
    new_ds_luid = t.datasources.publish_datasource(ds_filename=tdsx_to_publish, ds_name=ds_name,
                                                   project_obj=default_project, overwrite=True)

    # Repeat Multiple times to creates some revisions
    time.sleep(3)
    new_ds_luid = t.datasources.publish_datasource(ds_filename=tdsx_to_publish, ds_name=ds_name,
                                                   project_obj=default_project, overwrite=True)

    time.sleep(3)
    new_ds_luid = t.datasources.publish_datasource(ds_filename=tdsx_to_publish, ds_name=ds_name,
                                                   project_obj=default_project, overwrite=True)

    time.sleep(3)

    # Publish second one to be deleted
    new_ds_luid_2 = t.datasources.publish_datasource(ds_filename=tdsx_to_publish, ds_name="{} - 2".format(ds_name),
                                                     project_obj=default_project, overwrite=True)
    time.sleep(3)

    projects = t.projects.query_projects()
    projects_dict = t.xml_list_to_dict(projects)
    projects_list = list(projects_dict.keys())

    log_obj.log('Moving datasource to {} project'.format(projects_list[0]))
    t.datasources.update_datasource(datasource_name_or_luid=new_ds_luid,
                                    new_project_name_or_luid=projects_dict[projects_list[0]])

    log_obj.log("Querying workbook as Published Object")
    ds_obj = t.datasources.get_published_datasource_object(datasource_name_or_luid=new_ds_luid)

    # Permissions type stuff on it
    ds_permissions = ds_obj.get_permissions_obj_list()
    ds_obj.clear_all_permissions()
    all_users_perms = ds_obj.get_permissions_obj(group_name_or_luid="All Users")
    all_users_perms.set_all_to_allow()
    all_users_perms.set_capability_to_deny(capability_name='Connect')

    groups = t.groups.query_groups()
    groups_dict = t.xml_list_to_dict(groups)
    group_perms = ds_obj.get_permissions_obj(group_name_or_luid=list(groups_dict.keys())[1])
    group_perms.set_all_to_deny()

    ds_obj.set_permissions(permissions=[all_users_perms, group_perms])

    t.log('Adding tags to datasource')
    t.datasources.add_tags_to_datasource(ds_name_or_luid=new_ds_luid, tag_s=['dses', 'carrots', 'thangs', '晚飯'])

    t.log('Deleting a tag from workbook')
    t.datasources.delete_tags_from_datasource(new_ds_luid, 'carrots')

    t.log('Deleting workbook')
    t.datasources.delete_datasources(new_ds_luid_2)

    print('Finished Datasource Tests')


def favorites_tests(t: TableauServerRest31):
    print("Starting Favorites tests")

    # Favorites don't really have LUIDs, although they do have distinct names
    # You can't "double add a favorite, even with different names
    # Favorite Name doesn't even seem to appear in the UI in 2019.3, so it may not even be necessary anymore

    # Add a favorite for Workbook
    wbs = t.workbooks.query_workbooks()
    wbs_dict = t.xml_list_to_dict(wbs)
    wb_luid = list(wbs_dict.values())[0]
    t.favorites.add_workbook_to_user_favorites(favorite_name="My Favorite Workbook 1", username_or_luid=t.user_luid,
                                               wb_name_or_luid=wb_luid)

    # Test deleting. Note you delete on the actual content LUID, not a "favorite luid"
    t.favorites.delete_workbooks_from_user_favorites(wb_name_or_luid_s=wb_luid, username_or_luid=t.user_luid)

    # Add back again so you can see it
    wb_fav = t.favorites.add_workbook_to_user_favorites(favorite_name="My Favorite Workbook 1",
                                                        username_or_luid=t.user_luid,
                                                        wb_name_or_luid=wb_luid)

    # A View to favorites
    wb_views = t.workbooks.query_workbook_views(wb_luid)
    wb_views_dict = t.xml_list_to_dict(wb_views)
    t.favorites.add_view_to_user_favorites(favorite_name="A favored view!", username_or_luid=t.user_luid,
                                           view_name_or_luid=list(wb_views_dict.values())[0])
    view_fav = t.favorites.add_view_to_user_favorites(favorite_name="A lesser view", username_or_luid=t.user_luid,
                                                      view_name_or_luid=list(wb_views_dict.values())[1])
    t.favorites.delete_views_from_user_favorites(view_name_or_luid_s=list(wb_views_dict.values())[1],
                                                 username_or_luid=t.user_luid)

    # Datasource Favorite
    dses = t.datasources.query_datasources()
    dses_dict = t.xml_list_to_dict(dses)
    ds_luid = list(dses_dict.values())[0]
    t.favorites.add_datasource_to_user_favorites(favorite_name="My Favorite DS 1", username_or_luid=t.user_luid,
                                                 ds_name_or_luid=ds_luid)
    # Delete it
    t.favorites.delete_datasources_from_user_favorites(ds_name_or_luid_s=ds_luid, username_or_luid=t.user_luid)
    # Add it back
    ds_fav = t.favorites.add_datasource_to_user_favorites(favorite_name="My Favorite DS 1",
                                                          username_or_luid=t.user_luid,
                                                          ds_name_or_luid=ds_luid)

    # API 3.1 and later you can favorite a Project
    projects = t.projects.query_projects()
    p_dict = t.xml_list_to_dict(projects)
    p_luids = list(p_dict.values())
    t.favorites.add_project_to_user_favorites(favorite_name="My favorite project 1", username_or_luid=t.user_luid,
                                              proj_name_or_luid=p_luids[0])
    # Delete it
    t.favorites.delete_projects_from_user_favorites(proj_name_or_luid_s=p_luids[0], username_or_luid=t.user_luid)
    # Add A New One
    p_fav = t.favorites.add_project_to_user_favorites(favorite_name="My favorite project 1",
                                                      username_or_luid=t.user_luid, proj_name_or_luid=p_luids[1])

    # Current Status after adding
    current_favorites = t.favorites.query_user_favorites(username_or_luid=t.user_luid)
    current_favorites_json = t.favorites.query_user_favorites_json(username_or_luid=t.user_luid)
    print("Finished Favorites tests")


def subscription_tests(t: TableauServerRest):
    print('Starting Subscription tests')

    # All users in a Group
    groups = t.groups.query_groups()
    groups_dict = t.xml_list_to_dict(groups)
    group_names = list(groups_dict.keys())

    users_in_group = t.groups.query_users_in_group(group_name_or_luid=groups_dict[group_names[0]])
    users_dict = t.xml_list_to_dict(users_in_group)
    usernames = list(users_dict.keys())

    wbs = t.workbooks.query_workbooks()
    wbs_dict = t.xml_list_to_dict(wbs)
    wb_names = list(wbs_dict.keys())

    # Grab first workbook
    wb_luid = wbs_dict[wb_names[0]]

    sub_schedules = t.schedules.query_subscription_schedules()
    sched_dict = t.xml_list_to_dict(sub_schedules)
    sched_names = list(sched_dict.keys())

    # Grab first schedule
    sched_luid = sched_dict[sched_names[0]]

    # Subscribe them to the first workbook
    t.log('Adding subscription with subject Important weekly update to first workbook for all users in group 1')
    for user in users_dict:
        try:
            t.subscriptions.create_subscription_to_workbook(subscription_subject='Important weekly update',
                                                            wb_name_or_luid=wb_luid, schedule_name_or_luid=sched_luid,
                                                            username_or_luid=users_dict[user])
        # If subscriptions are not enabled on the site or server, then it should throw this exception. Break out of tests to continue
        except InvalidOptionException as e:
            t.log(e.msg)
            return False

    # Find the subscriptions for user 1, delete
    user_1_subs = t.subscriptions.query_subscriptions(username_or_luid=usernames[0])
    t.log('Deleting all subscriptions for user 1')
    sub_luids = []
    for sub in user_1_subs:
        sub_luids.append(sub.get('id'))
    # Multiple delete (could also have deleted in the loop above)
    t.subscriptions.delete_subscriptions(subscription_luid_s=sub_luids)

    # Update user 2 subscriptions
    t.log('Updating user 2s subscriptions to second schedule')
    user_2_subs = t.subscriptions.query_subscriptions(username_or_luid=usernames[1])
    for sub in user_2_subs:
        luid = sub.get('id')
        t.subscriptions.update_subscription(subscription_luid=luid, schedule_luid=sched_dict[sched_names[1]])

    print('Finished subscription tests')


def schedules_tests(t: TableauServerRest):
    print("Starting Schedules tests")
    # The amount of Exception handling here is important because Schedules are Server-wide, so there is a much
    # greater chance of conflict (and it is unsafe for this script to clear existing schedules that is did not create itself)
    try:
        t.log('Creating a daily extract schedule')
        t.schedules.create_daily_extract_schedule(name='Afternoon Delight', start_time='13:00:00')
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    try:
        t.log('Creating a monthly subscription schedule')
        new_monthly_luid = t.schedules.create_monthly_subscription_schedule(name='First of the Month', day_of_month='1',
                                                                            start_time='03:00:00',
                                                                            parallel_or_serial='Serial')
        t.log('Deleting monthly subscription schedule LUID {}'.format(new_monthly_luid))
        time.sleep(4)
        t.schedules.delete_schedule(schedule_name_or_luid=new_monthly_luid)
    except AlreadyExistsException as e:
        t.log('Skipping the add and delete since it already exists')

    try:
        t.log('Creating a monthly extract schedule')
        t.schedules.create_monthly_extract_schedule(name='Last Day of Month', day_of_month='LastDay',
                                                    start_time='03:00:00', priority=25)
    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    try:
        t.log('Creating a weekly extract schedule')
        weekly_luid = t.schedules.create_weekly_subscription_schedule(name='Mon Wed Fri',
                                                                      weekday_s=['Monday', 'Wednesday', 'Friday'],
                                                                      start_time='05:00:00')
        time.sleep(4)

        try:
            t.log('Updating schedule with LUID {}'.format(weekly_luid))
            t.schedules.update_schedule(schedule_name_or_luid=weekly_luid, new_name='Wed Fri',
                                        interval_value_s=['Wednesday', 'Friday'])
        except AlreadyExistsException as e:
            t.log('Skipping the Update since a schedule with the same name already exists')
            # You could also write logic here to find that existing schedule, delete it, then move this one over

    except AlreadyExistsException as e:
        t.log('Skipping the add since it already exists')

    print("Finished Schedules tests")


def revision_tests(t: TableauServerRest):
    # Revisions start at 1 and number upward.
    # They are never deleted, just flagged with "isDeleted=true"
    # The last one in the list has an "isCurrent=true" flag
    # To make something else Current, you have to either Delete ones ahead of it, or download from one revision
    # and republish it using the regular Publish method, so that it is would be the most recent revision

    print('Starting revision tests')
    wbs = t.workbooks.query_workbooks()
    for wb in wbs:
        wb_luid = wb.get('id')
        # You need all this to replace a workbook completely -- everything must match
        wb_name = wb.get('name')
        for elem in wb:
            if elem.tag.find('project') != -1:
                proj_luid = elem.get('id')

        proj_obj = t.projects.query_project(project_name_or_luid=proj_luid)
        revisions = t.revisions.get_workbook_revisions(workbook_name_or_luid=wb_luid)
        revision_count = len(revisions)
        log_obj.log('There are {} revisions of workbook {}'.format(len(revisions), wb.get('name')))
        log_obj.log_xml_response(revisions)
        wb_first_rev_filename = t.revisions.download_workbook_revision(wb_name_or_luid=wb_luid, revision_number=1,
                                                                       filename_no_extension='First Workbook Revision')
        t.revisions.remove_workbook_revision(wb_name_or_luid=wb_luid, revision_number=revision_count-1)
        # To set a revision to be active via republishing
        # Worth testing: Does the overwrite screw up any saved credentials, or do they stay save from previous publish?
        t.workbooks.publish_workbook(workbook_filename=wb_first_rev_filename, workbook_name=wb_name, project_obj=proj_obj,
                                     overwrite=True)

        break
    dses = t.datasources.query_datasources()
    for ds in dses:
        revisions = t.revisions.get_datasource_revisions(datasource_name_or_luid=ds.get('id'))
        log_obj.log('There are {} revisions of datasource {}'.format(len(revisions), ds.get('name')))
        log_obj.log_xml_response(revisions)

        ds_luid = ds.get('id')
        # You need all this to replace a workbook completely -- everything must match
        ds_name = ds.get('name')
        for elem in ds:
            if elem.tag.find('project') != -1:
                proj_luid = elem.get('id')

        proj_obj = t.projects.query_project(project_name_or_luid=proj_luid)
        revisions = t.revisions.get_datasource_revisions(datasource_name_or_luid=ds_luid)
        revision_count = len(revisions)
        log_obj.log('There are {} revisions of datasource {}'.format(len(revisions), ds.get('name')))
        log_obj.log_xml_response(revisions)
        ds_first_rev_filename = t.revisions.download_datasource_revision(ds_name_or_luid=ds_luid, revision_number=1,
                                                                       filename_no_extension='First Datasource Revision')
        t.revisions.remove_datasource_revision(datasource_name_or_luid=ds_luid, revision_number=revision_count - 1)
        # To set a revision to be active via republishing
        # Worth testing: Does the overwrite screw up any saved credentials, or do they stay save from previous publish?
        t.datasources.publish_datasource(ds_filename=ds_first_rev_filename, ds_name=ds_name, project_obj=proj_obj,
                                         overwrite=True)
        break

    print('Finished revision tests')


def extract_tests(t: TableauServerRest):
    print('Starting Extract tests')
    # Only possible in 2019.3+, also risky to run as tests, so commented out
    t.extracts.encrypt_extracts()
    t.extracts.decrypt_extracts()
    t.extracts.reencrypt_extracts()

    # Grab Extract workbook
    t.extracts.run_extract_refresh_for_workbook(wb_name_or_luid=extract_workbook_publish_name)

    t.extracts.run_extract_refresh_for_datasource(ds_name_or_luid=extract_datasource_publish_name)

    extract_scheds = t.schedules.query_extract_schedules()
    extract_scheds_dict = t.xml_list_to_dict(extract_scheds)
    extract_luids = list(extract_scheds_dict.values())
    # Run the first one if it exists
    if len(extract_luids) > 0:
        t.extracts.run_all_extract_refreshes_for_schedule(schedule_name_or_luid=extract_luids[0])

    extract_refresh_tasks = t.extracts.get_extract_refresh_tasks()
    for task in extract_refresh_tasks:
        t.extracts.run_extract_refresh_task(task_luid=task.get('id'))

    print('Finished Extract Refresh tests')


def alerts_tests(t: TableauServerRest32):
    # Must create at least one Data Driven Alert to be able to do anything with these methods. So needs to be run
    # AFTER initial tests
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
    t.flows.run_flow_task()
    t.flows.run_flow_now()
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