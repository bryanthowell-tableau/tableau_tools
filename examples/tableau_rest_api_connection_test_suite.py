# -*- coding: utf-8 -*-

from ...tableau_tools.tableau_rest_api import *
from ...tableau_tools import *
import time
# This is meant to test all relevant functionality of the tableau_tools library.
# It does a lot of things that you wouldn't necessarily do just to make sure they work

# Other than printing messages before and afte each test, logging of what actually happens going into the log file
# rather than to the console.

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
           # u"9.0": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u"9.1": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u"9.2": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u"9.3": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u'10.0': {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u'10.1': {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           # u"10.2": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           #u"10.3": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           #u"10.4": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           #u"10.5": {u"server": u"http://127.0.0.1", u"username": u"", u"password": u""},
           "10.5 Linux": {"server": "http://127.0.0.1", "username": "", "password": ""}
           }


# Configure which tests you want to run in here
def run_tests(server_url: str, username: str, password: str, site_content_url: str = 'default'):
    # There are used to ensure that Unicode is being handled correctly. They are random but hit a lot of character sets
    words = ['ASCII', 'Οὐχὶ ταὐτὰ', 'γιγνώσκειν', 'რეგისტრაცია', 'Международную', 'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา',
             'አይተዳደርም።', '晚飯', '晩ご飯', '저녁밥', 'bữa ăn tối', 'Señor']

    log_obj = Logger('tableau_rest_api_connection_test.log')

    # Test Files to Publish
    twbx_filename = 'test_workbook_excel.twbx'  # Replace with your own test file
    twbx_content_name = 'Test TWBX workbook'  # Replace with your own name

    twb_filename = 'twb_workbook.twb'  # Replace with your own file
    twb_content_name = 'Test Live Connection Workbook'

    tdsx_filename = 'test_datasource.tdsx'  # Replace with your own test file
    tdsx_content_name = 'Test TDSX Datasource'  # Use your own name

    tds_filename = 'test_live_datasource.tds'  # Replace with your test file
    tds_content_name = 'Test TDS Live Data Source'
    # End Test Files

    # Create a default connection
    default = TableauRestApiConnection(server=server_url, username=username, password=password,
                                       site_content_url=site_content_url)
    default.signin()
    default.enable_logging(log_obj)

    # Step 1: Creating a test site
    test_site = create_test_site(default, server_url, username, password, log_obj)

    # Step 2: Project tests
    project_tests(test_site, words)

    # Step 3: Group tests
    group_tests(test_site, words)

    # Step 4: Project Permissions tests
    project_permissions_tests(test_site)

    # Step 5: User Tests
    user_tests(test_site, words)

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


def create_test_site(tableau_server_default_connection: TableauRestApiConnection, server_url: str, username: str,
                     password: str, logger: Logger) -> TableauRestApiConnection:
    print("Creating a test site")
    logger.log('Creating test site')
    default_site = tableau_server_default_connection
    # Assign this however you'd like
    new_site_content_url = 'tableau_tools'
    new_site_name = 'Test Site 1'
    new_site_name_to_change_to = 'Test Site - tableau_tools'

    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        logger.log('Received content_url to delete {}'.format(new_site_content_url))
        test_site = TableauRestApiConnection(server_url, username, password, new_site_content_url)
        test_site.signin()
        test_site.enable_logging(logger)
        logger.log('Signed in successfully to {}'.format(new_site_content_url))

        site_xml = test_site.query_current_site()
        logger.log('Attempting to delete current site')
        test_site.delete_current_site()
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
    test_site = TableauRestApiConnection(server=server_url, username=username,
                                         password=password, site_content_url=new_site_content_url)
    test_site.signin()
    test_site.enable_logging(logger)
    logger.log('Signed in successfully to ' + new_site_content_url)

    # Update the site name
    logger.log('Updating site name')
    test_site.update_site(site_name=new_site_name_to_change_to)

    logger.log('Updating everything about site')
    if isinstance(test_site, TableauRestApiConnection):
        # If e-mail subscriptions are disabled for the Server, this comes back with an error
        #test_site.update_site(content_url=new_site_content_url, admin_mode=u'ContentAndUsers', user_quota=u'30',
        #                      storage_quota=u'400', disable_subscriptions=False, state=u'Active',
        #                      revision_history_enabled=True, revision_limit=u'15')
        test_site.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers', user_quota='30',
                              storage_quota='400', state='Active',
                              revision_history_enabled=True, revision_limit='15')
    else:
        test_site.update_site(content_url=new_site_content_url, admin_mode='ContentAndUsers', user_quota='30',
                              storage_quota='400', disable_subscriptions=False, state='Active')

    logger.log("Getting all site_content_urls on the server")
    all_site_content_urls = test_site.query_all_site_content_urls()

    logger.log(str(all_site_content_urls))
    test_site.query_sites()

    print('Finished creating new site')
    return test_site


def project_tests(t: TableauRestApiConnection, project_names: List[str]):

    print('Testing project methods')
    for project in project_names:
        t.log("Creating Project {}".format(project))
        t.create_project(project, project_desc='I am a not a folder, I am project', no_return=True)

    # Sleep ensures we don't get ahead of the REST API updating with the new projects
    time.sleep(4)
    t.log('Updating first project')
    t.update_project(name_or_luid=project_names[0], new_project_name='Updated {}'.format(project_names[0]),
                     new_project_description='This is only for important people')
    t.log("Deleting second and third projects")
    t.delete_projects([project_names[1], project_names[2]])
    print("Finished testing project methods")


def group_tests(t: TableauRestApiConnection, group_names: List[str]) -> Dict:
    print("Starting group tests")

    for group in group_names:
        t.log("Creating Group {}".format(group))
        new_group_luid = t.create_group(group)

    # Let all of the groups settle in
    time.sleep(3)
    t.log('Updating first group name')
    t.update_group(group_names[0], '{} (updated)'.format(group_names[0]))

    # Delete Groups not introduced until API 2.1
    if isinstance(t, TableauRestApiConnection):
        t.log('Deleting fourth group')
        t.delete_groups(group_names[3])

    t.log("Querying all the groups")
    groups_on_site = t.query_groups()

    # Convert the list to a dict {name : luid}
    groups_dict = t.convert_xml_list_to_name_id_dict(groups_on_site)
    t.log(str(groups_dict))
    print('Finished group tests')
    time.sleep(3)  # Let everything update
    return groups_dict


def project_permissions_tests(t:TableauRestApiConnection):
    print("Starting Permissions tests")
    projects = t.query_projects()
    projects_dict = t.convert_xml_list_to_name_id_dict(projects)
    project_names = list(projects_dict.keys())

    groups = t.query_groups()
    groups_dict = t.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    # Set permissions for one project
    t.log('Querying project called {}'.format(project_names[0]))
    proj_1 = t.query_project(projects_dict[project_names[0]])
    t.log('Setting project to locked permissions')
    proj_1.lock_permissions()
    t.log('Clearing all existing permissions on first project')
    proj_1.clear_all_permissions()

    proj_perms_list = []
    for group in groups_dict:
        proj_perms = proj_1.create_project_permissions_object_for_group(groups_dict[group], 'Viewer')
        proj_perms_list.append(proj_perms)

    proj_1.set_permissions_by_permissions_obj_list(proj_perms_list)

    # WB defaults
    wb_perms_list = []
    for group in groups_dict:
        wb_perms = proj_1.create_workbook_permissions_object_for_group(groups_dict[group], 'Interactor')
        wb_perms_list.append(wb_perms)
    t.log('Setting workbook permissions')
    proj_1.workbook_defaults.set_permissions_by_permissions_obj_list(wb_perms_list)

    # DS defaults
    ds_perms_list = []
    for group in groups_dict:
        ds_perms = proj_1.create_datasource_permissions_object_for_group(groups_dict[group], 'Editor')
        ds_perms_list.append(ds_perms)
    t.log('Setting datasource permissions')
    proj_1.datasource_defaults.set_permissions_by_permissions_obj_list(ds_perms_list)

    # Second Project
    t.log('Querying project called {}'.format(project_names[5]))
    proj_2 = t.query_project(projects_dict[project_names[5]])
    t.log('Unlocking permissions')
    proj_2.unlock_permissions()
    proj_2.clear_all_permissions(clear_defaults=False)  # Don't clear workbook or datasource defaults

    proj_perms = proj_2.create_project_permissions_object_for_group(groups_dict[group_names[6]])
    proj_perms.set_all_to_allow()
    proj_perms.set_capability_to_unspecified('Save')
    t.log('Setting project permissions for group {}'.format(group_names[6]))
    proj_2.set_permissions_by_permissions_obj_list([proj_perms, ])

    # Clone Permissions from one to another
    t.log('Cloning project permissions from {} to {}'.format(project_names[3], project_names[0]))
    proj_3 = t.query_project(projects_dict[project_names[3]])
    proj_3.replicate_permissions(proj_1)

    print('Finished Permissions tests')


def user_tests(t: TableauRestApiConnection, names: List[str]):
    print('Starting User tests')
    # Create some fake users to assign to groups

    new_user_luids = []
    for name in names:
        username = name
        full_name = name.upper()
        t.log("Creating User '{}' named '{}'".format(username, full_name))
        try:
            new_user_luid = t.add_user(username, full_name, 'Interactor', 'password', username + '@nowhere.com')
        except InvalidOptionException as e:
            print((e.msg))
            raise
        new_user_luids.append(new_user_luid)

    # This takes Users x Groups amount of time to complete, can really stretch out the test
    groups = t.query_groups()
    groups_dict = t.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    # Add all users to first group
    t.log("Adding users to group {}".format(group_names[0]))
    t.add_users_to_group(new_user_luids, groups_dict[group_names[0]])

    # Add first three users to second gruop

    t.log("Adding users to group {}".format(group_names[1]))
    t.add_users_to_group([new_user_luids[0], new_user_luids[1], new_user_luids[3]], group_names[1])

    # Remove sixth user from first gruop
    t.log('Removing user {} from group {}'.format(new_user_luids[5], group_names[0]))
    t.remove_users_from_group(new_user_luids[5], groups_dict[group_names[0]])

    t.log('Unlicensing the second user')
    t.update_user(new_user_luids[1], site_role='Unlicensed')

    t.log('Updating second user')
    t.update_user(new_user_luids[1], full_name='Updated User', password='h@ckm3', email='me@gmail.com')

    t.log('Removing the third user')
    t.remove_users_from_site(new_user_luids[2])

    # Sleep to let updates happen
    time.sleep(4)
    users = t.query_users()
    users_dict = t.convert_xml_list_to_name_id_dict(users)
    t.log(str(list(users_dict.keys())))

    if isinstance(t, TableauRestApiConnection):
        name_sort = Sort('name', 'desc')
        if isinstance(t, TableauRestApiConnection28):
            role_f = UrlFilter28.create_site_roles_filter(['Interactor', 'Publisher'])
        else:
            role_f = UrlFilter.create_site_role_filter('Interactor')
        ll_f = UrlFilter.create_last_login_filter('gte', '2018-01-01T00:00:00:00Z')

        users = t.query_users(sorts=[name_sort, ], site_role_filter=role_f, last_login_filter=ll_f)
        t.log('Here are sorted and filtered users')
        for user in users:
            t.log(user.get('name'))

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
