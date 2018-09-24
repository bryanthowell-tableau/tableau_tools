# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
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
def run_tests(server_url, username, password):
    # There are used to ensure that Unicode is being handled correctly. They are random but hit a lot of character sets
    words = ['ASCII', 'Οὐχὶ ταὐτὰ', 'γιγνώσκειν', 'რეგისტრაცია', 'Международную', 'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา',
             'አይተዳደርም።', '晚飯', '晩ご飯', '저녁밥', 'bữa ăn tối', 'Señor']

    log_obj = Logger('tableau_tools_test.log')

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
    default = TableauRestApiConnection26(server_url, username, password, site_content_url='default')
    default.signin()
    default.enable_logging(log_obj)

    # Step 1: Creating a test site
    test_site = create_test_site(default, server_url, username, password, log_obj)

    # Step 2: Project tests
    project_tests(test_site, words)

    # Step 3: Group tests
    group_tests(test_site, words)

    # Step 4: Project Permissions tests
    version = test_site.api_version
    # very few people still using 9.0-9.1, but permissions works the same without the default permissions
    if version != '2.0':
        project_permissions_tests21(test_site)

    # Step 5: User Tests
    user_tests(test_site, words)

    # Step 6: Publishing Workbook Tests
    workbooks_test(test_site, twbx_filename, twbx_content_name)

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


def create_test_site(default_site, server_url, username, password, logger):
    """
    :type default_site: TableauRestApiConnection
    :type server_url: unicode
    :type username: unicode
    :type password: unicode
    :type logger: Logger
    :rtype: TableauRestApiConnection25
    """
    print("Creating a test site")
    logger.log('Creating test site')

    # Assign this however you'd like
    new_site_content_url = 'tableau_tools'
    new_site_name = 'Test Site 1'
    new_site_name_to_change_to = 'Test Site - tableau_tools'

    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        logger.log('Received content_url to delete {}'.format(new_site_content_url))
        test_site = TableauRestApiConnection25(server_url, username, password, new_site_content_url)
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
    test_site = TableauRestApiConnection25(server_url, username, password, site_content_url=new_site_content_url)
    test_site.signin()
    test_site.enable_logging(logger)
    logger.log('Signed in successfully to ' + new_site_content_url)

    # Update the site name
    logger.log('Updating site name')
    test_site.update_site(site_name=new_site_name_to_change_to)

    logger.log('Updating everything about site')
    if isinstance(test_site, TableauRestApiConnection23):
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


def project_tests(t_site, project_names):
    """
    :type t_site: TableauRestApiConnection
    :type project_names: list[unicode]
    """
    print('Testing project methods')
    for project in project_names:
        t_site.log("Creating Project {}".format(project).encode('utf8'))
        t_site.create_project(project, project_desc='I am a not a folder, I am project', no_return=True)

    # Sleep ensures we don't get ahead of the REST API updating with the new projects
    time.sleep(4)
    t_site.log('Updating first project')
    t_site.update_project(project_names[0], new_project_name='Updated {}'.format(project_names[0]),
                          new_project_description='This is only for important people')
    t_site.log("Deleting second and third projects")
    t_site.delete_projects([project_names[1], project_names[2]])
    print("Finished testing project methods")


def group_tests(t_site, group_names):
    """
    :type t_site: TableauRestApiConnection21
    :param group_names:
    :return:
    """
    print("Starting group tests")

    for group in group_names:
        t_site.log("Creating Group {}".format(group))
        new_group_luid = t_site.create_group(group)

    # Let all of the groups settle in
    time.sleep(3)
    t_site.log('Updating first group name')
    t_site.update_group(group_names[0], '{} (updated)'.format(group_names[0]))

    # Delete Groups not introduced until API 2.1
    if isinstance(t_site, TableauRestApiConnection21):
        t_site.log('Deleting fourth group')
        t_site.delete_groups(group_names[3])

    t_site.log("Querying all the groups")
    groups_on_site = t_site.query_groups()

    # Convert the list to a dict {name : luid}
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups_on_site)
    t_site.log(str(groups_dict))
    print('Finished group tests')
    time.sleep(3)  # Let everything update
    return groups_dict


def project_permissions_tests21(t_site):
    """
    :type t_site: TableauRestApiConnection21
    :return:
    """
    print("Starting Permissions tests")
    projects = t_site.query_projects()
    projects_dict = t_site.convert_xml_list_to_name_id_dict(projects)
    project_names = list(projects_dict.keys())

    groups = t_site.query_groups()
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    # Set permissions for one project
    t_site.log('Querying project called {}'.format(project_names[0]))
    proj_1 = t_site.query_project(projects_dict[project_names[0]])
    t_site.log('Setting project to locked permissions')
    proj_1.lock_permissions()
    t_site.log('Clearing all existing permissions on first project')
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
    t_site.log('Setting workbook permissions')
    proj_1.workbook_defaults.set_permissions_by_permissions_obj_list(wb_perms_list)

    # DS defaults
    ds_perms_list = []
    for group in groups_dict:
        ds_perms = proj_1.create_datasource_permissions_object_for_group(groups_dict[group], 'Editor')
        ds_perms_list.append(ds_perms)
    t_site.log('Setting datasource permissions')
    proj_1.datasource_defaults.set_permissions_by_permissions_obj_list(ds_perms_list)

    # Second Project
    t_site.log('Querying project called {}'.format(project_names[5]))
    proj_2 = t_site.query_project(projects_dict[project_names[5]])
    t_site.log('Unlocking permissions')
    proj_2.unlock_permissions()
    proj_2.clear_all_permissions(clear_defaults=False)  # Don't clear workbook or datasource defaults

    proj_perms = proj_2.create_project_permissions_object_for_group(groups_dict[group_names[6]])
    proj_perms.set_all_to_allow()
    proj_perms.set_capability_to_unspecified('Save')
    t_site.log('Setting project permissions for group {}'.format(group_names[6]))
    proj_2.set_permissions_by_permissions_obj_list([proj_perms, ])

    # Clone Permissions from one to another
    t_site.log('Cloning project permissions from {} to {}'.format(project_names[3], project_names[0]))
    proj_3 = t_site.query_project(projects_dict[project_names[3]])
    proj_3.replicate_permissions(proj_1)

    print('Finished Permissions tests')


def user_tests(t_site, names):
    """
    :type t_site: TableauRestApiConnection
    :type names: list[unicode]
    :return:
    """
    print('Starting User tests')
    # Create some fake users to assign to groups

    new_user_luids = []
    for name in names:
        username = name
        full_name = name.upper()
        t_site.log("Creating User '{}' named '{}'".format(username, full_name))
        try:
            new_user_luid = t_site.add_user(username, full_name, 'Interactor', 'password', username + '@nowhere.com')
        except InvalidOptionException as e:
            print((e.msg))
            raise
        new_user_luids.append(new_user_luid)

    # This takes Users x Groups amount of time to complete, can really stretch out the test
    groups = t_site.query_groups()
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    # Add all users to first group
    t_site.log("Adding users to group {}".format(group_names[0]))
    t_site.add_users_to_group(new_user_luids, groups_dict[group_names[0]])

    # Add first three users to second gruop

    t_site.log("Adding users to group {}".format(group_names[1]))
    t_site.add_users_to_group([new_user_luids[0], new_user_luids[1], new_user_luids[3]], group_names[1])

    # Remove sixth user from first gruop
    t_site.log('Removing user {} from group {}'.format(new_user_luids[5], group_names[0]))
    t_site.remove_users_from_group(new_user_luids[5], groups_dict[group_names[0]])

    t_site.log('Unlicensing the second user')
    t_site.update_user(new_user_luids[1], site_role='Unlicensed')

    t_site.log('Updating second user')
    t_site.update_user(new_user_luids[1], full_name='Updated User', password='h@ckm3', email='me@gmail.com')

    t_site.log('Removing the third user')
    t_site.remove_users_from_site(new_user_luids[2])

    # Sleep to let updates happen
    time.sleep(4)
    users = t_site.query_users()
    users_dict = t_site.convert_xml_list_to_name_id_dict(users)
    t_site.log(str(list(users_dict.keys())))

    if isinstance(t_site, TableauRestApiConnection25):
        name_sort = Sort('name', 'desc')
        if isinstance(t_site, TableauRestApiConnection28):
            role_f = UrlFilter28.create_site_roles_filter(['Interactor', 'Publisher'])
        else:
            role_f = UrlFilter25.create_site_role_filter('Interactor')
        ll_f = UrlFilter25.create_last_login_filter('gte', '2018-01-01T00:00:00:00Z')

        users = t_site.query_users(sorts=[name_sort, ], site_role_filter=role_f, last_login_filter=ll_f)
        t_site.log('Here are sorted and filtered users')
        for user in users:
            t_site.log(user.get('name'))

    print('Finished User tests')


def workbooks_test(t_site, twbx_filename, twbx_content_name, twb_filename=None, twb_content_name=None):
    """
    :type t_site: TableauRestApiConnection
    :type twbx_filename: unicode
    :type twbx_content_name: unicode
    :type twb_filename: unicode
    :type twb_content_name: unicode
    :return:
    """
    print("Starting Workbook tests")

    default_project = t_site.query_project('Default')
    t_site.log('Publishing workbook as {}'.format(twbx_content_name))
    new_wb_luid = t_site.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    # Repeat Multiple times to creates some revisions
    time.sleep(3)
    new_wb_luid = t_site.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    time.sleep(3)
    new_wb_luid = t_site.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    time.sleep(3)

    # Publish second one to be deleted
    new_wb_luid_2 = t_site.publish_workbook(twbx_filename, "{} - 2".format(twbx_content_name),
                                            default_project, overwrite=True)
    time.sleep(3)

    projects = t_site.query_projects()
    projects_dict = t_site.convert_xml_list_to_name_id_dict(projects)
    projects_list = list(projects_dict.keys())

    t_site.log('Moving workbook to {} project'.format(projects_list[0]))
    t_site.update_workbook(new_wb_luid, default_project.luid, new_project_luid=projects_dict[projects_list[0]], show_tabs=True)

    t_site.log("Querying workbook")
    t_site.query_workbook(new_wb_luid)

    # Save workbook preview image
    t_site.log("Saving workbook preview image")
    t_site.save_workbook_preview_image(new_wb_luid, 'Workbook preview')

    t_site.log("Downloading workbook file")
    t_site.download_workbook(new_wb_luid, 'saved workbook')

    t_site.log("Query workbook connections")
    t_site.query_workbook_connections(new_wb_luid)

    t_site.log("Querying workbook views")
    wb_views = t_site.query_workbook_views(new_wb_luid)
    wb_views_dict = t_site.convert_xml_list_to_name_id_dict(wb_views)

    t_site.log(str(wb_views_dict))

    for wb_view in wb_views_dict:
        t_site.log("Adding {} to favorites for me".format(wb_view))
        t_site.add_view_to_user_favorites('Fav - {}'.format(wb_view), t_site.username, wb_view, wb_name_or_luid=new_wb_luid)

    for wb_view in wb_views_dict:
        t_site.log("Deleting {} from favorites for me".format(wb_view))
        t_site.delete_views_from_user_favorites(wb_views_dict.get(wb_view), t_site.username, new_wb_luid)

    t_site.log('Adding tags to workbook')
    t_site.add_tags_to_workbook(new_wb_luid, ['workbooks', 'flights', 'cool', '晚飯'])

    t_site.log('Deleting a tag from workbook')
    t_site.delete_tags_from_workbook(new_wb_luid, 'flights')

    t_site.log("Add workbook to favorites for me")
    t_site.add_workbook_to_user_favorites('My favorite workbook', new_wb_luid, t_site.username)

    t_site.log("Deleting workbook from favorites for me")
    t_site.delete_workbooks_from_user_favorites(new_wb_luid, t_site.username)

    #    # Saving view as file
    #    for wb_view in wb_views_dict:
    #        t_site.log(u"Saving a png for {}".format(wb_view)
    #        t_site.save_workbook_view_preview_image(wb_luid, wb_views_dict.get(wb_view), '{}_preview'.format(wb_view))

    t_site.log('Deleting workbook')
    t_site.delete_workbooks(new_wb_luid_2)
    print('Finished Workbook tests')


def publishing_datasources_test(t_site, tdsx_file, tdsx_content_name):
    """
    :type t_site: TableauRestApiConnection
    :param tdsx_file:
    :param tdsx_content_name:
    :return:
    """
    print("Starting Datasource tests")
    default_project = t_site.query_project('Default')

    t_site.log("Publishing as {}".format(tdsx_content_name))
    new_ds_luid = t_site.publish_datasource(tdsx_file, tdsx_content_name, default_project, overwrite=True)

    time.sleep(3)

    projects = t_site.query_projects()
    projects_dict = t_site.convert_xml_list_to_name_id_dict(projects)
    projects_list = list(projects_dict.keys())

    t_site.log('Moving datasource to {} project'.format(projects_list[1]))
    t_site.update_datasource(new_ds_luid, default_project.luid, new_project_luid=projects_dict[projects_list[1]])

    t_site.log("Querying datasource")
    t_site.query_workbook(new_ds_luid)

    t_site.log('Downloading and saving datasource')
    t_site.download_datasource(new_ds_luid, 'saved_datasource')

    # Can't add to favorites until API version 2.3
    if isinstance(t_site, TableauRestApiConnection23):
        t_site.log('Adding to Favorites')
        t_site.add_datasource_to_user_favorites('The Greatest Datasource', new_ds_luid, t_site.username)

        t_site.log('Removing from Favorites')
        t_site.delete_datasources_from_user_favorites(new_ds_luid, t_site.username)

    # t_site.log("Publishing TDS with credentials -- reordered args")
    # tds_cred_luid = t_site.publish_datasource('TDS with Credentials.tds', 'TDS w Creds', project,
    # connection_username='postgres', overwrite=True, connection_password='')

    # t_site.log("Update Datasource connection")
    # t_site.update_datasource_connection(tds_cred_luid, 'localhost', '5432', db_username, db_password)

    # t_site.log("Deleting the published DS")
    # t_site.delete_datasources(new_ds_luid)

    print('Finished Datasource Tests')


def schedule_test(t_site):
    """
    :type t_site: TableauRestApiConnection23
    :return:
    """
    print('Started Schedule tests')
    all_schedules = t_site.query_schedules()
    schedule_dict = t_site.convert_xml_list_to_name_id_dict(all_schedules)
    t_site.log('All schedules on Server: {}'.format(str(schedule_dict)))
    try:
        t_site.log('Creating a daily extract schedule')
        t_site.create_daily_extract_schedule('Afternoon Delight', start_time='13:00:00')
    except AlreadyExistsException as e:
        t_site.log('Skipping the add since it already exists')

    try:
        t_site.log('Creating a monthly subscription schedule')
        new_monthly_luid = t_site.create_monthly_subscription_schedule('First of the Month', '1',
                                                                       start_time='03:00:00', parallel_or_serial='Serial')
        t_site.log('Deleting monthly subscription schedule LUID {}'.format(new_monthly_luid))
        time.sleep(4)
        t_site.delete_schedule(new_monthly_luid)
    except AlreadyExistsException as e:
        t_site.log('Skipping the add since it already exists')
    try:
        t_site.log('Creating a monthly extract schedule')
        t_site.create_monthly_extract_schedule('Last Day of Month', 'LastDay', start_time='03:00:00', priority=25)
    except AlreadyExistsException as e:
        t_site.log('Skipping the add since it already exists')

    try:
        t_site.log('Creating a weekly extract schedule')
        weekly_luid = t_site.create_weekly_subscription_schedule('Mon Wed Fri', ['Monday', 'Wednesday', 'Friday'],
                                                   start_time='05:00:00')
        time.sleep(4)

        t_site.log('Updating schedule with LUID {}'.format(weekly_luid))
        t_site.update_schedule(weekly_luid, new_name='Wed Fri', interval_value_s=['Wednesday', 'Friday'])
    except AlreadyExistsException as e:
        t_site.log('Skipping the add since it already exists')

    print('Finished Schedule tests')


def subscription_tests(t_site):
    """
    :type t_site: TableauRestApiConnection23
    :return:
    """
    print('Starting Subscription tests')
    # All users in a Group
    groups = t_site.query_groups()
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups)
    group_names = list(groups_dict.keys())

    users_in_group = t_site.query_users_in_group(groups_dict[group_names[0]])
    users_dict = t_site.convert_xml_list_to_name_id_dict(users_in_group)
    usernames = list(users_dict.keys())

    wbs = t_site.query_workbooks()
    wbs_dict = t_site.convert_xml_list_to_name_id_dict(wbs)
    wb_names = list(wbs_dict.keys())

    # Grab first workbook
    wb_luid = wbs_dict[wb_names[0]]

    sub_schedules = t_site.query_subscription_schedules()
    sched_dict = t_site.convert_xml_list_to_name_id_dict(sub_schedules)
    sched_names = list(sched_dict.keys())

    # Grab first schedule
    sched_luid = sched_dict[sched_names[0]]

    # Subscribe them to the first workbook
    t_site.log('Adding subscription with subject Important weekly update to first workbook for all users in group 1')
    for user in users_dict:
        t_site.create_subscription_to_workbook('Important weekly update', wb_luid, sched_luid, users_dict[user])

    # Find the subscriptions for user 1, delete
    t_site.query_subscriptions()
    user_1_subs = t_site.query_subscriptions(username_or_luid=usernames[0])
    t_site.log('Deleting all subscriptions for user 1')
    for sub in user_1_subs:
        luid = sub.get('id')
        t_site.delete_subscriptions(luid)

    # Update user 2 subscriptions
    t_site.log('Updating user 2s subscriptions to second schedule')
    user_2_subs = t_site.query_subscriptions(username_or_luid=usernames[1])
    for sub in user_2_subs:
        luid = sub.get('id')
        t_site.update_subscription(luid, schedule_luid=sched_dict[sched_names[1]])

    print('Finished subscription tests')


def revision_tests(t_site, workbook_name, project_name):
    """
    :type t_site: TableauRestApiConnection23
    :return:
    """
    print('Starting revision tests')
    revisions = t_site.get_workbook_revisions(workbook_name, project_name)
    t_site.log('There are {} revisions of workbook {}'.format(len(revisions), workbook_name))

    print('Finished revision tests')


def extract_refresh_test(t_site):
    """
    :type t_site: TableauRestApiConnection26
    :return:
    """
    # Only possible in 10.3 / API 2.6 and above
    if isinstance(t_site, TableauRestApiConnection26):
        print('Starting Extract Refresh tests')
        tasks = t_site.get_extract_refresh_tasks()

        print('Finished Extract Refresh tests')


for server in servers:
    print("Logging in to {}".format(servers[server]['server']))
    run_tests(servers[server]['server'], servers[server]['username'], servers[server]['password'])
