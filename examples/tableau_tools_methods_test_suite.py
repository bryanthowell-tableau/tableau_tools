# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import urllib2
import time

# This is meant to test all relevant functionality of the tableau_tools library.
# It does a lot of things that you wouldn't necessarily do just to make sure they work

# Other than printing messages before and afte each test, logging of what actually happens going into the log file
# rather than to the console.

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
           # u"9.0": {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u"9.1": {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u"9.2": {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u"9.3": {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u'10.0': {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u'10.1': {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           # u"10.2": {u"server": u"127.0.0.1", u"username": u"", u"password": u""},
           u"10.3": {u"server": u"127.0.0.1", u"username": u"", u"password": u""}
           }

# Configure which tests you want to run in here
def run_tests(server_url, username, password):
    # There are used to ensure that Unicode is being handled correctly. They are random but hit a lot of character sets
    words = [u'ASCII', u'Οὐχὶ ταὐτὰ', u'γιγνώσκειν', u'რეგისტრაცია', u'Международную', u'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา',
             u'አይተዳደርም።', u'晚飯', u'晩ご飯', u'저녁밥', u'bữa ăn tối', u'Señor']

    # Test Files to Publish
    twbx_filename = 'test_workbook.twbx' # Replace with your own test file
    twbx_content_name = 'Test workbook' # Replace with your own name

    logger = Logger(u'tableau_tools_test.log')

    # Create a default connection
    default = TableauRestApiConnection25(server_url, username, password, u'default')
    default.enable_logging(logger)
    default.signin()

    # Step 1: Creating a test site
    test_site = create_test_site(default, server_url, username, password, logger)

    # Step 2: Project tests
    project_tests(test_site, words)

    # Step 3: Group tests
    group_tests(test_site, words)

    # Step 4: Project Permissions tests
    version = test_site.api_version
    if version == u'2.0':
        project_permissions_tests20(test_site)
    else:
        project_permissions_tests21(test_site)

    # Step 5: User Tests
    user_tests(test_site, words)

    # Step 6: Publishing Workbook Tests
    workbooks_test(test_site, twbx_filename, twbx_content_name)

#    tde_filename = 'Flights Data.tde'
#    tde_content_name = 'Flights Data'
#    tds_filename = 'TDS to Publish SS.tds'
#    tds_content_name = 'SS TDS'
#    ds_luid = publishing_datasources_test(test_site, international_words[1], tde_filename, tde_content_name,
  #                                        tds_filename, tds_content_name)

 #   workbook_tests(test_site, wb_luid, username)


def create_test_site(default_site, server_url, username, password, logger):
    """
    :type default_site: TableauRestApiConnection
    :type server_url: unicode
    :type username: unicode
    :type password: unicode
    :type logger: Logger
    :rtype: TableauRestApiConnection25
    """
    print u"Creating a test site"
    logger.log(u'Creating test site')
    # Assign this however you'd like
    new_site_content_url = u'tsite'
    new_site_name = u'Test Site'
    new_site_name_to_change_to = u'Test Site 2'
    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        logger.log(u'Received content_url to delete ' + new_site_content_url)
        test_site = TableauRestApiConnection25(server_url, username, password, new_site_content_url)
        test_site.signin()
        test_site.enable_logging(logger)
        logger.log(u'Signed in successfully to ' + new_site_content_url)

        site_xml = test_site.query_current_site()

        logger.log(u'Attempting to delete current site')
        test_site.delete_current_site()
        logger.log(u"Deleted site " + new_site_name)
    except RecoverableHTTPException as e:
        logger.log(e.tableau_error_code)
        logger.log(u"Cannot delete site that does not exist, assuming it already exists and continuing")
    except Exception as e:
        raise

    try:
        # Create the new site
        logger.log(u'Now going into the create site')
        default_site.log(u'Logging with the log function')
        new_site_id = default_site.create_site(new_site_name, new_site_content_url)
        logger.log(u'Created new site ' + new_site_id)
    except AlreadyExistsException as e:
        print e.msg
        print u"Cannot create new site due to error, exiting"
        exit()
    except Exception as e:
        raise

    # Once we've created the site, we need to sign into it to do anything else
    test_site = TableauRestApiConnection25(server_url, username, password, new_site_content_url)
    test_site.enable_logging(logger)

    test_site.signin()
    logger.log(u'Signed in successfully to ' + new_site_content_url)
    # Update the site name
    logger.log(u'Updating site name')
    test_site.update_site(site_name=new_site_name_to_change_to)
    logger.log(u"Getting all site_content_urls on the server")
    all_site_content_urls = test_site.query_all_site_content_urls()
    logger.log(unicode(all_site_content_urls))
    print u'Finished creating new site'
    return test_site


def project_tests(t_site, project_names):
    """
    :type t_site: TableauRestApiConnection
    :type project_names: list[unicode]
    """
    print u'Testing project methods'
    for project in project_names:
        t_site.log(u"Creating Project {}".format(project).encode(u'utf8'))
        t_site.create_project(project)

    # Sleep ensures we don't get ahead of the REST API updating with the new projects
    time.sleep(4)
    t_site.log(u'Updating first project')
    t_site.update_project(project_names[0], u'Updated ' + project_names[0], u'This is only for important people')
    t_site.log(u"Deleting second and third projects")
    t_site.delete_projects([project_names[1], project_names[2]])
    print u"Finished testing project methods"


# Delete Groups not introudced until API 2.1
def group_tests(t_site, group_names):
    """
    :type t_site: TableauRestApiConnection21
    :param group_names:
    :return:
    """
    print u"Starting group tests"
    for group in group_names:
        t_site.log(u"Creating Group {}".format(group))
        new_group_luid = t_site.create_group(group)

    # Let all of the groups settle in
    time.sleep(3)
    t_site.log(u'Updating first group name')
    t_site.update_group(group_names[0], group_names[0] + u' (updated)')

    t_site.log(u'Deleting fourth group')
    t_site.delete_groups(group_names[3])

    t_site.log(u"Querying all the groups")
    groups_on_site = t_site.query_groups()

    # Convert the list to a dict {name : luid}
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups_on_site)
    t_site.log(unicode(groups_dict))
    print u'Finished group tests'
    time.sleep(3)  # Let everything update
    return groups_dict


def project_permissions_tests20(t, project_obj, group_luids):
    """
    :type t: TableauRestApiConnection20
    :type project_obj: Project21 or Project20
    :type group_luids: list[unicode]
    :return:
    """

    for group_luid in group_luids:
        perms = project_obj.get_project_permissions_object(u'group', group_luid)

        perms.set_capability(u"View", u"Allow")
        perms.set_capability(u"Save", u"Allow")


def project_permissions_tests21(t_site):
    """
    :type t_site: TableauRestApiConnection21
    :return:
    """
    print u"Starting Permissions tests"
    projects = t_site.query_projects()
    projects_dict = t_site.convert_xml_list_to_name_id_dict(projects)
    project_names = projects_dict.keys()

    groups = t_site.query_groups()
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups)
    group_names = groups_dict.keys()

    # Set permissions for one project
    t_site.log(u'Querying project called {}'.format(project_names[0]))
    proj_1 = t_site.query_project(projects_dict[project_names[0]])
    t_site.log(u'Setting project to locked permissions')
    proj_1.lock_permissions()
    t_site.log(u'Clearing all existing permissions on first project')
    proj_1.clear_all_permissions()

    proj_perms_list = []
    for group in groups_dict:
        proj_perms = proj_1.create_project_permissions_object_for_group(groups_dict[group], u'Viewer')
        proj_perms_list.append(proj_perms)
    proj_1.set_permissions_by_permissions_obj_list(proj_perms_list)

    # WB defaults
    wb_perms_list = []
    for group in groups_dict:
        wb_perms = proj_1.create_workbook_permissions_object_for_group(groups_dict[group], u'Interactor')
        wb_perms_list.append(wb_perms)
    t_site.log(u'Setting workbook permissions')
    proj_1.workbook_defaults.set_permissions_by_permissions_obj_list(wb_perms_list)

    # DS defaults
    ds_perms_list = []
    for group in groups_dict:
        ds_perms = proj_1.create_datasource_permissions_object_for_group(groups_dict[group], u'Editor')
        ds_perms_list.append(ds_perms)
    t_site.log(u'Setting datasource permissions')
    proj_1.datasource_defaults.set_permissions_by_permissions_obj_list(ds_perms_list)

    # Second Project
    t_site.log(u'Querying project called {}'.format(project_names[5]))
    proj_2 = t_site.query_project(projects_dict[project_names[5]])
    t_site.log(u'Unlocking permissions')
    proj_2.unlock_permissions()
    proj_2.clear_all_permissions(clear_defaults=False)  # Don't clear workbook or datasource defaults

    proj_perms = proj_2.create_project_permissions_object_for_group(groups_dict[group_names[6]])
    proj_perms.set_all_to_allow()
    proj_perms.set_capability_to_unspecified(u'Save')
    t_site.log(u'Setting project permissions for group {}'.format(group_names[6]))
    proj_2.set_permissions_by_permissions_obj_list([proj_perms, ])

    # Clone Permissions from one to another
    t_site.log(u'Cloning project permissions from {} to {}'.format(project_names[3], project_names[0]))
    proj_3 = t_site.query_project(projects_dict[project_names[3]])
    proj_3.replicate_permissions(proj_1)

    print u'Finished Permissions tests'


def user_tests(t_site, names):
    """
    :type t_site: TableauRestApiConnection
    :type names: list[unicode]
    :return:
    """
    print u'Starting User tests'
    # Create some fake users to assign to groups

    new_user_luids = []
    for name in names:
        username = name
        full_name = name.upper()
        t_site.log(u"Creating User '{}' named '{}'".format(username, full_name))
        new_user_luid = t_site.add_user(username, full_name, u'Interactor', u'password', username + u'@nowhere.com')
        new_user_luids.append(new_user_luid)

    # This takes Users x Groups amount of time to complete, can really stretch out the test
    groups = t_site.query_groups()
    groups_dict = t_site.convert_xml_list_to_name_id_dict(groups)
    group_names = groups_dict.keys()

    # Add all users to first group
    t_site.log(u"Adding users to group {}".format(group_names[0]))
    t_site.add_users_to_group(new_user_luids, groups_dict[group_names[0]])

    # Add first three users to second gruop

    t_site.log(u"Adding users to group {}".format(group_names[1]))
    t_site.add_users_to_group([new_user_luids[0], new_user_luids[1], new_user_luids[3]], groups_dict[group_names[1]])

    t_site.log(u'Unlicensing the second user')
    t_site.update_user(new_user_luids[1], site_role=u'Unlicensed')

    t_site.log(u'Removing the third user')
    t_site.remove_users_from_site(new_user_luids[2])

    # Sleep to let updates happen
    time.sleep(4)
    users = t_site.query_users()
    users_dict = t_site.convert_xml_list_to_name_id_dict(users)
    t_site.log(unicode(users_dict.keys()))

    print u'Finished User tests'


def workbooks_test(t_site, twbx_filename, twbx_content_name):
    """
    :type t_site: TableauRestApiConnection
    :type twbx_filename: unicode
    :type twbx_content_name: unicode
    :return:
    """
    print u"Starting Workbook tests"

    default_project = t_site.query_project(u'Default')
    t_site.log(u'Publishing workbook as {}'.format(twbx_content_name))
    new_wb_luid = t_site.publish_workbook(twbx_filename, twbx_content_name, default_project, overwrite=True)

    time.sleep(3)

    projects = t_site.query_projects()
    projects_dict = t_site.convert_xml_list_to_name_id_dict(projects)
    projects_list = projects_dict.keys()

    t_site.log(u'Moving workbook to {} project'.format(projects_list[0]))
    t_site.update_workbook(new_wb_luid, default_project.luid, new_project_luid=projects_dict[projects_list[0]], show_tabs=True)

    t_site.log(u"Querying workbook")
    t_site.query_workbook(new_wb_luid)

    # Save workbook preview image
    t_site.log(u"Saving workbook preview image")
    t_site.save_workbook_preview_image(new_wb_luid, u'Workbook preview')

    t_site.log(u"Downloading workbook file")
    t_site.download_workbook(new_wb_luid, u'saved workbook')

    t_site.log(u"Query workbook connections")
    t_site.query_workbook_connections(new_wb_luid)

    t_site.log(u"Querying workbook views")
    wb_views = t_site.query_workbook_views(new_wb_luid)
    wb_views_dict = t_site.convert_xml_list_to_name_id_dict(wb_views)

    t_site.log(unicode(wb_views_dict))
#
#    for wb_view in wb_views_dict:
#        t_site.log(u"Adding {} to favorites for me".format(wb_view)
#        t_site.add_view_to_user_favorites('Fav: {}'.format(wb_view), wb_views_dict.get(wb_view), t_site.query_user_luid_by_username(username))
#
#    for wb_view in wb_views_dict:
#        t_site.log(u"Deleting {} to favorites for me".format(wb_view)
#        t_site.delete_views_from_user_favorites(wb_views_dict_site.get(wb_view), t_site.query_user_luid_by_username(username))
#
#    # Saving view as file
#    for wb_view in wb_views_dict:
#        t_site.log(u"Saving a png for {}".format(wb_view)
#        t_site.save_workbook_view_preview_image(wb_luid, wb_views_dict.get(wb_view), '{}_preview'.format(wb_view))
#
#    t_site.log(u'Adding tags to workbook')
#    t_site.add_tags_to_workbook(wb_luid, ['workbooks', 'flights', 'cool'])
#
#    t_site.log(u'Deleting a tag from workbook')
#    t_site.delete_tags_from_workbook(wb_luid, 'flights')
#
#    t_site.log(u"Add workbook to favorites for me")
#    t_site.add_workbook_to_user_favorites('My favorite workbook', wb_luid, t_site.query_user_luid_by_username(username))
#
#    t_site.log(u"Deleting workbook from favorites for me")
#    t_site.delete_workbooks_from_user_favorites(wb_luid, t_site.query_user_luid_by_username(username))

    print u'Finished Workbook tests'


def publishing_datasources_test(t, proj_name, tde_filename, tde_content_name, tds_filename, tds_content_name):
    project_luid = t.query_project_luid(proj_name)
    print 'Publishing datasource to {}'.format(proj_name.encode(u'utf-8'))

    new_ds_luid = t.publish_datasource(tde_filename, tde_content_name, project_luid, True)
    print 'Publishing as {}'.format(new_ds_luid)
    print "Query the datasource"
    ds_xml = t.query_datasource(new_ds_luid)

    datasources = t.query_datasources()

    print "Saving Datasource"
    t.download_datasource(new_ds_luid, 'saved_datasource')

    # print "Deleting the published DS"
    # test_site.delete_datasources(new_ds_luid)

    print "Publishing a TDS"
    tds_luid = t.publish_datasource(tds_filename, tds_content_name, project_luid)

    # print "Publishing TDS with credentials -- reordered args"
    # tds_cred_luid = test_site.publish_datasource('TDS with Credentials.tds', 'TDS w Creds', project_luid, connection_username='postgres', overwrite=True, connection_password='')

    # print "Update Datasource connection"
    # test_site.update_datasource_connection(tds_cred_luid, 'localhost', '5432', db_username, db_password)

    print "Saving TDS"
    t.download_datasource(tds_luid, 'TDS Save')

    # print "Publishing a TDSX"
    # test_site.publish_datasource('TDSX to Publish.tdsx', 'TDSX Publish Test', project_luid)

    return new_ds_luid


def datasource_tests(t, ds_luid):
    print "Moving datasource to production"
    # t.update_datasource_by_luid(ds_luid, 'Flites Datums', production_luid)


for server in servers:
    print u"Logging in to {}".format(servers[server][u'server'])
    run_tests(u"http://" + servers[server][u'server'], servers[server][u'username'], servers[server][u'password'])

