# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import urllib2
import time

# Use your own server credentials
username = u''
password = u''

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
           # u"9.0": u"127.0.0.1",
           # u"9.1": u"127.0.0.1",
           # u"9.2": u"127.0.0.1",
           # u"9.3": u"127.0.0.1",
           # u'10.0' : u"127.0.0.1",
           # u'10.1' : u"127.0.0.1",
           u"10.2": u"127.0.0.1"

           }

non_latin_words = [u'ASCII', u'Οὐχὶ ταὐτὰ', u'γιγνώσκειν', u'რეგისტრაცია', u'Международную', u'โฮจิ๋นเรียกทัพทั่วหัวเมืองมา', u'አይተዳደርም።',
                       u'晚飯', u'晩ご飯', u'저녁밥', u'bữa ăn tối', u'Señor']


# Configure which tests you want to run in here
def run_tests(server_url, username, password):
    logger = Logger(u'rest_example.log')
    projects_to_create = [u'Sandbox', u'Approved Datasources', u'Production']
    groups_to_create = [u'Publishers', u'Site Admins', u'Super Admins', u'Sales', u'Marketing', u'IT', u'VPs']

    # Actual tests
    default = TableauRestApiConnection25(server_url, username, password, u'default')
    default.enable_logging(logger)
    default.signin()
    test_site = create_test_site(default, server_url, username, password, logger)

    #first_project_obj = project_tests(test_site, non_latin_words)

    groups_dict = group_tests(test_site, groups_to_create)
  #  groups_luids = groups_dict.values()

   # project_permissions_tests(test_site, first_project_obj, groups_luids)

    users_dict = user_tests(test_site, 120, groups_dict)
    test_site.query_user(u'user1')
    print u'user1 luid is {}'.format(test_site.query_user_luid(u'user1'))
#    twbx_filename = 'test_workbook.twbx' # Replace with your own test file
#    twbx_content_name = 'Test workbook' # Replace with your own name
#    twb_filename = 'TWB to Publish.twb'
#    twb_content_name = 'TWB Publish Test'
#   wb_luid = publishing_workbooks_test(test_site, international_words[1], twbx_filename, twbx_content_name,
  #                                      twb_filename, twb_content_name)

#    tde_filename = 'Flights Data.tde'
#    tde_content_name = 'Flights Data'
#    tds_filename = 'TDS to Publish SS.tds'
#    tds_content_name = 'SS TDS'
#    ds_luid = publishing_datasources_test(test_site, international_words[1], tde_filename, tde_content_name,
  #                                        tds_filename, tds_content_name)

 #   workbook_tests(test_site, wb_luid, username)


def create_test_site(d, server_url, username, password, logger):
    # Assign this however you'd like
    new_site_content_url = u'tsite'
    new_site_name = u'Test Site'
    new_site_name_to_change_to = u'Test Site 2'
    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        print u'Received content_url to delete ' + new_site_content_url
        test_site = TableauRestApiConnection25(server_url, username, password, new_site_content_url)
        test_site.enable_logging(logger)
        test_site.signin()
        print u'Signed in successfully to ' + new_site_content_url

        print u'Querying the current site'
        site_xml = test_site.query_current_site()

        print u'Attempting to delete current site'
        test_site.delete_current_site()
        print u"Deleted site " + new_site_name
    except NoMatchFoundException as e:
        print e.msg
        print u"Cannot delete site that does not exist"
    except Exception as e:
        raise

    try:
        # Create the new site
        print u'Now going into the create site'
        d.log(u'Logging with the log function')
        new_site_id = d.create_site(new_site_name, new_site_content_url)
        print u'Created new site ' + new_site_id
    except AlreadyExistsException as e:
        print e.msg
        print u"Cannot create new site, exiting"
        exit()
    except Exception as e:
        raise

    # Once we've created the site, we need to sign into it to do anything else
    test_site = TableauRestApiConnection21(server_url, username, password, new_site_content_url)
    test_site.enable_logging(logger)

    test_site.signin()
    # Add groups and users to the site
    print u'Signed in successfully to ' + new_site_content_url
    # Update the site name
    print u'Updating site name'
    test_site.update_site(new_site_name_to_change_to)
    return test_site


def project_tests(t, project_names):
    for project in project_names:
        print u"Creating Project {}".format(project).encode(u'utf8')
        t.create_project(project)
    time.sleep(4)

    proj_obj = t.update_project(project_names[0], u'Protected' + project_names[0], u'This is only for important people')
    return proj_obj


def group_tests(t, group_names):
    for group in group_names:
        print "Creating Group '" + group + "'"
        new_group_luid = t.create_group(group)
        print "updating the group name"
        time.sleep(3)
        t.update_group(new_group_luid, group + ' (updated)')

    print "Sleeping 1 second for group creation to finish"
    # It does take a second for the indexing to update, so if you've made a lot of changes, pause for 1 sec
    time.sleep(1)

    print "Get all the groups"
    groups_on_site = t.query_groups()
    # Assign permissions on each project, for each group

    print "Converting the groups to a dict"
    # Convert the list to a dict {name : luid}
    groups_dict = t.convert_xml_list_to_name_id_dict(groups_on_site)
    print groups_dict
    return groups_dict


def project_permissions_tests20(t, project_obj, group_luids):
    """
    :type t: TableauRestApiConnection
    :type project_obj: Project21 or Project20
    :type group_luids: list[unicode]
    :return:
    """

    for group_luid in group_luids:
        perms = project_obj.get_project_permissions_object(u'group', group_luid)

        perms.set_capability(u"View", u"Allow")
        perms.set_capability(u"Save", u"Allow")


def project_permissions_tests21(t, project_obj, group_luids):
    """
    :type t: TableauRestApiConnection
    :type project_obj: Project21 or Project20
    :type group_luids: list[unicode]
    :return:
    """

    for group_luid in group_luids:
        perms = project_obj.get_project_permissions_object_for_group(group_luid)

        perms.set_capability(u"View", u"Allow")
        perms.set_capability(u"Save", u"Allow")

        wb_def_permissions = project_obj.get_workbook_permissions_object_for_group(group_luid)
        wb_def_permissions.set_capabilities_to_match_role(u"Interactor")
        print u"Setting default permissions for workbooks on first project"
        project_obj.workbook_default.set_permissions_by_permissions_obj_list([wb_def_permissions, ])

        print u'Updating the permissions on the first project'
        try:
            project_obj.set_permissions_by_permissions_obj_list([perms, ])
        except InvalidOptionException as e:
            print e.msg
            raise


def user_tests(t, num_of_users, groups_dict):
    # Create some fake users to assign to groups
    new_user_luids = []
    for i in range(1, num_of_users):
        username = "user" + str(i)
        full_name = "User {}".format(str(i))
        print "Creating User '{}' named '{}'".format(username, full_name)
        new_user_luid = t.add_user(username, full_name, 'Interactor', 'password', username + '@nowhere.com')
        print "New User LUID : {}".format(new_user_luid)
        new_user_luids.append(new_user_luid)

    for name in non_latin_words:
        username = name
        full_name = name
        print "Creating User '{}' named '{}'".format(username.encode(u'utf8'), full_name.encode(u'utf8'))
        new_user_luid = t.add_user(username, full_name, 'Interactor', 'password', username + u'@nowhere.com')
        print "New User LUID : {}".format(new_user_luid)
        new_user_luids.append(new_user_luid)

    # This takes Users x Groups amount of time to complete, can really stretch out the test
    for group in groups_dict:
        print "Adding users to group {}".format(group)
        t.add_users_to_group(new_user_luids, groups_dict.get(group))

    users = t.query_users()
    users_dict = t.convert_xml_list_to_name_id_dict(users)
    print "All the users:"
    print users_dict
    user_1_luid = t.query_user_luid('user1')
    print " User 1 luid: {}".format(user_1_luid)
    # Teardown users
    # Delete all of the users that were just created
    # test_site.remove_users_from_site(new_user_luids)
    return users_dict


def publishing_workbooks_test(t, proj_name, twbx_filename, twbx_content_name, twb_filename=None, twb_content_name=None):
    try:
        project_luid = t.query_project_luid(proj_name)
    except NoMatchFoundException as e:
        print e.msg
        raise
    print 'Publishing TWBX workbook to {}'.format(proj_name.encode('utf-8'))
    production_luid = t.query_project_luid(proj_name)

    new_wb_luid = t.publish_workbook(twbx_filename, twbx_content_name, production_luid, True)
    print 'Moving workbook to Sandbox'
    t.update_workbook(new_wb_luid, project_luid, show_tabs=True)
    print "querying workbook"
    wb_xml = t.query_workbook(new_wb_luid)
    return new_wb_luid

    # Save workbook preview image
    print "Saving workbook preview image"
    t.save_workbook_preview_image(new_wb_luid, 'Workbook preview')

    print "Saving workbook file"
    t.download_workbook(new_wb_luid, 'saved workbook')

    if twbx_filename is not None:
        print "Publishing a TWB"
        twb_luid = t.publish_workbook(twb_filename, twbx_content_name, project_luid)

        print "Downloading TWB"
        t.download_workbook(twb_luid, 'TWB Save')


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


def workbook_tests(t, wb_luid, username):
    print "Query workbook connections"
    wb_connections = t.query_workbook_connections(wb_luid)
    print wb_connections

    print "Querying workbook views"
    wb_views = t.query_workbook_views(wb_luid, True)
    print wb_views

    wb_views_dict = t.convert_xml_list_to_name_id_dict(wb_views)

    print wb_views_dict

    for wb_view in wb_views_dict:
        print "Adding {} to favorites for me".format(wb_view)
        t.add_view_to_user_favorites('Fav: {}'.format(wb_view), wb_views_dict.get(wb_view), t.query_user_luid_by_username(username))

    for wb_view in wb_views_dict:
        print "Deleting {} to favorites for me".format(wb_view)
        t.delete_views_from_user_favorites(wb_views_dict.get(wb_view), t.query_user_luid_by_username(username))

    # Saving view as file
    for wb_view in wb_views_dict:
        print "Saving a png for {}".format(wb_view)
        t.save_workbook_view_preview_image(wb_luid, wb_views_dict.get(wb_view), '{}_preview'.format(wb_view))

    print 'Adding tags to workbook'
    t.add_tags_to_workbook(wb_luid, ['workbooks', 'flights', 'cool'])

    print 'Deleting a tag from workbook'
    t.delete_tags_from_workbook(wb_luid, 'flights')

    print "Add workbook to favorites for me"
    t.add_workbook_to_user_favorites('My favorite workbook', wb_luid, t.query_user_luid_by_username(username))

    print "Deleting workbook from favorites for me"
    t.delete_workbooks_from_user_favorites(wb_luid, t.query_user_luid_by_username(username))


def datasource_tests(t, ds_luid):
    print "Moving datasource to production"
    # t.update_datasource_by_luid(ds_luid, 'Flites Datums', production_luid)


for server in servers:
    print "Logging in to {}".format(server)
    run_tests("http://" + servers[server], username, password)

