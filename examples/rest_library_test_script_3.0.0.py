from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import urllib2
import time

# Use your own server credentials
username = ''
password = ''

# Allows for testing against multiple versions of Tableau Server. Feel free to use just one
servers = {
           # "9.0": "127.0.0.1",
           # "9.1": "127.0.0.1",
           # "9.2": "127.0.0.1",
           "9.3": "127.0.0.1"
           }


# Configure which tests you want to run in here
def run_tests(server_url, username, password):
    logger = Logger('rest_example.log')
    projects_to_create = ['Sandbox', 'Approved Datasources', 'Production']
    groups_to_create = ['Publishers', 'Site Admins', 'Super Admins', 'Sales', 'Marketing', 'IT', 'VPs']

    # Actual tests
    default = TableauRestApiConnection(server_url, username, password, 'default')
    default.enable_logging(logger)
    default.signin()
    test_site = create_test_site(default, server_url, username, password, logger)

    first_project_luid = project_tests(test_site, projects_to_create)

    groups_dict = group_tests(test_site, groups_to_create)
    groups_luids = groups_dict.values()

    project_permissions_tests(test_site, first_project_luid, groups_luids)

    users_dict = user_tests(test_site, 120, groups_dict)

    twbx_filename = 'test_workbook.twbx' # Replace with your own test file
    twbx_content_name = 'Test workbook' # Replace with your own name
    twb_filename = 'TWB to Publish.twb'
    twb_content_name = 'TWB Publish Test'
    wb_luid = publishing_workbooks_test(test_site, projects_to_create[1], twbx_filename, twbx_content_name,
                                        twb_filename, twb_content_name)

    tde_filename = 'Flights Data.tde'
    tde_content_name = 'Flights Data'
    tds_filename = 'TDS to Publish SS.tds'
    tds_content_name = 'SS TDS'
    ds_luid = publishing_datasources_test(test_site, projects_to_create[1], tde_filename, tde_content_name,
                                          tds_filename, tds_content_name)

    workbook_tests(test_site, wb_luid, username)


def create_test_site(d, server_url, username, password, logger):
    # Assign this however you'd like
    new_site_content_url = 'tsite'
    new_site_name = 'Test Site'
    new_site_name_to_change_to =  'Test Site 2'
    # Determine if site exists with current name. Delete if it does.
    # Then create new site with the same name and contentUrl
    try:
        delete_login_content_url = d.query_site_content_url_by_site_name(new_site_name_to_change_to)
        print 'Received content_url to delete ' + delete_login_content_url
        test_site = TableauRestApiConnection(server_url, username, password, delete_login_content_url)
        test_site.enable_logging(logger)
        test_site.signin()
        print 'Signed in successfully to ' + delete_login_content_url

        print 'Querying the current site'
        site_xml = test_site.query_current_site()
        print site_xml

        print 'Attempting to delete current site'
        test_site.delete_current_site()
        print "Deleted site " + new_site_name
    except NoMatchFoundException as e:
        print e.msg
        print "Cannot delete site that does not exist"
    except Exception as e:
        raise

    try:
        # Create the new site
        print 'Now going into the create site'
        d.log('Logging with the log function')
        new_site_id = d.create_site(new_site_name, new_site_content_url)
        new_site_id
        print 'Created new site ' + new_site_id
    except AlreadyExistsException as e:
        print e.msg
        print "Cannot create new site, exiting"
        exit()
    except Exception as e:
        raise

    # Once we've created the site, we need to sign into it to do anything else
    test_site = TableauRestApiConnection(server_url, username, password, new_site_content_url)
    test_site.enable_logging(logger)

    test_site.signin()
    # Add groups and users to the site
    print 'Signed in successfully to ' + new_site_content_url
    # Update the site name
    print 'Updating site name'
    test_site.update_current_site(new_site_name_to_change_to)
    return test_site


def project_tests(t, project_names):
    for project in project_names:
        print "Creating Project '" + project + "'"
        t.create_project(project)
    time.sleep(4)
    first_luid = t.query_project_luid_by_name(project_names[0])
    # Change the first name
    t.update_project_by_name(project_names[0], 'Protected' + project_names[0], 'This is only for important people')
    return first_luid


def group_tests(t, group_names):
    for group in group_names:
        print "Creating Group '" + group + "'"
        new_group_luid = t.create_group(group)
        print "updating the group name"
        time.sleep(3)
        t.update_group_by_luid(new_group_luid, group + ' (updated)')

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


def project_permissions_tests(t, project_luid, group_luids):
    proj_obj = t.get_project_object_by_luid(project_luid)
    for group_luid in group_luids:
        gcap = t.get_grantee_capabilities_object(u'group', group_luid, content_type=u'project')
        if t.api_version == u"2.0":
            gcap.set_capability('Read', 'Allow')
            gcap.set_capability('Filter', 'Allow')
            gcap.set_capability('ShareView', 'Allow')
            gcap.set_capability('Delete', 'Allow')
            gcap.set_capability('Write', 'Deny')
            gcap.set_capability('View Underlying Data', 'Deny')
        else:
            gcap.set_capability(u"View", u"Allow")
            gcap.set_capability(u"Save", u"Allow")

            wb_def_gcap = t.get_grantee_capabilities_object(u'group', group_luid, content_type=u'workbook')
            wb_def_gcap.set_capabilities_to_match_role(u"Interactor")
            print "Setting default permissions for workbooks on first project"
            proj_obj.workbook_default.set_permissions_by_gcap_obj(wb_def_gcap)

        print 'Updating the permissions on the first project'
        try:
            proj_obj.set_permissions_by_gcap_obj(gcap)
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

    # This takes Users x Groups amount of time to complete, can really stretch out the test
    for group in groups_dict:
        print "Adding users to group {}".format(group)
        t.add_users_to_group_by_luid(new_user_luids, groups_dict.get(group))

    users = t.query_users()
    users_dict = t.convert_xml_list_to_name_id_dict(users)
    print "All the users:"
    print users_dict
    user_1_luid = t.query_user_luid_by_username('user1')
    print " User 1 luid: {}".format(user_1_luid)
    # Teardown users
    # Delete all of the users that were just created
    # test_site.remove_users_from_site_by_luid(new_user_luids)
    return users_dict


def publishing_workbooks_test(t, proj_name, twbx_filename, twbx_content_name, twb_filename=None, twb_content_name=None):
    project_luid = t.query_project_luid_by_name(proj_name)
    print 'Publishing TWBX workbook to {}'.format(proj_name)
    production_luid = t.query_project_luid_by_name(proj_name)

    new_wb_luid = t.publish_workbook(twbx_filename, twbx_content_name, production_luid, True)
    print 'Moving workbook to Sandbox'
    t.update_workbook_by_luid(new_wb_luid, project_luid, show_tabs=True)
    print "querying workbook"
    wb_xml = t.query_workbook_by_luid(new_wb_luid)
    return new_wb_luid

    # Save workbook preview image
    print "Saving workbook preview image"
    t.save_workbook_preview_image_by_luid(new_wb_luid, 'Workbook preview')

    print "Saving workbook file"
    t.download_workbook_by_luid(new_wb_luid, 'saved workbook')

    if twbx_filename is not None:
        print "Publishing a TWB"
        twb_luid = t.publish_workbook(twb_filename, twbx_content_name, project_luid)

        print "Downloading TWB"
        t.download_workbook_by_luid(twb_luid, 'TWB Save')


def publishing_datasources_test(t, proj_name, tde_filename, tde_content_name, tds_filename, tds_content_name):
    project_luid = t.query_project_luid_by_name(proj_name)
    print 'Publishing datasource to {}'.format(proj_name)

    new_ds_luid = t.publish_datasource(tde_filename, tde_content_name, project_luid, True)
    print 'Publishing as {}'.format(new_ds_luid)
    print "Query the datasource"
    ds_xml = t.query_datasource_by_luid(new_ds_luid)

    datasources = t.query_datasources()

    print "Saving Datasource"
    t.download_datasource_by_luid(new_ds_luid, 'saved_datasource')

    # print "Deleting the published DS"
    # test_site.delete_datasources_by_luid(new_ds_luid)

    print "Publishing a TDS"
    tds_luid = t.publish_datasource(tds_filename, tds_content_name, project_luid)

    # print "Publishing TDS with credentials -- reordered args"
    # tds_cred_luid = test_site.publish_datasource('TDS with Credentials.tds', 'TDS w Creds', project_luid, connection_username='postgres', overwrite=True, connection_password='')

    # print "Update Datasource connection"
    # test_site.update_datasource_connection_by_luid(tds_cred_luid, 'localhost', '5432', db_username, db_password)

    print "Saving TDS"
    t.download_datasource_by_luid(tds_luid, 'TDS Save')

    # print "Publishing a TDSX"
    # test_site.publish_datasource('TDSX to Publish.tdsx', 'TDSX Publish Test', project_luid)

    return new_ds_luid


def workbook_tests(t, wb_luid, username):
    print "Query workbook connections"
    wb_connections = t.query_workbook_connections_by_luid(wb_luid)
    print wb_connections

    print "Querying workbook permissions"
    wb_permissions = t.query_workbook_permissions_by_luid(wb_luid)
    print wb_permissions

    print "Querying workbook views"
    wb_views = t.query_workbook_views_by_luid(wb_luid, True)
    print wb_views

    wb_views_dict = t.convert_xml_list_to_name_id_dict(wb_views)

    print wb_views_dict

    for wb_view in wb_views_dict:
        print "Adding {} to favorites for me".format(wb_view)
        t.add_view_to_user_favorites_by_luid('Fav: {}'.format(wb_view), wb_views_dict.get(wb_view), t.query_user_luid_by_username(username))

    for wb_view in wb_views_dict:
        print "Deleting {} to favorites for me".format(wb_view)
        t.delete_views_from_user_favorites_by_luid(wb_views_dict.get(wb_view), t.query_user_luid_by_username(username))

    # Saving view as file
    for wb_view in wb_views_dict:
        print "Saving a png for {}".format(wb_view)
        t.save_workbook_view_preview_image_by_luid(wb_luid, wb_views_dict.get(wb_view), '{}_preview'.format(wb_view))

    print 'Adding tags to workbook'
    t.add_tags_to_workbook_by_luid(wb_luid, ['workbooks', 'flights', 'cool'])

    print 'Deleting a tag from workbook'
    t.delete_tags_from_workbook_by_luid(wb_luid, 'flights')

    print "Add workbook to favorites for me"
    t.add_workbook_to_user_favorites_by_luid('My favorite workbook', wb_luid, t.query_user_luid_by_username(username))

    print "Deleting workbook from favorites for me"
    t.delete_workbooks_from_user_favorites_by_luid(wb_luid, t.query_user_luid_by_username(username))


def datasource_tests(t, ds_luid):
    print "Moving datasource to production"
    t.update_datasource_by_luid(ds_luid, 'Flites Datums', production_luid)


for server in servers:
    print "Logging in to {}".format(server)
    run_tests("http://" + servers[server], username, password)

