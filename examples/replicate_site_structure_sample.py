# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
from tableau_tools.tableau_documents import *
import time
import os

o_server = u'http://127.0.0.1'
o_username = u''
o_password = u''
original_content_url = u'test_site'

n_server = u'http://127.0.0.1'
n_username = u''
n_password = u''
new_site_name = u'Test Site Replica'
new_site_content_url = u'test_site_replica'


# Sign in to the original site with an administrator level user
o = TableauRestApiConnection31(server=o_server, username=o_username,
                               password=o_password, site_content_url=original_content_url)
o.signin()
logger = Logger(u'replicate_site_sample.log')
logger.enable_debug_level()
# Enable logging after sign-in to hide credentials
o.enable_logging(logger)

# Sign in to the new Server on default as a Server Admin
n_default = TableauRestApiConnection31(server=n_server, username=n_username,
                                       password=n_password, site_content_url=u"default")
n_default.signin()
n_default.enable_logging(logger)

# Die if the new site already exists
try:
    n_default.create_site(new_site_name, new_site_content_url, admin_mode=u'ContentOnly')
    print(u'New Site Created')
except AlreadyExistsException as e:
#    print(e.msg)
#    print(u"Cannot create new site, it already exists. Exiting")
#    exit()
    # Alternative pathway blows away the existing site if it finds one with that site_content_url
    print(u'Site with name already existed, removing and then creating the new site')
    n_existing_to_replace = TableauRestApiConnection31(server=n_server, username=n_username,
                                                       password=n_password, site_content_url=new_site_content_url)
    n_existing_to_replace.signin()
    n_existing_to_replace.enable_logging(logger)
    n_existing_to_replace.delete_current_site()
    # Now Create the new site
    n_default.create_site(new_site_name, new_site_content_url)
    print(u'New Site Created')
n_default.signout()


# Connect to the newly created site
n = TableauRestApiConnection31(server=n_server, username=n_username,
                               password=n_password, site_content_url=new_site_content_url)
n.signin()
n.enable_logging(logger)
print(u"Signed in to new site, beginning replication")

# Now we start replicating from one site to the other
print(u"Starting groups")
# Replicate Groups first, so you can put users in them
groups = o.query_groups()
for group in groups:
    # Can't add All Users because it is generated automatically
    if group.get(u'name') == u'All Users':
        continue

    n.create_group(direct_xml_request=n.build_request_from_response(group))
print(u"Finished groups")
# Replicate Users
print(u"Starting users")
users = o.query_users()
# Loop through the Element objects themselves because of more details to add
excluded_server_admins = []
for user in users:
    # Can't add the Guest user
    if user.get(u'name') == u'guest':
        continue

    # It seems that ServerAdmins cannot be added programmatically, and might not come in automatically in all cases
    # Best to exclude them (or with Tableau Online, move down to a SiteAdministrator role)
    if user.get(u'siteRole') == u'ServerAdministrator':
        # With Tableau Online, move them down to a SiteAdministrator. Uncomment for Tableau Online
        #user.set(u'siteRole', u'SiteAdministratorCreator')

        # If you are on a Tableau Server and already a Server Administrator, you need to be excluded from
        print(u"Excluded this ServerAdministrator {}".format(user.get(u'name')))
        excluded_server_admins.append(user.get(u'name'))
        continue

    # This assumes a straight replication from source username to the new site
    user_request = n.build_request_from_response(user)

    # If instead you are changing how the usernames work (for example, using e-mail addresses, you can change here
    # Uncomment the following for that feature
    #new_username = u'{}@{}'.format(user.get(u'name'), u'mydomain.net')
    #user_request.set(u'name', new_username)

    # This actually sends the request (for Tableau Server)
    n.add_user(direct_xml_request=user_request)

    # Tableau Online variation
    # Tableau Online only allows you to add an e-mail address, as the name field, so you shouldn't use the dir

    # Similar to above if you need to tr
    # new_username = u'{}@{}'.format(user.get(u'name'), u'mydomain.net')
    # n.add_user(username=new_username, site_role=user.get(u'siteRole'), auth_setting=u'SAML')  # or u'ServerDefault'
# Sleep a bit to let them all get added in
print(u"Finished users, sleeping for a few moments")
time.sleep(4)

# Put users in groups
print(u"Starting users in groups")
# Grab all the group names from the original site
groups_dict = o.convert_xml_list_to_name_id_dict(groups)

# Loop through all the original groups, then request the users in each one from the original site
for group in groups_dict:
    if group == u'All Users':
        continue
    group_users = o.query_users_in_group(group)
    group_users_dict = o.convert_xml_list_to_name_id_dict(group_users)

    # If names match exactly, use this
    new_group_users = group_users_dict.keys()

    # Exclude any users who were Server Admins, they cannot be added programmatically and thus won't be on the Site
    if len(excluded_server_admins) > 0:
        new_group_without_server_admins = []
        for user in new_group_users:
            if user in excluded_server_admins:
                continue
            else:
                new_group_without_server_admins.append(user)
        new_group_users = new_group_without_server_admins
    # If you needed to do name translations from original to the new one when you added above,
    # do the same here (uncomment)
    #new_group_users = []
    #for user in group_users_dict:
    # Skip any users who are on the excluded_server_admins list
    #    if user.get(u'name') in excluded_server_admins:
    #       continue
    #    new_name = u'{}@{}'.format(user.get(u'name'), u'mydomain.net')
    #    new_group_users.append(new_name)

    n.add_users_to_group(new_group_users, group)
print(u"Finished users into groups")
# Create all of the projects
print(u"Started projects")
projects = o.query_projects()
proj_dict = o.convert_xml_list_to_name_id_dict(projects)
for proj in projects:
    if proj.get(u'name') == u'Default':
        continue
    proj_request = o.build_request_from_response(proj)

    # With the possibility of parent projects, you actually want to add all projects FIRST,
    # then add in the parent relationships. Otherwise you might try to add a parent before it exists on the new site
    # This removes the parentProjectId attribute from the new request
    for p in proj_request:
        if p.get(u'parentProjectId') is not None:
            del(p.attrib[u'parentProjectId'])

    n.create_project(direct_xml_request=proj_request, no_return=True)

# Let the projects get all settled in
print(u"Finished groups, sleeping for a few moments")
time.sleep(4)

print(u"Starting parent project assignment")
# Now Assign projects to their parents if they have one
# We'll use XPath to only grab elements that have a parentProjectId
child_projects = projects.findall(u'.//t:project[@parentProjectId]', n.ns_map)
for proj in child_projects:
    # Double check it's working correctly
    if proj.get(u'parentProjectId') is not None:
        # Get the Name of the Project that is the Parent
        o_parent_project = o.query_project_xml_object(proj.get(u'parentProjectId'))
        new_parent_project_name = o_parent_project.get(u'name')
        n.update_project(proj.get(u'name'),
                         parent_project_name_or_luid=new_parent_project_name)


# Let the project updates get all settled in
print(u'Finished parent project assignment, sleeping for a few moments')
time.sleep(4)

# Set Permissions for all the Projects to Match when usernames and group names perfectly match between the systems
print(u'Starting project permissions')
for proj_name in proj_dict:
    orig_proj = o.query_project(proj_name)
    new_proj = n.query_project(proj_name)

    # If you are transferring where the usernames may vary (say to Online where all usernames are e-mail addresses
    # must come up with a mechanism for mapping the username.
    users_mapping = None
    # Create a username_map dict to pass like {'original_username', : 'new_username'}.
    # Uncomment the following if necessary:
    # users_mapping = { 'username' : 'username@domain.net', 'admin' : 'admin@domain.net' }

    new_proj.replicate_permissions_direct_xml(orig_proj, username_map=users_mapping)

print(u'Finished project permissions')

# Migrate Schedules BEFORE data sources and workbooks, so that you can update their Extract and Subscriptions at the
# time you publish them

# Schedules
# If migrating to Tableau Online, comment out the whole following section,
# you'll need to do a mapping when replicating subscriptions

print(u'Starting schedules')
# Schedules are Server wide, so you must be a Server admin to sync them
# You probably don't want to override existing schedules on the new Server
# So you might comment this whole thing out, or do a name check like:
existing_schedules_new_server = n.query_schedules()

# Let's assume a quick name match is enough to know whether to bother to add
existing_schedules_dict = n.convert_xml_list_to_name_id_dict(existing_schedules_new_server)

schedules = o.query_schedules()

# element_luid : new_schedule_luid
wb_extract_tasks = {}
ds_extract_tasks = {}

for sched in schedules:
    # Check if a Schedule with this name already exists
    if sched.get(u'name') in existing_schedules_dict:
        print(u'Skipping Schedule {}, a schedule with this name already exists on server'.format(sched.get(u'name')))
        final_sched_luid = existing_schedules_dict[sched.get(u'name')]

    else:
        sched_request = n.build_request_from_response(sched)
        final_sched_luid = n.create_schedule(direct_xml_request=sched_request)

    # Now pull all of the Extract Refresh tasks for each schedule, so that you can assign them to the newly published
    # workbooks and datasources when you do that
    if sched.get(u'type') == u'Extract':
        extracts_for_o_schedule = o.query_extract_refresh_tasks_by_schedule(sched.get(u'id'))
        for extract_task in extracts_for_o_schedule:
            # this will be either workbook or datasource tag
            for element in extract_task:
                if element.tag.find(u'datasource') != -1:
                    ds_extract_tasks[element.get(u'id')] = final_sched_luid
                elif element.tag.find(u'workbook') != -1:
                    wb_extract_tasks[element.get(u'id')] = final_sched_luid

print(u'WB Refresh tasks found:')
print(wb_extract_tasks)
print(u'DS Refresh Tasks found:')
print(ds_extract_tasks)
print(u'Finished schedules, sleeping for a few moments')

time.sleep(4)

# Workbooks
# This is all based on the replicate_workbooks_with_published_dses method in the template_publish_sample.py example
# Because of published data sources, it makes the most sense to replicate workbooks first and also whatever published
# data sources happen to be connected to them, then loop back around later for any stray data sources

# Keep track of datasources that are republished to not replicate effort later
replicated_datasources_luid_list = []


# Simple object to keep all the details this requires straight
class PublishedDSInfo:
    def __init__(self, orig_content_url):
        self.orig_content_url = orig_content_url
        self.orig_luid = None
        self.orig_name = None
        self.orig_proj_name = None
        self.new_luid = None
        self.new_content_url = None


# Determine which published data sources to copy across
# Do in a first step in case multiple workbooks are connected to the same
# set of data sources
orig_ds_content_url = {}
error_wbs = []



# Go Project by Project because workbook names are unique within Projects at least
o_projects = o.query_projects()
o_proj_dict = o.convert_xml_list_to_name_id_dict(o_projects)
for proj in o_proj_dict:
    # use a LUID lookup through o_proj_dict[proj] to reduce name lookups
    print(u'Starting workbooks in project {}'.format(proj))
    wbs_in_proj = o.query_workbooks_in_project(o_proj_dict[proj])
    workbook_name_or_luids = o.convert_xml_list_to_name_id_dict(wbs_in_proj)

    # which is necessary to make sure there is no name duplication
    wb_files = {}

    # Go through all the workbooks you'd identified to publish over
    for wb in workbook_name_or_luids:
        try:

            # We need the workbook downloaded even if there are no published data sources
            # But we have to open it up to see if there are any
            wb_filename = o.download_workbook(wb_name_or_luid=wb, filename_no_extension=wb,
                                              proj_name_or_luid=o_proj_dict[proj])
            wb_files[wb] = wb_filename
            # Open up the file using the tableau_documents sub-module to find out if the
            # data sources are published. This is easier and more exact than using the REST API
            wb_obj = TableauFile(wb_filename, logger)
            dses = wb_obj.tableau_document.datasources  # type: list[TableauDatasource]
            for ds in dses:
                # Add any published datasources to the dict with the object to hold the details
                if ds.published is True:
                    orig_ds_content_url[ds.published_ds_content_url] = PublishedDSInfo(ds.published_ds_content_url)
        except NoMatchFoundException as e:
            logger.log(u'Could not find a workbook with name or luid {}, skipping'.format(wb))
            error_wbs.append(wb)
        except MultipleMatchesFoundException as e:
            logger.log(u'wb {} had multiple matches found, skipping'.format(wb))
            error_wbs.append(wb)

    print(u"Found {} published data sources to move over".format(len(orig_ds_content_url)))

    # Look up all these data sources and find their LUIDs so they can be downloaded
    all_dses = o.query_datasources()

    for ds_content_url in orig_ds_content_url:
        print(ds_content_url)

        ds_xml = all_dses.findall(u'.//t:datasource[@contentUrl="{}"]'.format(ds_content_url), o.ns_map)
        if len(ds_xml) == 1:
            orig_ds_content_url[ds_content_url].orig_luid = ds_xml[0].get(u'id')
            orig_ds_content_url[ds_content_url].orig_name = ds_xml[0].get(u'name')

            for element in ds_xml[0]:
                if element.tag.find(u'project') != -1:
                    orig_ds_content_url[ds_content_url].orig_proj_name = element.get(u'name')
                    break
        else:
            # This really shouldn't be possible, so you might want to add a break point here
            print(u'Could not find matching datasource for contentUrl {}'.format(ds_content_url))

    print(u'Finished finding all of the info from the data sources')

    # Download those data sources and republish them
    # You need the credentials to republish, as always

    dest_project = n.query_project(proj)

    for ds in orig_ds_content_url:
        ds_filename = o.download_datasource(orig_ds_content_url[ds].orig_luid, u'downloaded ds')
        proj_obj = n.query_project(orig_ds_content_url[ds].orig_proj_name)

        ds_obj = TableauFile(ds_filename)
        ds_dses = ds_obj.tableau_document.datasources  # type: list[TableauDatasource]
        # You may need to change details of the data source connections here
        # Uncomment below if you have things to change
        credentials = None
        for ds_ds in ds_dses:
            for conn in ds_ds.connections:
        # Change the dbname is most common
                # Credential mapping example, could be much more full
                if conn.server.find(u'servername') != -1:
                    credentials = {u'username': u'uname', u'password': u'pword'}

        new_ds_filename = ds_obj.save_new_file(u'Updated Datasource')

        # Here is also where any credential mapping would need to happen, because credentials can't be retrieved
        if credentials is not None:
            orig_ds_content_url[ds].new_luid = n.publish_datasource(new_ds_filename, orig_ds_content_url[ds].orig_name,
                                                                    proj_obj, overwrite=True,
                                                                    connection_username=credentials[u'username'],
                                                                    connection_password=credentials[u'password'])
        else:
            orig_ds_content_url[ds].new_luid = n.publish_datasource(new_ds_filename, orig_ds_content_url[ds].orig_name,
                                                                    proj_obj, overwrite=True)
        print(u'Published data source, resulting in new luid {}'.format(orig_ds_content_url[ds].new_luid))

        # Add to an Extract Schedule if one was scheduled
        if orig_ds_content_url[ds].orig_luid in ds_extract_tasks:
            n.add_datasource_to_schedule(ds_name_or_luid=orig_ds_content_url[ds].new_luid,
                                         schedule_name_or_luid=ds_extract_tasks[orig_ds_content_url[ds].orig_luid])

        # Add to the list that don't need to be republished
        replicated_datasources_luid_list.append(orig_ds_content_url[ds].orig_luid)

        # Clean the file
        os.remove(new_ds_filename)

        try:
            new_ds = n.query_datasource(orig_ds_content_url[ds].new_luid)
            orig_ds_content_url[ds].new_content_url = new_ds[0].get(u'contentUrl')
            print(u'New Content URL is {}'.format(orig_ds_content_url[ds].new_content_url))
        except RecoverableHTTPException as e:
            print(e.tableau_error_code)
            print(e.http_code)
            print(e.luid)

    print(u'Finished republishing all data sources to the new site')

    # Now that you have the new contentUrls that map to the original ones,
    # and you know the DSes have been pushed across, you can open up the workbook and
    # make sure that all of the contentUrls are correct
    for wb in wb_files:
        t_file = TableauFile(wb_files[wb], logger_obj=logger)
        dses = t_file.tableau_document.datasources  # type: list[TableauDatasource]
        for ds in dses:
            if ds.published is True:
                # Set the Site of the published data source
                ds.published_ds_site = new_site_content_url

                o_ds_content_url = ds.published_ds_content_url
                if o_ds_content_url in orig_ds_content_url:
                    ds.published_ds_content_url = orig_ds_content_url[o_ds_content_url].new_content_url
            # If the datasources AREN'T published, then you may need to change details directly here
            else:
                print(u'Not a published data source')
            #    for conn in ds.connections:
            # Change the dbname is most common
            # conn.dbname = u'prod'
            # conn.port = u'10000'

        temp_wb_file = t_file.save_new_file(u'Modified Workbook'.format(wb))

        # Map any credentials for embedded datasources in the workbook here as well

        new_workbook_luid = n.publish_workbook(workbook_filename=temp_wb_file, workbook_name=wb,
                                               project_obj=dest_project,
                                               overwrite=True, check_published_ds=False)
        print(u'Published new workbook {}'.format(new_workbook_luid))
        os.remove(temp_wb_file)

        # Add to an Extract Schedule if one was scheduled
        o_wb_luid = workbook_name_or_luids[wb]
        if o_wb_luid in wb_extract_tasks:
            print(u'Adding to Extract Refresh Schedule')
            n.add_workbook_to_schedule(wb_name_or_luid=new_workbook_luid,
                                       schedule_name_or_luid=wb_extract_tasks[o_wb_luid])

    print(u'Finished workbooks for project {}'.format(proj))
print(u'Finished publishing all workbooks')


# Published Datasources that were not attached to a workbook
# Note -- data sources with embedded data credentials must have them supplied again at this publish time
# Must publish Data Sources

# Look up all the data sources, go project by project to avoid naming issues
o_projects = o.query_projects()
proj_dict = o.convert_xml_list_to_name_id_dict(o_projects)
for proj in proj_dict:

    all_dses_in_proj = o.query_datasources(project_name_or_luid=proj_dict[proj])
    all_dses_in_proj_dict = o.convert_xml_list_to_name_id_dict(all_dses_in_proj)
    for ds_name in all_dses_in_proj_dict:
        o_ds_luid = all_dses_in_proj_dict[ds_name]
        # Only publish data sources that had not been moved over (unused data sources)
        if o_ds_luid not in replicated_datasources_luid_list:
            # Download data source and republish
            ds_filename = o.download_datasource(o_ds_luid, u'downloaded ds')
            proj_obj = n.query_project(proj_dict[proj])

            ds_obj = TableauFile(ds_filename)
            ds_dses = ds_obj.tableau_document.datasources  # type: list[TableauDatasource]
            # You may need to change details of the data source connections here
            # Uncomment below if you have things to change
            # for ds_ds in ds_dses:
            #    for conn in ds_ds.connections:
            # Change the dbname is most common
            # conn.dbname = u'prod'
            # conn.port = u'10000'

            new_ds_filename = ds_obj.save_new_file(u'Updated Datasource')

            # Here is also where any credential mapping would need to happen, because credentials can't be retrieved

            new_ds_luid = n.publish_datasource(new_ds_filename, ds_name, proj_obj, overwrite=True)
            print(u'Published data source, resulting in new luid {}'.format(new_ds_luid))

            # Add to an Extract Schedule if one was scheduled
            if o_ds_luid in ds_extract_tasks:
                n.add_datasource_to_schedule(ds_name_or_luid=new_ds_luid,
                                             schedule_name_or_luid=ds_extract_tasks[o_ds_luid])


# Subscriptions
# It is easy to replicate subscription to a whole workbook but this is the amount of work it would take for a view:
# (1) Get the view ID, then look up the workbook it belongs to
# (2) Query the views for that workbook on the NEW site
# (3) Query the view for the workbook on the original site
# (4) Do a text name match between the Views on both sites to get the new site view luid
# (5) Subscribe to that view luid on the new site

# Is this possible? Yes. It it worth it? I'm not sure


print(u'Starting subscriptions')
subscriptions = o.query_subscriptions()

# Subscriptions actually require 3 different IDs to line up --
# the content (workbook or view), the user and the subscription
# Everything must be replicated over

# If migrating to Tableau Online, build a translation dictionary for the original schedule name and the new schedule name

# If changing names from one site to another, user you mapping dictionary here instead of querying for users
#n_users = n.query_users()
#n_users_dict = n.convert_xml_list_to_name_id_dict(n_users)


#for sub in subscriptions:
#    sub_request = n.build_request_from_response(sub)

    #n.create_subscription()



# Alerts: Currently not replicable
# Only available in API 3.2 and later
# No way to create an equivalent data driven alert, because while you can get the existence of an Alert from the
# originating site, there is no method to ADD a data driven alert to the new site, only to Add a User to an
# existing Data Driven Alert
