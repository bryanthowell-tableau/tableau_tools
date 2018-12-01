# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import time

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
# Enable logging after sign-in to hide credentials
o.enable_logging(logger)

# Sign in to the new Server on default as a Server Admin
n_default = TableauRestApiConnection31(server=n_server, username=n_username,
                                       password=n_password, site_content_url=u"default")
n_default.signin()
n_default.enable_logging(logger)

# Die if the new site already exists
try:
    n_default.create_site(new_site_name, new_site_content_url)
except AlreadyExistsException as e:
#    print(e.msg)
#    print(u"Cannot create new site, it already exists. Exiting")
#    exit()
    # Alternative pathway blows away the existing site if it finds one with that site_content_url
    n_existing_to_replace = TableauRestApiConnection31(server=n_server, username=n_username,
                                                       password=n_password, site_content_url=new_site_content_url)
    n_existing_to_replace.signin()
    n_existing_to_replace.enable_logging(logger)
    n_existing_to_replace.delete_current_site()
    # Now Create the new site
    n_default.create_site(new_site_name, new_site_content_url)
n_default.signout()


# Connect to the newly created site
n = TableauRestApiConnection31(server=n_server, username=n_username,
                               password=n_password, site_content_url=new_site_content_url)
n.signin()
n.enable_logging(logger)

# Now we start replicating from one site to the other

# Replicate Groups first, so you can put users in them
groups = o.query_groups()
for group in groups:
    # Can't add All Users because it is generated automatically
    if group.get(u'name') == u'All Users':
        continue

    n.create_group(direct_xml_request=n.build_request_from_response(group))

# Replicate Users
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
time.sleep(4)

# Put users in groups

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

# Create all of the projects
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


# Let the projects get all settled in
time.sleep(4)

# Set Permissions for all the Projects to Match when usernames and group names perfectly match between the systems
for proj_name in proj_dict:
    orig_proj = o.query_project(proj_name)
    new_proj = n.query_project(proj_name)
    new_proj.replicate_permissions_direct_xml(orig_proj)

# If you are transferring where the usernames may vary (say to Online where all usernames are e-mail addresses
# must come up with a mechanism for

# Datasources
# Note -- data sources with embedded data credentials must have them supplied again at this publish time
# Must publish Data Sources

# Workbooks

# Schedules
# Schedules are Server wide, so you must be a Server admin to sync them
# You probably don't want to override existing schedules on the new Server

# If migrating to Tableau Online, comment out this section
#schedules = o.query_schedules()
#for sched in schedules:


# Subscriptions
# If migrating to Tableau Online, build a translation dictionary for the

# Alerts
