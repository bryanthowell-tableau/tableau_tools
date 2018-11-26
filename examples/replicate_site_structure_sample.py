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
o = TableauRestApiConnection28(server=o_server, username=o_username,
                               password=o_password, site_content_url=original_content_url)
o.signin()
logger = Logger(u'replicate_site_sample.log')
# Enable logging after sign-in to hide credentials
o.enable_logging(logger)

# Sign in to the new Server on default as a Server Admin
n_default = TableauRestApiConnection28(server=n_server, username=n_username,
                               password=n_password, site_content_url=u"default")
n_default.signin()
n_default.enable_logging(logger)

# Die if the new site already exists
try:
    n_default.create_site(new_site_name, new_site_content_url)
except AlreadyExistsException as e:
    print(e.msg)
    print(u"Cannot create new site, it already exists. Exiting")
    exit()
n_default.signout()

# Connect to the newly created site
n = TableauRestApiConnection28(server=n_server, username=n_username,
                               password=n_password, site_content_url=new_site_content_url)
n.signin()
n.enable_logging(logger)

# Now we start replicating from one site to the other

# Replicate Groups first, so you can put users in them
groups = o.query_groups()
groups_dict = o.convert_xml_list_to_name_id_dict(groups)
for group in groups_dict:
    # Can't add All Users because it is generated automatically
    if group == u'All Users':
        continue
    n.create_group(group)

# Replicate Users
users = o.query_users()
# Loop through the Element objects themselves because of more details to add
for user in users:
    # Can't add the Guest user
    if user.get(u'name') == u'guest':
        continue
    # Can't add a server Admin
    if user.get(u'siteRole') == u'ServerAdministrator':
        continue
    n.add_user(user.get(u'name'), user.get(u'fullName'), user.get(u'siteRole'))

# Sleep a bit to let them all get added in
time.sleep(4)

# Put users in groups
for group in groups_dict:
    if group == u'All Users':
        continue
    group_users = o.query_users_in_group(group)
    group_users_dict = o.convert_xml_list_to_name_id_dict(group_users)
    n.add_users_to_group(group_users_dict, group)

# Create all of the projects
projects = o.query_projects()
proj_dict = o.convert_xml_list_to_name_id_dict(projects)
for proj in projects:
    if proj.get(u'name') == u'Default':
        continue
    # This assumes locked permissions and no description. Can iterate over objects if you need some of that
    n.create_project(proj.get(u'name'), proj.get(u'description'), proj.get(u'contentPermissions'),
                     publish_samples=False, no_return=True)

# Assign projects to their parents if they have one
for proj in projects:
    if proj.get(u'parentProjectId') is not None:
        # Get the Name of the Project that is the Parent
        o_parent_project = o.query_project(proj.get(u'parentProjectId'))
        new_parent_project_name = o_parent_project.get_xml_obj().get(u'name')
        n.update_project(proj.get(u'name'),
                         parent_project_name_or_luid=new_parent_project_name)


# Let the projects get all settled in
time.sleep(4)

# Set Permissions for all the Projects to Match
for proj_name in proj_dict:
    orig_proj = o.query_project(proj_name)
    new_proj = n.query_project(proj_name)
    new_proj.replicate_permissions(orig_proj)

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

