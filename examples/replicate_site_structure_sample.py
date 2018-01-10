# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import urllib2
import time

server = u'http://127.0.0.1'
username = u''
password = u''

original_content_url = u'test_site'
new_site_name = u'Test Site Replica'
new_site_content_url = u'test_site_replica'

o = TableauRestApiConnection25(server, username, password, original_content_url)
o.signin()
logger = Logger(u'replicate_site_sample.log')
# Enable logging after sign-in to hide credentials
o.enable_logging(logger)

try:
    o.create_site(new_site_name, new_site_content_url)
except AlreadyExistsException:
    print e.msg
    print u"Cannot create new site, it already exists"
    exit()

n = TableauRestApiConnection25(server, username, password, new_site_content_url)
n.signin()
n.enable_logging(logger)

# Replicate Groups
groups = o.query_groups()
groups_dict = o.convert_xml_list_to_name_id_dict(groups)
for group in groups_dict:
    # Can't add All Users because it is generated
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

# Replicate Projects
projects = o.query_projects()
proj_dict = o.convert_xml_list_to_name_id_dict(projects)
for proj in proj_dict:
    if proj == u'Default':
        continue
    # This assumes locked permissions and no description. Can iterate over objects if you need some of that
    n.create_project(proj, no_return=True)


    n.create_project()

# Let the projects get all settled in
time.sleep(4)

# Set Permissions for all the Projects to Match
for proj_name in proj_dict:
    orig_proj = o.query_project(proj_name)
    new_proj = n.query_project(proj_name)
    new_proj.replicate_permissions(orig_proj)

# Bring down all datasources
# Note -- data sources with embedded data credentials must have them supplied again at this publish time
