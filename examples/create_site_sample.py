# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *
import time

server = u'http://127.0.0.1'
username = u''
password = u''

new_site_content_url = u'test_site'
new_site_name = u'Sample Test Site'

# Choose the API version for your server. 10.2 = 25
default = TableauRestApiConnection26(server, username, password)
try:
    default.signin()
    default.create_site(new_site_name, new_site_content_url)
except AlreadyExistsException as e:
    print e.msg
    print u"Cannot create new site, it already exists"
    exit()

time.sleep(4)


t = TableauRestApiConnection26(server, username, password, new_site_content_url)
t.signin()
logger = Logger(u'create_site_sample.log')
# Enable logging after sign-in to hide credentials
t.enable_logging(logger)

# Create some groups
groups_to_create = [u'Administrators', u'Executives', u'Managers', u'Line Level Workers']

for group in groups_to_create:
    t.create_group(group)

# Remove all permissions from Default Project
time.sleep(4)
default_proj = t.query_project(u'Default')
default_proj.lock_permissions()

default_proj.clear_all_permissions() # This clears all, including the defaults


# Add in any default permissions you'd like at this point
admin_perms = default_proj.create_project_permissions_object_for_group(u'Administrators', role=u'Project Leader')
default_proj.set_permissions_by_permissions_obj_list([admin_perms, ])

admin_perms = default_proj.create_workbook_permissions_object_for_group(u'Administrators', role=u'Editor')
admin_perms.set_capability(u'Download Full Data', u'Deny')
default_proj.workbook_defaults.set_permissions_by_permissions_obj_list([admin_perms, ])

admin_perms = default_proj.create_datasource_permissions_object_for_group(u'Administrators', role=u'Editor')
default_proj.datasource_defaults.set_permissions_by_permissions_obj_list([admin_perms, ])

# Change one of these
new_perms = default_proj.create_project_permissions_object_for_group(u'Administrators', role=u'Publisher')
default_proj.set_permissions_by_permissions_obj_list([new_perms, ])

# Create Additional Projects
projects_to_create = [u'Sandbox', u'Data Source Definitions', u'UAT', u'Finance', u'Real Financials']
for project in projects_to_create:
    t.create_project(project, no_return=True)

# Set any additional permissions on each project

# Add Users
users_to_add = [u'user_1', u'user_2', u'user_3']
for user in users_to_add:
    t.add_user(user, user, u'Publisher')

time.sleep(3)
# Add Users to Groups
t.add_users_to_group(u'user_1', u'Managers')
t.add_users_to_group(u'user_2', u'Administrators')
t.add_users_to_group([u'user_2', u'user_3'], u'Executives')
