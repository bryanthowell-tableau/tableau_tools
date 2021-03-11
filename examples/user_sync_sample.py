# This is example code showing a sync process from a database with users in it
# It uses psycopg2 to connect to a PostgreSQL database
# You can substitute in any source of the usernames and groups
# The essential logic is that you do a full comparison on who should exist and who doesn't, and both add and remove


import psycopg2.extensions
import psycopg2

from  tableau_tools import *

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

conn = psycopg2.connect(host='', database='', user='', password='')
logger = Logger('')

username = ''
password = ''
server = 'http://'

t = TableauServerRest33(server=server, username=username, password=password, site_content_url='default')
t.enable_logging(logger)
t.signin()

# Connect to the DB
cur = conn.cursor()

# Create Groups
sql_statement = 'SELECT groups FROM permissions GROUP BY groups;'
cur.execute(sql_statement)

# Get all the groups on the Tableau Server
groups = t.query_groups()
groups_dict = t.xml_list_to_dict(groups)

# Loop through the results
for row in cur:
    if row[0] not in groups_dict:
        print('Creating group {}'.format(row[0]))
        luid = t.groups.create_group(group_name=row[0])
        groups_dict[row[0]] = luid

print(groups_dict)

# Create all

# Sync the users themselves
sql_statement = 'SELECT user_id, user_name, groups FROM permissions'
cur.execute(sql_statement)

# Get all the users on the site
users = t.users.query_users()
users_dict = t.xml_list_to_dict(users)

# Loop through users, make sure they exist
for row in cur:
    if row[0] not in users_dict:
        print('Creating user {}'.format(row[0].encode('utf8')))
        luid = t.users.add_user(username=row[0], fullname=row[1], site_role='Publisher')
        users_dict[row[0]] = luid

print(users_dict)

# Create projects for each user
for user in users_dict:
    proj_obj = t.projects.create_project("My Saved Reports - {}".format(user))
    user_luid = users_dict[user]
    perms_obj = proj_obj.get_permissions_obj(username_or_luid=user_luid, role='Publisher')
    proj_obj.set_permissions([perms_obj, ])


# Reset back to beginning to reuse query
cur.scroll(0, mode='absolute')

# For the check of who shouldn't be on the server
groups_and_users = {}

# List of usernames who should be in the system
usernames = {}
# Add users who are missing from a group
for row in cur:
    user_luid = users_dict.get(row[0])
    group_luid = groups_dict.get(row[2])

    usernames[row[0]] = None

    # Make a data structure where we can check each group that exists on server
    if groups_and_users.get(group_luid) is None:
        groups_and_users[group_luid] = []
    groups_and_users[group_luid].append(user_luid)

    print('Adding user {} to group {}'.format(row[0].encode('utf8'), row[2].encode('utf8')))
    t.groups.add_users_to_group(username_or_luid_s=user_luid, group_name_or_luid=group_luid)

# Determine if any users are in a group who do not belong, then remove them
for group_luid in groups_and_users:
    if group_luid == groups_dict['All Users']:
        continue
    users_in_group_on_server = t.groups.query_users_in_group(group_luid)
    users_in_group_on_server_dict = t.xml_list_to_dict(users_in_group_on_server)
    # values() are the LUIDs in these dicts
    for user_luid in list(users_in_group_on_server_dict.values()):
        if user_luid not in groups_and_users[group_luid]:
            print('Removing user {} from group {}'.format(user_luid, group_luid))
            t.groups.remove_users_from_group(username_or_luid_s=user_luid, group_name_or_luid=group_luid)

# Determine if there are any users who are in the system and not in the database, set them to unlicsened
users_on_server = t.users.query_users()
for user_on_server in users_on_server:
    # Skip the guest user
    if user_on_server.get("name") == 'guest':
        continue
    if user_on_server.get("name") not in usernames:
        if user_on_server.get("siteRole") not in ['ServerAdministrator', 'SiteAdministrator']:
            print(('User on server {} not found in security table, set to Unlicensed'.format(user_on_server.get("name").encode('utf8'))))
            # Just set them to 'Unlicensed'
            t.users.update_user(username_or_luid=user_on_server.get("name"), site_role='Unlicensed')
