from tableau_tools import *
import datetime

# This script is to show how to add or modify a user prior to SSO
# The use case is for passing in properties to a user session in a secure way
# When using Restricted (standard) Trusted Tickets, a user cannot see or modify their Full Name property,
# which allows it to be repurposed as a stored of secure values set programmatically
#
# If you have Core based licensing, and no need to track users beyond just that momentary session, you can also
# create usernames at will and include details on the username. This could also be useful if you use SAML or Unrestricted
# trusted tickets (where the user can change the Full Name property,
# although you do lose out on schedules and customization and such due to the transient nature of those sessions

server_url = ""
username = ""
password = ""
site_content_url = ""

t_server = TableauServerRest35(server=server_url, username=username, password=password,
                        site_content_url=site_content_url)
t_server.signin()

def update_user_with_fullname_properties(tableau_server: TableauServerRest, username: str,
                            properties_on_fullname: List[str], delimiter: str):
    t = tableau_server
    final_fullname = delimiter.join(properties_on_fullname)
    t.users.update_user(username_or_luid=username, full_name=final_fullname)

def create_a_temporary_user(tableau_server: TableauServerRest, username: str, other_properties_on_username: List[str],
                            properties_on_fullname: List[str], delimiter: str) -> str:
    t = tableau_server
    if len(other_properties_on_username) > 0:
        full_list = [username, ]
        full_list.extend(other_properties_on_username)
        final_username = delimiter.join(full_list)
    else:
        final_username = username
    final_fullname = delimiter.join(properties_on_fullname)
    new_user_luid = t.users.add_user(username=final_username, fullname=final_fullname, site_role='Viewer')
    # Because we might be storing properties on the username, return it back to whatever is then going to SSO the user
    return final_username

# Typically we don't delete users from Tableau Server, but just set them to Unlicensed.
# See delete_old_unlicensed_users for a pattern for actually deleting them based on some filter criteria
def unlicense_a_temporary_user(tableau_server: TableauServerRest, username: str):
    t = tableau_server
    t.users.unlicense_users(username_or_luid_s=username)

# This logic gets actually removes any licensed user who didn't log in today.
# Could be lengthened out for a long-term type cleanup script
def delete_old_unlicensed_users(tableau_server: TableauServerRest):
    t = tableau_server
    today = datetime.datetime.now()
    offset_time = datetime.timedelta(days=1)
    time_to_filter_by = today - offset_time
    last_login_filter = t.url_filters.get_last_login_filter('lte', time_to_filter_by)
    site_role_f = t.url_filters.get_site_role_filter('Unlicensed')
    unlicensed_users_from_before_today = t.users.query_users(last_login_filter=last_login_filter,
                                                             site_role_filter=site_role_f)
    users_dict = t.xml_list_to_dict(unlicensed_users_from_before_today)
    # users_dict is username : luid, so you just need the values but must cast to a list
    t.users.remove_users_from_site(list(users_dict.values()))