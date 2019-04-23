# -*- coding: utf-8 -*-

from tableau_tools.tableau_rest_api import *
from tableau_tools import *

# This class is designed to take a single TableauRestApiConnectionNN object and then swaps around the sign-in tokens
# as necessary

class RestConnectionsManager():

    def __init__(self, rest_connection_object):
        """
        :type rest_connection_object: TableauRestApiConnection32
        """
        # Each site should have a "master token" for signing in as the impersonated user the first time
        self.site_master_tokens = {}  # site : token
        # This collection then holds any tokens from an individual user's session on a given site
        self.site_user_tokens = {}  # site : { username : token }

        self.rest_connection = rest_connection_object  # type: TableauRestApiConnection32

        self.connection_signed_in = False


    def _sign_in_error(self):
        print("Tableau Server REST API Service unreachable")
        self.connection_signed_in = False
        return False

    def _sign_in(self):
        self.rest_connection.signin()
        self.default_connection_token = self.rest_connection.token
        return True

    # This command can sign in from the very beginning if necessary
    def sign_in_connection_object(self):
        # This is a failure within this code, not a failure to reach the Tableau Server and sign in
        if self.rest_connection is None:
            raise NotSignedInException()

        # Try to sign in to the Tableau Server REST API
        try:
            return self._sign_in()

        # Trying all these exception types than capturing anything, but probably should figure exactly what is wrong
        except NotSignedInException as e:
            try:
                return self._sign_in()
            except:
                return self._sign_in_error()
        # Try to sign-in again?
        except RecoverableHTTPException as e:
            try:
                return self._sign_in()
            except:
                return self._sign_in_error()
        # Should be capturing requests ConnectionError exception
        except Exception as e:
            try:
                return self._sign_in()
            except:
                return self._sign_in_error()


    def sign_in_site_master(self, site_content_url):
        self.rest_connection.token = None
        self.rest_connection.site_content_url = site_content_url
        try:
            self.rest_connection.signin()
        except:
            try:
                self.rest_connection.signin()
            except:
                raise

        # Now grab that token
        self.site_master_tokens[site_content_url] = self.rest_connection.token

        # If no exist site
        if site_content_url not in self.site_user_tokens:
            self.site_user_tokens[site_content_url] = {}

        # And reset back to the default
        self.rest_connection.token = self.default_connection_token
        return True

    # All this check is if a user token exists
    def check_user_token(self, username, site_content_url):
        # Has the site been signed into before? If not, create it
        if site_content_url not in self.site_master_tokens.keys():
            # If the site has no master token, create it
            # But we're keeping the same connection object, to limit the total number of tokens
            self.sign_in_site_master(site_content_url)

            # Also create an entry in the users dict for this vertical. The check is probably unnecessary but why not
            if site_content_url not in self.site_user_tokens.keys():
                self.site_user_tokens[site_content_url] = {}
            # No user token can exist if nothing even existed on that site yet
            return False
        # Do they have an entry?
        elif username in self.site_user_tokens[site_content_url].keys():
            # Now check if a token exists
            if self.site_user_tokens[site_content_url][username] is None:
                return False
            else:
                return True
        # Didn't find them, no connection
        else:
            return False


    def create_user_connection(self, username, site_content_url):
        # Swap to the master session for the vsite to get the user luid
        self.rest_connection.token = self.site_master_tokens[site_content_url]
        self.rest_connection.site_content_url = site_content_url
        try:
            user_luid = self.rest_connection.query_user_luid(username)

        except:
            # Retry at least once
            try:
                user_luid = self.rest_connection.query_user_luid(username)
            except:
                # Assume something wrong with the site_master token, create new session
                try:
                    self.sign_in_site_master(site_content_url)
                    self.rest_connection.token = self.site_master_tokens[site_content_url]
                    user_luid = self.rest_connection.query_user_luid(username)
                except:
                    # Maybe another check here?
                    raise


        # Now blank the connection token so you can sign in
        self.rest_connection.token = None
        try:
            self.rest_connection.signin(user_luid)
            self.site_user_tokens[username] = self.rest_connection.token
            # Should this be exception instead of return of True?
            return True
        # This is bad practice to capture any exception, improve when we know what a "user not found" looks like

        except:
            # Try one more time in case it is connection issue
            try:
                self.rest_connection.signin(user_luid)
                self.site_user_tokens[username] = self.rest_connection.token
            except:
                # Should this be exception instead of return of True?
                return False

    def switch_user_and_site(self, username, site_content_url):
        user_exists = self.check_user_token(username, site_content_url)
        if user_exists is False:
            # Create the connection
            self.create_user_connection(username, site_content_url)
        elif user_exists is True:
            # This token could be out of date, but test for that exception when you try to run a command
            self.rest_connection.token = self.site_user_tokens[site_content_url][username]
            self.rest_connection.site_content_url = site_content_url
        else:
            raise Exception()

    def switch_to_site_master(self, site_content_url):
        if site_content_url not in self.site_master_tokens.keys():
            self.sign_in_site_master(site_content_url)
        self.rest_connection.token = self.site_master_tokens[site_content_url]
        self.rest_connection.site_content_url = site_content_url


# Connect to the default site to bootstrap the process

server = ""
master_default_username = ""
master_default_password = ""
default_site_content_url = ""
d = TableauRestApiConnection32(server=server, username=master_default_username, password=master_default_password,
                               site_content_url=default_site_content_url)

# This manages all the connections here on out
connections = RestConnectionsManager(rest_connection_object=d)
connections.sign_in_connection_object()

# Examples of using the connection manager
connections.switch_user_and_site("some_user", "site_a")
my_projects = connections.rest_connection.query_projects()
my_projects_dict = connections.rest_connection.convert_xml_list_to_name_id_dict(my_projects)
print(my_projects_dict)

# Now switching to a different user
connections.switch_user_and_site("some_other_user", "site_b")
my_projects = connections.rest_connection.query_projects()
my_projects_dict = connections.rest_connection.convert_xml_list_to_name_id_dict(my_projects)
print(my_projects_dict)

#
# Simple implementation for single site
#

user_rest_connections = {}
master_username = 'site_admin'
master_password = 'hackm3'
site_content_url = "mysite"
m = TableauRestApiConnection32(server=server, username=master_username, password=master_password,
                               site_content_url=site_content_url)
m.signin()

user_to_impersonate_1 = 'user_a'
user_luid = m.query_user_luid(user_to_impersonate_1)
user_rest_connections[user_to_impersonate_1] = TableauRestApiConnection32(server=server, username=master_username,
                                                                          password=master_password,
                                                                          site_content_url=site_content_url)

user_rest_connections[user_to_impersonate_1].signin(user_luid)

user_to_impersonate_2 = 'user_b'
user_luid = m.query_user_luid(user_to_impersonate_2)
user_rest_connections[user_to_impersonate_2] = TableauRestApiConnection32(server=server, username=master_username,
                                                                          password=master_password,
                                                                          site_content_url=site_content_url)
user_rest_connections[user_to_impersonate_2].signin(user_luid)
