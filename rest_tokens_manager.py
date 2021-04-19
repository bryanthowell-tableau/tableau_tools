from .tableau_exceptions import NotSignedInException, RecoverableHTTPException
from .logging_methods import LoggingMethods
from typing import Dict, List
#
# RestConnectionsManager class handles all of the session tokens, so that a single connection object
# can send the requests for any user who is active
# Eventually the token data structure could be put in the cache and shared, so that
# multiple running processes could exist handling lots of traffic but sharing the session tokens
#
class RestTokensManager(LoggingMethods):
    # The tokens manager is separate from the connection object
    # It is not static, but all of its methods act UPON a REST Connection object, so you have to pass
    # that Rest Connection object in for every method

    # Then you define what you set in the constructor (when you create) here in the __init__
    # Must always start with self (the equivalent of "this" in Python)
    def __init__(self):

        # Each site should have a "master token" for signing in as the impersonated user the first time
        self.site_master_tokens = {}  # site : token
        # This collection then holds any tokens from an individual user's session on a given site
        self.site_user_tokens = {}  # site : { username : token }

        self.default_connection_token = None
        self.connection_signed_in = False

        self.sites_luids_dict = None


    def _sign_in_error(self):
        self.log("Tableau Server REST API Service unreachable")
        # Send an e-mail to settings.SYS_ADMIN_EMAIL_ALIAS if in Develop or Production Environment
        # IMPLEMENT
        # If basic rest_connection_object is None, then always return a "Service Not Available" response
        self.connection_signed_in = False
        return False

    # This method can sign in from the very beginning if necessary
    def _sign_in(self, rest_connection):
        rest_connection.signin()
        self.connection_signed_in = True
        self.default_connection_token = rest_connection

        # Query all the sites to allow for skipping a sign-in once you have the token
        sites = rest_connection.sites.query_sites()
        self.sites_luids_dict = {}
        for site in sites:
            self.sites_luids_dict[site.get('contentUrl')] = site.get('id')
        return True

    # Signs in to the default site with the Server Admin user credentials
    def sign_in_connection_object(self, rest_connection):
        # This is a failure within this code, not a failure to reach the Tableau Server and sign in
        if rest_connection is None:
            raise NotSignedInException()

        # Try to sign in to the Tableau Server REST API
        try:
            return self._sign_in(rest_connection)

        # Trying all these exception types than capturing anything, but probably should figure exactly what is wrong
        except NotSignedInException as e:
            try:
                return self._sign_in(rest_connection)
            except:
                return self._sign_in_error()
        # Try to sign-in again?
        except RecoverableHTTPException as e:
            try:
                return self._sign_in(rest_connection)
            except:
                return self._sign_in_error()
        # Should be capturing requests ConnectionError exception
        except Exception as e:
            try:
                return self._sign_in(rest_connection)
            except:
                return self._sign_in_error()

    # Signs in to a particular site with the Server Admin
    def sign_in_site_master(self, rest_connection, site_content_url):
        rest_connection.token = None
        rest_connection.site_content_url = site_content_url
        try:
            rest_connection.signin()
        except:
            try:
                rest_connection.signin()
            except:
                raise

        # Now grab that token
        self.site_master_tokens[site_content_url] = {"token": rest_connection.token,
                                             "user_luid": rest_connection.user_luid}

        # If no exist site
        if site_content_url not in self.site_user_tokens:
            self.site_user_tokens[site_content_url] = {}

        # And reset back to the default
        rest_connection.token = self.default_connection_token
        return True

    # All this check is if a user token exists
    def check_user_token(self, rest_connection, username, site_content_url):
        self.log('Checking user tokens. Current site master tokens are: {} . Current user tokens are: {}'.format(self.site_master_tokens, self.site_user_tokens))

        # Has the site been not used before? If not, create it
        if site_content_url not in self.site_master_tokens.keys():
            # If the site has no master token, create it
            # But we're keeping the same connection object, to limit the total number of tokens
            self.sign_in_site_master(rest_connection, site_content_url)

            # Also create an entry in the users dict for this site. The check is probably unnecessary but why not
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


    def create_user_connection(self, rest_connection, username, site_content_url):
        # Swap to the master session for the site to get the user luid

        self.log("Swapping to master site token {}".format(self.site_master_tokens[site_content_url]["token"]))
        master_token = self.site_master_tokens[site_content_url]["token"]
        master_user_luid = self.site_master_tokens[site_content_url]["user_luid"]
        self.log("sites_luid_dict: {}".format(self.sites_luids_dict))
        master_site_luid = self.sites_luids_dict[site_content_url]
        self.log("Master Site LUID for swap is {} on site {}".format(master_site_luid, site_content_url))
        rest_connection.swap_token(site_luid=master_site_luid, user_luid=master_user_luid, token=master_token)
        # Needed for Signin commands
        rest_connection.site_content_url = site_content_url
        try:
            self.log("Trying to get user_luid for {}".format(username))
            user_luid = rest_connection.query_user_luid(username)

        except:
            # Retry at least once
            try:
                user_luid = rest_connection.query_user_luid(username)
            except:
                # Assume something wrong with the site_master token, create new session
                try:
                    self.sign_in_site_master(rest_connection, site_content_url)
                    rest_connection.token = self.site_master_tokens[site_content_url]["token"]
                    user_luid = rest_connection.query_user_luid(username)
                except:
                    # Maybe another check here?
                    raise


        # Now blank the connection token so you can sign in
        rest_connection.token = None
        try:
            rest_connection.signin(user_luid)
            # Storing a dict here with the user_luid because it will be useful with swapping
            self.site_user_tokens[site_content_url][username] = {"token": rest_connection.token, "user_luid": user_luid}
            # Should this be exception instead of return of True?
            return True
        # This is bad practice to capture any exception, improve when we know what a "user not found" looks like

        except:
            # Try one more time in case it is connection issue
            try:
                rest_connection.signin(user_luid)
                self.site_user_tokens[site_content_url][username] = {"token": rest_connection.token, "user_luid": user_luid}
            except:
                # Should this be exception instead of return of True?
                return False

    def switch_user_and_site(self, rest_connection, username: str, site_content_url: str) -> Dict:
        user_exists = self.check_user_token(rest_connection, username, site_content_url)
        if user_exists is False:
            # Create the connection
            self.log("Had to create new user connection for {} on site {}".format(username, site_content_url))
            self.create_user_connection(rest_connection, username, site_content_url)
            self.log("Created connection. User {} on site {} has token {}".format(username, site_content_url, rest_connection.token))
        elif user_exists is True:
            # This token could be out of date, but test for that exception when you try to run a command

            self.log("Existing user connection found for {} on {}".format(username, site_content_url))
            token = self.site_user_tokens[site_content_url][username]["token"]
            user_luid = self.site_user_tokens[site_content_url][username]["user_luid"]
            site_luid = self.sites_luids_dict[site_content_url]
            self.log("Swapping connection to existing token {}".format(token))
            rest_connection.swap_token(site_luid=site_luid, user_luid=user_luid, token=token)
            self.log("RestAPi object token {} is now in place for user {} and site {}".format(rest_connection.token,
                                                                           rest_connection.user_luid,
                                                                           rest_connection.site_luid))
        else:
            raise Exception()
        # Return this here so it can be cached
        return self.site_user_tokens

    def switch_to_site_master(self, rest_connection, site_content_url):
        if site_content_url not in self.site_master_tokens.keys():
            self.sign_in_site_master(rest_connection, site_content_url)

        token = self.site_master_tokens[site_content_url]["token"]
        user_luid = self.site_master_tokens[site_content_url]["user_luid"]
        site_luid = self.sites_luids_dict[site_content_url]
        rest_connection.swap_token(site_luid=site_luid, user_luid=user_luid, token=token)
        # Return this here so it can be cached
        return self.site_master_tokens