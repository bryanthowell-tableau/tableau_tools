from tableau_tools import *
#import time

# This script shows two example generic functions which utilize the RestTokensManager class
# It is an example of how you can create a wrapper REST API which exposes some of the
# Tableau REST API functionality but not all of it, while using the impersonation feature
# so that each request is performed for the User, without needing their credentials (only admin credentials)
# Taken from a Django project, but Flask code would work similarly

# You would probably store your admin credentials securely in an settings or ENV file
server = settings.TABLEAU_SERVER
admin_username = settings.TABLEAU_ADMIN_USERNAME
admin_password = settings.TABLEAU_ADMIN_PASSWORD
# This is most likely just 'default' but you might have some reason not to bootstrap from there
default_site_content_url = settings.TABLEAU_DEFAULT_SITE_CONTENT_URL

tableau_tools_logger = Logger('tableau_tools.log')

# Connect to the default site to bootstrap the process

# In a running app server, the same process is always running so it needs a Master login PER site
# But also, REST API sessions do timeout, so we need to check for that possibility and remove
# Sessions once they have timed out

# You must use an Admin Username and Password, as PAT does not have Impersonation at this time (2020.2)
d = TableauServerRest(server=server, username=admin_username, password=admin_password,
                               site_content_url=default_site_content_url)
# alternatively could use the older TableauRestApiConnection objects if you had code built on those objects

# If you are using a self-signed cert or need to pass in a CERT chain, this pass directly to
# the requests library https://requests.readthedocs.io/en/master/user/quickstart/ to do whatever SSL option you need:
# d.verify_ssl_cert = False

d.enable_logging(tableau_tools_logger)
# Other options you might turn off for deeper logging:
# tableau_tools_logger.enable_request_logging()
# tableau_tools_logger.enable_response_logging()
# tableau_tools_logger.enable_debug_level()


# This manages all the connection tokens here on out
connections = RestTokensManager()

#
# RestTokensManager methods are all functional -- you pass in a TableauServerRest or TableauRestApiConnection object
# and then it perhaps actions on that object, such as logging in as a different user or switching to
# an already logged in user.
# Internally it maintains a data structure with the Admin tokens for any site that has been signed into
# And the individual User Tokens for any User / Site combination that has been signed into
# It does not RUN any REST API commands other than sign-in: You run those commands on the
# connection object once it has been returned

# For example, once this is run, the connection object 'd' will have been signed in, and you can
# do any REST API command against 'd', and it will be done as the master on the default site
# This is just bootstrapping at the very beginning to make sure we've connected successfully
# with the admin credentials. If there are errors at this point, something is likely wrong
# with the configuration/credentials or the Tableau Server
default_token = connections.sign_in_connection_object(d)

# Next is a generic_request function (based on Django pattern), that utilizes the connections object

# Every one of our REST methods follows basically this pattern
# So it has been made generic
# You pass the callback function to do whatever you want with
# the REST API object and whatever keyword arguments it needs
# Callback returns a valid type of HttpResponse object and we're all good
def generic_request(request, site, callback_function, **kwargs):
    # Generic response to start. This will be returned if no other condition overwrites it
    response = HttpResponseServerError()

    # If request is none, then it is an admin level function
    if request is not None:
        # Check user, if non, response is Http Forbidden
        # This function represents whatever your application needs to do to tell you the user who has logged in securely
        username = check_user_session(request)
        if username is None:
            response = HttpResponseForbidden()
            return response
    else:
        # If username is none, the request is run as the Site Admin
        username = None

    # Create Connection Object for Given User
    # Just create, but don't sign in. Will use swap via the TokenManager
    t = TableauServerRest32(server=server, username=admin_username, password=admin_password,
                                   site_content_url=default_site_content_url)

    # Again, you might need to pass in certain arguments to requests library if using a self-signed cert
    #t.verify_ssl_cert = False
    t.enable_logging(tableau_tools_logger)

    # Check for connection, attempt to reestablish if possible
    if connections.connection_signed_in is False:
        tableau_tools_logger.log("Signing back in to the master user")
        # If the reconnection fails, return Server Error response
        if connections.sign_in_connection_object(rest_connection=t) is False:
            # This is a Django error response, take it as whatever HTTP error you'd like to throw
            response = HttpResponseServerError()
    # If connection is already confirmed, just swap to the user token for the site
    else:
        # Site Admin level request
        if username is None:
            tableau_tools_logger.log("Swapping to Site Admin ")
            connections.switch_to_site_master(rest_connection=t, site_content_url=site)
            tableau_tools_logger.log("Token is now {}".format(t.token))
        # Request as a particular username
        else:
            tableau_tools_logger.log("Swapping in existing user token for user {}".format(username))
            connections.switch_user_and_site(rest_connection=t, username=username, site_content_url=site)
            tableau_tools_logger.log("Token is now {}".format(t.token))

    # Do action with connection
    # Whatever callback function was specified will be called with RestApiConnection / TableauServerRest object as first argument
    # then any other kwargs in the order they were passed.
    # The callback function must return a Django HttpResponse (or related) object
    # But within the callback, 't' is the TableauServerRest or TableauRestApiConnection object with the token for the
    # particular user you want
    try:
        response = callback_function(t, **kwargs)

    except NotSignedInException as e:
        if username is None:
            tableau_tools_logger.log("Master REST API session on site {} has timed out".format(site))
            del connections.site_master_tokens[site]
            # Rerun the connection
            tableau_tools_logger.log("Creating new user token for site master")
            connections.switch_to_site_master(rest_connection=t, site_content_url=site)
            tableau_tools_logger.log("Token is now {}".format(t.token))
        else:
            tableau_tools_logger.log("User {} REST API session on vertical {} has timed out".format(username, site))
            del connections.site_user_tokens[site][username]
            # Rerun the connection
            tableau_tools_logger.log("Creating new user token for username {} on vertical {}".format(username, site))
            connections.switch_user_and_site(rest_connection=t, username=username, site_content_url=site)
            tableau_tools_logger.log("Token is now {}".format(t.token))
        # Rerun the orginal callback command
        tableau_tools_logger.log("Doing callback function again now that new token exists")
        response = callback_function(t, **kwargs)
    # Originally, the code looked at the following two exceptions. This is been replaced by looking at NotSignedInException
    # RecoverableHTTPException is an exception from tableau_tools, when it is known what the error represents
    # HTTPError is a Requests library exception, which might happen if tableau_tools doesn't wrap the particular error.
    # except (RecoverableHTTPException, HTTPError) as e:
    # if e.http_code == 401:
    except Exception as e:
        raise e
    # Destroy REST API Connection Object, which is just used within this code block
    del t
    # Return Response
    return response

# There were originally separate functions but they shared enough code to be merged together
def admin_request(request, site, callback_function, **kwargs):
    # We don't pass the 'request' here, because it would have the end user's username attached via the session
    # The point is that username ends up None in the generic_request call, forcing it to use the admin
    return generic_request(None, site, callback_function, **kwargs)


#
# Here is an example of an actual exposed endpoint
#

# This is what is passed in as the callback function - so rest_connection is the 't' object passed in by generic_request
# Returns all of the Projects a user can see content in, alphabetically sorted
def query_projects(rest_connection: TableauServerRest):
    p_sort = Sort('name', 'asc')
    p = rest_connection.query_projects_json(sorts=[p_sort, ])
    return JsonResponse(p)

# An exposed endpoint linked to an actual URL
def projects(request, site):
    #log("Starting to request all workbooks")
    # Note we are just wrapping the generic request (this one doesn't take keyword arguments, but anything after
    # 'query_projects' would be passed as an argument into the query_projects function (if it took arguments)
    response = generic_request(request, site, query_projects)
    return response