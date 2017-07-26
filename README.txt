tableau_tools README

tableau_tools was written by Bryant Howell (bhowell@tableau.com) and is documented mainly at tableauandbehold.com. The main repository for tableau_tools is https://github.com/bryantbhowell/tableau_tools/ . It is owned by Tableau Software but is not an officially supported library. If you have questions or issues with the library, please use the GitHub site or e-mail Bryant Howell directly. Tableau Support will not answer questions regarding the code.

tableau_tools is intended to be a simple-to-use library to handle all Tableau Server needs. The tableau_rest_api sub-package is a complete implementation of the Tableau Server REST API (https://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api.htm ). The tableau_documents sub-package works directly with Tableau files to do manipulations necessary for making programmatic changes. There is an examples folder filled with scripts that do the most common Tableau Server administrative challenges. 

Notes on Getting Started:
All strings passed into tableau_tools should be Unicode. The library is completely Unicode throughout and passing text in this way ensures no issues with encoding. tableau_tools uses ElementTree (cElementTree more precisely) library for all its XML parsing and generation. Some of the methods return Element objects which can be manipulated via standard ElementTree methods. 

tableau_tools was programmed using PyCharm and works very well in that IDE. It is highly recommended if you are going to code with tableau_tools.

The TableauDatasourceGenerator class uses the TDEFileGenerator class, which requires the TableauSDK to be installed. You can find the SDK at https://onlinehelp.tableau.com/current/api/sdk/en-us/SDK/tableau_sdk_installing.htm#downloading

--- Version history ---
1.5.2: works with 9.1 and before. Python 2.7 compatible
2.0.0+: works with 9.2 (and previous versions as well). Python 2.7 compatible
2.1.0+: works with 9.3 (previous versions as well). Python 2.7 compatible
3.0.0+: tableau_rest_api library refactored and added to new tableau_tools package
4.0.0: Big rewrite to simplify and improve. You will need to update your scripts most likely.

0.0 tableau_tools Library Structure
tableau_tools
    tableau_rest_api
        tableau_rest_api_server_connection
        published_content (Project, Workbook, Datasource)
        permissions
        rest_xml_request
    tableau_documents
        tableau_connection
        tableau_datasource 
        tableau_document
        tableau_file
        tableau_workbook
        tde_file_generator   
    logger
    tabcmd
    tableau_base
    tableau_http
    tableau_emailer
    tableau_exceptions
    tableau_repository
    

0.1 Importing tableau_tools library
It is recommended that you import everything from the tableau_tools package like:

from tableau_tools import *
from tableau_tools.tableau_rest_api import *
from tableau_tools.tableau_documents import *

0.2 Logger class
The Logger class implements useful and verbose logging to a plain text file that all of the other objects can use. You declare a single Logger object, then pass it to the other objects, resulting in a single continuous log file of all actions.

Logger(filename)

If you want to log something in your script into this log, you can call

Logger.log(l)

where l is a unicode string. You do not need to add a "\n", it will be added automatically. 

0.3 TableauBase class
Many classes within the tableau_tools package inherit from the TableauBase class. TableauBase implements the enable_logging(Logger) method, along with other a .log() method that calls to Logger.log(). It also has many static methods, mapping dicts, and helper classes related to Tableau in general. 

It should never be necessary to use TableauBase by itself.

0.4 tableau_exceptions
The tableau_exceptions file defines a variety of Exceptions that are specific to Tableau, particularly the REST API. They are not very complex, and most simply include a msg property that will clarify the problem if logged

1. tableau_tools.tableau_rest_api sub-package
Please see the README.txt file in the sub-package folder itself.
1.0 tableau_rest_api

1.1 TableauRestApiConnection classes
tableau_tools 4.0+ implements the different versions of the Tableau Server REST API as descendent classes from the parent TableauRestApiConnection class. TableauRestApiConnection implements the 2.0 version of the API, equivalent to Tableau 9.0 and 9.1. TableauRestApiConnection21 implements the 2.1 version of the API, and so forth. New versions of Tableau Server support older versions of the API, so this allows you to keep your scripts the same even when moving to a new release of Tableau Server, and then you can try new functionality by simply changing to the new TableauRestApiConnection version.

TableauRestApiConnection(server, username, password, site_content_url=""): 9.0 and 9.1
TableauRestApiConnection21: 9.2
TableauRestApiConnection22: 9.3
TableauRestApiConnection23: 10.0
TableauRestApiConnection24: 10.1
TableauRestApiConnection25: 10.2
TableauRestApiConnection26: 10.3

You need to intialize at least one object of this class. 
Ex.:
t = TableauRestApiConnection25(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd", site_content_url=u"site1")


1.2 Enabling logging for TableauRestApiConnection classes

logger = Logger(u"log_file.txt")
TableauRestApiConnection.enable_logging(logger)

1.3 Signing in
The TableauRestApiConnection doesn't actually sign in and create a session until you make a signin() call

Ex.
t = TableauRestApiConnection(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd", site_content_url=u"site1")
logger = Logger(u"log_file.txt")
t.enable_logging(logger)
t.signin() 

Now that you are signed-in, the TableauRestApiConnection object will hold all of the session state information and can be used to make any number of calls to that Site. 

1.4 Connecting to multiple sites
The Tableau REST API only allows a session to a single Site at a time. To deal with multiple sites, you can create multiple TableauRestApiConnection objects representing each site. To sign in to a site, you need the site_content_url, which is the portion of the URL that represents the Site. 

TableauRestApiConnection.query_all_site_content_urls()

returns an array that can be iterated. You must sign in to one site first to get this list however. So if you wanted to do an action to all sites, do the following:

default = TableauRestApiConnection26(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd")
default.signin()
site_content_urls = default.query_all_site_content_urls()

for site_content_url in site_content_urls:
    t = TableauRestApiConnection26(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd", site_content_url=site_content_url)
    t.signin()
    ...
    
2. Basics and Querying

2.1 LUIDs - Locally Unique IDentifiers
The Tableau REST API represents each object on the server (project, workbook, user, group, etc.) with a Locally Unique IDentifier (LUID). Every command other than the sign-in to a particular site (which uses the site_content_url) requires a LUID. LUIDs are returned when you create an object on the server, or they can be retrieved by the Query methods and then searched to find the matching LUID. tableau_tools 4.0 handles translations between real world names and LUIDs automatically for the vast majority of methods. Any parameter names that can accept both LUIDs and names are named along the pattern : "..._name_or_luid". There are few cases where only the LUID can be accepted. In this case, there are


2.2 Plural querying methods
The simplest method for getting information from the REST API are the "plural" querying methods

TableauRestApiConnection.query_groups()
TableauRestApiConnection.query_users()
TableauRestApiConnection.query_workbooks()
TableauRestApiConnection.query_projects()
TableauRestApiConnection.query_datasources()

These will all return an ElementTree object representing the results from the REST API call. This can be useful if you need all of the information returned, but most of your calls to these methods will be to get a dictionary of names : luids you can use for lookup. There is a simple static method for this conversion

TableauRestApiConnection.convert_xml_list_to_name_id_dict(lxml_obj)

Ex.
default = TableauRestApiConnection25(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd")
default.signin()
groups = default.query_groups()
groups_dict = default.convert_xml_list_to_name_id_dict(groups)

for group_name in groups_dict:
    print "Group name {} is LUID {}".format(group_name, groups_dict[group_name])

2.2.1 Filtering and Sorting (Tableau Server 9.3+):
TableauRestApiConnection22 implements filtering and sorting for the methods where it is allowed. Singular lookup methods are programmed to take advantage of this automatically for improved perofrmance, but the plural querying methods can use the filters to bring back specific sets.

http://onlinehelp.tableau.com/current/api/rest_api/en-us/help.htm#REST/rest_api_concepts_filtering_and_sorting.htm%3FTocPath%3DConcepts%7C_____7

Filters can be passed via a UrlFilter class object. The UrlFilter class implements static factory methods to generate objects with the correct settings for each type of filter you might want to pass. 

UrlFilter.create_name_filter(name):
UrlFilter.create_owner_name_filter(owner_name)
UrlFilter.create_site_role_filter(site_role)
UrlFilter.create_last_login_filter(operator, last_login_time)
UrlFilter.create_created_at_filter(operator, created_at_time)
UrlFilter.create_updated_at_filter(operator, updated_at_time)
UrlFilter.create_tags_filter(operator, tags)

Ex. 
owner_name_filter = UrlFilter.create_owner_name_filter(u'Bryant')


There is also a Sort object, which can just be initialized with the right parameters
Sort(

    
2.3 LUID Lookup Methods
There are numerous methods for finding an LUID based on the name of a piece of content. An example would be:

TableauRestApiConnection24.query_group_luid(name)

These methods are very useful when you need a LUID to generate another action. With tableau_tools 4.0+, you shouldn't need these methods very frequently, as the majority of methods will do the lookup automaticaly if a name is passed in.   


2.4 Singular querying methods
There are methods for getting the XML just for a single object, but they actually require calling to the plural methods internally in many cases where there is no singular method actually implemented in Tableau Server. 

Most methods follow this pattern:

TableauRestApiConnection.query_project(name_or_luid)
TableauRestApiConnection.query_user(username_or_luid)
TableauRestApiConnection.query_datasource(ds_name_or_luid, proj_name_or_luid=None)
TableauRestApiConnection.query_workbook(wb_name_or_luid, p_name_or_luid=None, username_or_luid=None)

You'll notice that query_workbook and query_datasource include parameters for the project (and the username for workbooks). This is because workbook and datasource names are only unique within a Project of a Site, not within a Site. If you search without the project specified, the method will return a workbook if only one is found, but if multiple are found, it will throw a MultipleMatchesFoundException .

Starting in tableau_tools 4.0, query_project returns a Project object, which is necessary when setting Permissions.

TableauRestApiConnection.query_project(project_name_or_luid) : returns Project


2.5 Querying Permissions
In tableau_tools 4.0+, all Permissions are handled through a PublishedContent object (Project, Workbook, or Datasource). There are no direct methods to access them, because the PublishedContent methods include the most efficient algorithms for updating Permissions with the least amount of effort.

2.6 "Download" and "Save" methods
Published content (workbooks and datasources) and thumbnails can all be queried, but they come down in formats that need to be saved in most cases. For this reason, their methods are named as following:

TableauRestApiConnection.save_workbook_preview_image(wb_luid, filename)
TableauRestApiConnection.save_workbook_view_preview_image_by_luid(wb_luid, view_luid, filename)

The download_ methods actually can result in a programmatic representation of the content if there is no filename given. This will be discussed in another section of the guide.

# Do not include file extension. Without filename, only returns the response
TableauRestApiConnection.download_datasource_by_luid(ds_luid, filename=None)
TableauRestApiConnection.download_workbook_by_luid(wb_luid, filename=None, no_obj_return=False)


3. Administrative Actions (adding, removing, and syncing)

3.1 Adding Users
There are two separate actions in the Tableau REST API to add a new user. First, the user is created, and then additional details are set using an update command. tableau_rest_api implements these two together as: 

TableauRestApiConnection.add_user(username, fullname, site_role=u'Unlicensed', password=None, email=None, update_if_exists=False)

If you just want to do the basic add, without the update, then do:

TableauRestApiConnection.add_user_by_username(username, site_role=u'Unlicensed', update_if_exists=False)

The update_if_exists flag allows for the role to be changed even if the user already exists when set to True.


3.2 Create Methods for other content types
The other methods for adding content start with "create_". Each of these will return the LUID of the newly created content

TableauRestApiConnection.create_project(project_name, project_desc=None, locked_permissions=False)
TableauRestApiConnection.create_site(new_site_name, new_content_url, admin_mode=None, user_quota=None, storage_quota=None, disable_subscriptions=None)
TableauRestApiConnection.create_group(self, group_name)
TableauRestApiConnection.create_group_from_ad_group(self, ad_group_name, ad_domain_name, default_site_role=u'Unlicensed', sync_as_background=True)

Ex.
new_luid = t.create_group(u"Awesome People")

3.3 Adding users to a Group
Once users have been created, they can be added into a group via the following method, which can take either a single string or a list/tuple set. Anywhere you see the "luid_s" pattern in a parameter, it means you can pass a unicode string or a list of unicode strings to make the action happen to all of those in the list. 

TableauRestApiConnection.add_users_to_group(username_or_luid_s, group_name_or_luid)

Ex.
usernames_to_add = [u"user1@example.com", u"user2@example.com", u"user3@example.com"]
users_luids = []
for username in usernames_to_add:
    new_luid = t.add_user_by_username(username, site_role=u"Interactor")
    users_luids.append(new_luid)

new_group_luid = t.create_group(u"Awesome People")
t.add_users_to_group_by_luid(users_luids, new_group_luid)

3.5 Update methods
If you want to make a change to an existing piece of content on the server, there are methods that start with "update_". Many of these use optional keyword arguments, so that you only need to specify what you'd like to change.

Here's an example for updating a datasource:
TableauRestApiConnection.update_datasource(name_or_luid, new_datasource_name=None, new_project_luid=None,
                          new_owner_luid=None, proj_name_or_luid=False

Note that if you want to change the actual content of a workbook or datasource, that requires a Publish action with Overwrite set to True                          
                          
3.6 Deleting / Removing Content
Methods with "remove_" are used for user membership, where the user still exists on the server at the end.

TableauRestApiConnection.remove_users_from_site_by_luid(user_luid_s)
TableauRestApiConnection.remove_users_from_group_by_luid(user_luid_s, group_luid)

Methods that start with "delete_" truly delete the content 

TableauRestApiConnection.delete_workbooks(wb_name_or_luid_s)
TableauRestApiConnection.delete_projects(project_name_or_luid_s)
etc.

3.7 Deleting a site
The method for deleting a site requires that you first be signed into that site

TableauRestApiConnection.delete_current_site()

If you are testing a script that creates a new site, you might use the following pattern to delete the existing version before rebuilding it:

d = TableauRestApiConnection24(server, username, password, site_content_url='default')
d.enable_logging(logger)
d.signin()

new_site_content_url = u"my_site_name"
try:
    print "Attempting to create site {}".format(new_site_content_url)
    d.create_site(new_site_content_url, new_site_content_url)
except AlreadyExistsException:
    print "Site replica already exists, deleting bad replica"
    t = TableauRestApiConnection24(server, username, password, site_content_url=new_site_content_url)
    t.enable_logging(logger)
    t.signin()
    t.delete_current_site()

    d.signin()
    d.create_site(new_site_content_url, new_site_content_url)

print "Logging into {} site".format(new_site_content_url)
t = TableauRestApiConnection(server, username, password, site_content_url=new_site_content_url)
t.enable_logging(logger)
t.signin()


4. Permissions
The tableau_rest_api library handles permissions via the Permissions and PublishedContent (Project, Workbook, Datasource) classes, encapsulating all of the necessary logic to make changes to permissions both easy and efficient.

Permissions are by far the most complex issue in the Tableau REST API. Every content object (Project, Workbook or Datasource) can have permissions (known as "capabilities" in the REST API) set for each member object (Group or User). This is represented in the REST API by granteeCapabilities XML, which is a relatively complex XML object. Capabilities can also be "unspecified", and if this is the case, they simply are missing from the granteeCapabilities XML.

Additionally, there is no "update" functionality for permissions capabilities -- if you want to submit changes, you must first delete out those permissions. Thus any "update" must involve determining the current state of the permissions on an object and removing those permissions before assigning the new permissions. 

The most efficient algorithm for sending an update is thus:

    a. For the given user or group to be updated, see if there are any existing permissions for that user or group
    b. If the existing permissions match exactly, do not make any changes (Otherwise, you'd have to delete out every permission only to reset it exactly as it was before)
    c. If the permissions do not match exactly, delete all of the existing permissions for that user or group (and only those that are set, therefore saving wasted deletion calls)
    d. Set the new permissions for that user or group

tableau_rest_api handles this through two concepts -- the Permissions object that represents the permissions / capabilities, and the PublishedContent classes, which represente the objects on the server that have permissions.

4.1 PublishedContent classes (Project20/Project21, Workbook, Datasource)
There are three classes that represent the state of published content to a server; they all descend from the PublishedContent class, but there is no reason to ever access PublishedContent directly. Each of these require passing in an active and signed-in TableauRestApiConnection object so that they can perform actions against the Tableau Server.

Project obviously represents a project. In API Verison 2.1, a Project also contains a child Workbook and Datasource object that represent the Default Permissions that can be set for that project. In API Version 2.0, the Project simply has a full set of capabilities that include those that apply to a workbook or a datasource. This reflects the difference in Tableau Server itself. If you are still on 9.1 or before, make sure to set your tableau_server_version argument so that the Project class behaves correctly.


TableauRestApiConnection.get_published_datasource_object(datasource_name_or_luid, project_name_or_luid)
TableauRestApiConnection.get_published_workbook_object(workbook_name_or_luid, project_name_or_luid)

There is also a get_published_project_object method, but the standard query_project() method returns the Project object in tableau_tools 4.0+, so you can just use that method.
TableauRestApiConnection.get_published_project_object(project_name_or_luid, project_xml_obj=None)

Project20 represents the 9.0 and 9.1 style Project without default permissions.
Project21 represents all 9.2+ server versions with Default Permissions and locking content permissions to project.

The TableauRestApiConnectionXX class will give you the right Project20/Project21 object for its version.


Project21 implements the lock and unlock methods that only work in API Version 2.1+
Project21.lock_permissions()
Project21.unlock_permission()
Project21.are_permissions_locked()

You access the default permissions objects with the following, which are Workbook or Datasource object:

Project21.workbook_defaults
Project21.datasource_defaults

4.2 Permissions classes
Any time you want to set or change permissions, you should instantiate one of the Permissions classes to represent that set of permissions/capabilities available.

WorkbookPermissions20(group_or_user, group_or_user_luid)
WorkbookPermissions21(group_or_user, group_or_user_luid)
DatasourcePermissions20(group_or_user, group_or_user_luid)
DatasourcePermissions21(group_or_user, group_or_user_luid)
ProjectPermissions20(group_or_user, group_or_user_luid)
ProjectPermissions21(group_or_user, group_or_user_luid)

You can get the correct permissions object through factory methods on the Project20 and Project21 classes. The option role parameter sets the permissions to match one of the named roles in Tableau Server. It is a shortcut to the set_capabilities_to_match_role method:

Project20.create_datasource_permissions_object_for_group(luid, role=None)
Project20.create_workbook_permissions_object_for_group(luid, role=None)
Project20.create_project_permissions_object_for_group(luid, role=None)
Project20.create_datasource_permissions_object_for_user(luid, role=None)
Project20.create_workbook_permissions_object_for_user(luid, role=None)
Project20.create_project_permissions_object_for_user(luid, role=None)

Project21.create_datasource_permissions_object_for_group(luid, role=None)
Project21.create_workbook_permissions_object_for_group(luid, role=None)
Project21.create_project_permissions_object_for_group(luid, role=None)
Project21.create_datasource_permissions_object_for_user(luid, role=None)
Project21.create_workbook_permissions_object_for_user(luid, role=None)
Project21.create_project_permissions_object_for_user(luid, role=None)

This ProjectXX object should be acquired by querying or creating a project, returning the correct Project object. You shouldn't ever need to contruct any of them manually.

Ex. 

proj = t.query_project(u'My Project')
best_group_perms_obj = proj.get_workbook_permissions_object_for_group(u'Best Group')
second_best_group_perms_obh = proj.get_workbook_permissions_object_for_group(u'Second Best Group', role=u'Interactor')

4.2 Setting Capabilities
The Permissions classes have methods for setting capabilities individually, or matching the selectable "roles" in the Tableau Server UI. 

The two allowable modes are u"Allow" and u"Deny", whereas setting unspecified has its own method.

Permissions.set_capability(capability_name, mode)
Permissions.set_capability_to_unspecified(capability_name)

There are two quick methods for all to allow or all to deny:

Permissions.set_all_to_deny()
Permissions.set_all_to_allow()

There is also a method to match the roles from the Tableau Server UI. It is aware of both the api version and the content_type, and will give you an error if you choose a role that is not available for that content type ("Project Leader" on a Workbook, for example)

Permissions.set_capabilities_to_match_role(role)

Ex. 
proj = t.query_project(u'My Project')
best_group_perms_obj = proj.create_workbook_permissions_object_for_group(u'Best Group')
best_group_perms_obj.set_capabilities_to_match_role(u"Publisher")
# alternatively, you can set this in the factory method
# best_group_perms_obj = proj.create_workbook_permissions_object_for_group(u'Best Group', role=u'Publisher')

4.2 Permissions Setting
All of the PublishedContent classes (Workbook, ProjectXX and Datasource) inherit the following method for setting permissions:

PublishedContent.set_permissions_by_permissions_obj_list(new_permissions_obj_list)

This method does all of the necessary checks to send the simplest set of calls to update the content object. It takes a list of Permissions objects and compares against any existing permissions to add or update as necessary.

Ex.
proj = t.query_project(u'My Project')
best_group_perms_obj = proj.create_project_permissions_object_for_group(u'Best Group')
best_group_perms_obj.set_capabilities_to_match_role(u"Publisher")
proj.set_permissions_by_permissions_obj_list([best_group_perms_obj, ]) # Note creating a list for singular item

# Setting default permissions for workbook
best_group_perms_obj = proj.create_workbook_permissions_object_for_group(u'Best Group')
best_group_perms_obj.set_capabilities_to_match_role(u"Interactor")
proj.workbook_defaults.set_permissions_by_permissions_obj_list([best_group_perms_obj, ])

# Setting default permissions for data source
best_group_perms_obj = proj.create_datasource_permissions_object_for_group(u'Best Group', role=u'Editor')
proj.datasource_defaults.set_permissions_by_permissions_obj_list([best_group_perms_obj, ])

4.3 Reusing Permissions Objects
If you have a Permissions object that represents a set of permissions you want to reuse, you can copy the object so that you can work with this. To do this, use the python copy module, with the deepcopy method

best_group_perms_obj = proj.create_datasource_permissions_object_for_group(u'Best Group', role=u'Editor')
second_best_group_perms_obj = copy.deepcopy(best_group_perms_obj) # Now these are separate actual objects
second_best_group_perms_obj.luid = t.query_group_luid(u'Second Best Group')

# Transform to user from group
my_user_perms_obj = copy.deepcopy(second_best_group_perms_obj)
my_user_perms_obj.group_or_user = u'user'
my_user_perms_obj.luid = t.query_user_luid(u'My User Name')

# Set on proj
proj.clear_all_permissions()
proj.set_permissiosn_by_permissions_obj_list([best_group_perms_obj, second_best_group_perms_obj, my_user_perms_obj])

4.4 Replicating Permissions from One Site to Another
 -- There is an included example script "replicate_site_structure_sample.py" which shows this in action
The PublishedContent class has a method called 
PublishedContent.convert_permissions_obj_list_from_orig_site_to_current_site(permissions_obj_list, orig_site)

orig_site is a TableauRestApiConnection class object that is a signed-in connection to the original site. This allows the method to translate the names of Groups and Users from the Originating Site to the site where the PublishedContent lives. In most cases, you'll do this on a Project object. The method returns a list of Permissions objects, which can be put directly into set_permissions_by_permissions_obj_list

Ex.

orig_proj = o.query_project(proj_name)
new_proj = n.query_project(proj_name)

# Clear everything on the new one
new_proj.clear_all_permissions()

# Project Permissions
o_perms_obj_list = orig_proj.current_perms_obj_list
n_perms_obj_list = new_proj.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list, o)
new_proj.set_permissions_by_permissions_obj_list(n_perms_obj_list)

# Workbook Defaults
o_perms_obj_list = orig_proj.workbook_defaults.current_perms_obj_list
n_perms_obj_list = new_proj.workbook_defaults.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list, o)
new_proj.workbook_defaults.set_permissions_by_permissions_obj_list(n_perms_obj_list)

# Project Defaults
o_perms_obj_list = orig_proj.datasource_defaults.current_perms_obj_list
n_perms_obj_list = new_proj.datasource_defaults.convert_permissions_obj_list_from_orig_site_to_current_site(o_perms_obj_list, o)
new_proj.datasource_defaults.set_permissions_by_permissions_obj_list(n_perms_obj_list)


5. Publishing Content
The Tableau REST API can publish both data sources and workbooks, either as TWB / TDS files or TWBX or TDSX files. It actually has two different methods of publishing; one as a single upload, and the other which chunks the upload. tableau_rest_api encapsulates all this into two methods that detect the right calls to make. The default threshold is 20 MB for a file before it switches to chunking. This is set by the "single_upload_limit" variable. 

If a workbook references a published data source, that data source must be published first. Additionally, unlike Tableau Desktop, the REST API will not find linked files and upload them. A workbook with a "live connection" to an Excel file, for example, must be saved as a TWBX rather than a TWB for an upload to work correctly. The error messages if you do not follow this order are not very clear. 

5.1 Publishing a Workbook or Datasource
The publish methods must upload directly from disk. If you are manipulating a workbook or datasource using the TableauFile / TableauDocument classes, please save the file prior to publishing. Also note that you specify a Project object rather than the LUID.

TableauRestApiConnection.publish_workbook(workbook_filename, workbook_name, project_obj, overwrite=False, connection_username=None, connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=False)

TableauRestApiConnection.publish_datasource(ds_filename, ds_name, project_obj, overwrite=False, connection_username=None, connection_password=None, save_credentials=True)


6. Refreshing Extracts (Tableau 10.3+)
The TableauRestApiConnection26 class, representing the API for Tableau 10.3, includes methods for triggering extract refreshes via the REST API.

TableauRestApiConnection26.run_all_extract_refreshes_for_schedule(schedule_name_or_luid) 

runs through all extract tasks related to a given schedule and sets them to run.

If you want to run one task individually, use

TableauRestApiConnection26.run_extract_refresh_for_workbook(wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None)
TableauRestApiConnection26.run_extract_refresh_for_datasource(ds_name_or_luid, proj_name_or_luid=None, username_or_luid=None)

You can get all extract refresh tasks on the server using

TableauRestApiConnection26.get_extract_refresh_tasks()

although if you simply want to set all of the extract schedules to run, use

TableauRestApiConnection22.query_extract_schedules()

There is equivalent for for subscription schedules:
TableauRestApiConnection22.query_subscription_schedules()

Ex.
extract_schedules = t.query_extract_schedules()
sched_dict = t.convert_xml_list_to_name_id_dict(extract_schedules)
for sched in sched_dict:
    t.run_all_extract_refreshes_for_schedule(sched_dict[sched])  # This passes the LUID
    # t.run_all_extract_refreshes_for_schedule(sched_dict) # You can pass the name also, it just causes extra lookups

7. Modifying Tableau Documents (for Template Publishing)
tableau_documents implements some features that go beyond the Tableau REST API, but are extremely useful when dealing with a large number of workbooks or datasources, particularly for multi-tenented Sites. These methods actually allow unsupported changes to the Tableau workbook or datasource XML. If something breaks with them, blame the author of the library and not Tableau Support, who won't help you with them.

6.1 Document classes
The tableau_documents library is a hierarchical set of classes which model Tableau's files and the data structures within them. The model looks slightly different whether a workbook or a datasource, because workbooks can embed multiple datasources:

Datasource:

TableauFile
    TableauDatasource (TableauDocument)
        [TableauConnection]
        TableauColumns
    
Workbook:

TableauFile
    TableauWorkbook (TableauDocument)
        [TableauDatasource]
            [TableauConnection]
            TableauColumns
    

6.1 TableauFile class
The TableauFile class represents an actual existing Tableau file on the local storage (.tds, .tdsx, .twb, .twbx). It is initialized with:

TableauFile(filename, logger_obj=None, create_new=False, ds_version=u'10')

TableauFile determines what type of file has been opened, and if it is a packaged workbook or datasource, it extracts the embedded TWB or TDS file temporarily to disk so that it can be accessed as a file. All of this is done to disk so that everything is not loaded and kept in memory.      

TableauFile.file_type property  returns one of [u'twb', u'twbx, u'tds', u'tdsx'], which allows you to determine a particular set of actions to take depending on the file type. 

TableauFile.tableau_document property retrieves the TableauDocument object within. This will actually be either a TableauWorkbook or TableauDatasource object, which is why the file_type is useful. 

TableauFile also allows you to create a new datasource from scratch. To implement, initialize without a file name like:

tf = TableauFile(None, logger_obj, create_new=True, ds_version=u'10') # ds_version=u'9' for a 9.0 style datasource

The TableauFile.tableau_document object will be a new TableauDatasource object, ready to be set built up.

6.2 TableauDocument
The TableauDocument class helps map the differences between TableauWorkbook and TableauDatasource. It only implements two properties:

TableauDocument.document_type  : return either [u'datasource', u'workbook'] . More generic than TableauFile.file_type
TableauDocument.datasources : returns an array of TableauDatasource objects. 
For a TableauDatasource, TableauDocuemnt.datasources will only have a single datasource, itself, in datasources[0]. TableauWorkbooks might have more than one. This property thus allows you to do modifications on both individual datasources and those embedded within workbooks without worrying about whether the document is a workbook or a datasource.

TableauDocument also implements a save_file method:

TableauDocument.save_file(filename_no_extension, save_to_directory=None)

which does the correct action based on whether it is a TableauDatasource or a TableauWorkbook (implemented separately for each)

6.3 TableauWorkbook class
At this point in time, the TableauWorkbook class is really just a container for TableauDatasources, which it creates automatically when initialized. Because workbook files can get very very large, the initializer algorithm only reads through the datasources, which are at the beginning of the document, and then leaves the rest of the file on disk.

TableauWorkbook.save_file(filename_no_extension, save_to_directory=None)

is used to save a TWB file. It also uses the algorithm from the initializer method to read the existing TWB file from disk, line by line. It skips the original datasource section and instead writes in the new datasource XML from the array of TableauDatasource objects. The benefit of this is that the majority of the workbook is untouched, and larger documents do not cause high memory usage.

At the current time, this means that you cannot modify any of the other functionality that is specified in the workbook itself. Additional methods could be implemented in the future based on a similar algorithm (picking out specific subsections and representing them in memory as ElementTree objects, then inserting back into place later). 

6.4 TableauDatasource class
The TableauDatasource class is represents the XML contained within a TDS (or an embedded datasource within a workbook). 

Tableau Datasources changed considerably from the 9 series to the 10 series; Tableau 10 introduced the concept of Cross-Database JOIN, known internally as Federated Connections. So a datasource in 10.0+ can have multiple connections. tableau_tools handles determinig the all of this automatically, unless you are creating a TableauDatasource object from scratch (more on this later), in whcih case you need to specify which type of datasource you want. 


If you are opening a TDS file, you should use TableauFile to open it, where the TableauDatasource object will be available via TableauFile.tableau_document. You really only need to create TableauDatasource object yourself when creating one from scratch, in which case you initialize it like:

TableauDatasource(datasource_xml=None, logger_obj=None, ds_version=None)

ex. 

logger = Logger('ds_log.txt')
new_ds = TableauDatasource(ds_version=u'10', logger_obj=logger)

ds_version takes either u'9' or u'10, because it is more on basic structure and the individual point numbers don't matter.

6.5 TableauConnection
In a u'9' version TableauDatasource, there is only connections[0] because there was only one connection. A u'10' version can have any number of federated connections in this array. If you are creating connections from scratch, I highly recommend doing single connections. There hasn't been any work to make sure federated connections work correctly with modifications.

The TableauConnection class represents the connection to the datasource, whether it is a database, a text file. It should be created automatically for you through the TableauDatasource object. 

You can access and set all of the relevant properties for a connection, using the following properties

TableauConnection.server
TableauConnection.dbname
TableauConnection.schema  # equivalent to dbname. Actual XML does vary -- Oracle has schema attribute while others have dbname. Either method will do the right thing
TableauConnection.port
TableauConnection.connection_type
TableauConnection.sslmode
TableauConnection.authentication

When you set using these properties, the connection XML will be changed when the save method is called on the TableauDatasource object.

ex.
twb = TableauFile(u'My TWB.twb')
dses = twb.tableau_document.datasources
for ds in dses:
    if ds.published is not True:  # See next section on why you should check for published datasources
        for conn in ds.connections:
            if conn.dbname == u'test_db':
                conn.dbname = u'production_db'
                conn.port = u'5128'

                
twb.save_new_file(u'Modified Workbook')


6.6 Published Datasources in a workbook
Datasources in a workbook come in two types: Embedded and Published. An embedded datasource looks just like a standard TDS file, except that there can be multiple in a workbook. Published Datasources have an additional tag called <repository-location> which tells the information about the Site and the published Datasource name

To see if a datasource is published, use the property
TableauDatasource.published : returns True or False

If published is True, you can get or set the Site of the published DS. This was necessary in Tableau 9.2 and 9.3 to publish to different sites, and it still might be best practice, so that there is no information about other sites passed in (see notes). TableauRestApiConnection.publish_workbook and .publish_datasource both do this check and modification for you automatically so that the site is always correct.

ex.
twb = TableauFile(u'My TWB.twb')
dses = twb.tableau_document.datasources
for ds in dses:
    if ds.published is True:
        print ds.published_ds_site
        # Change the ds_site
        ds.published_ds_site = u'new_site'  # Remember to use content_url rather than the pretty site name

        
        
***NOTE: From this point on, things become increasingly experimental and less supported. However, I can assure you that many Tableau customers do these very things, and we are constantly working to improve the functionality for making datasources dynamically.

6.7 Adding an Extract to an Existing TableauDatasource
TableauDatasource.add_extract(new_extract_filename) 

sets a datasource to have an extract added when the datasource is saved. This command will automatically switch a the saving from a TDS file to a TDSX file or a TWB file to a TWBX when the TableauFile.save_new_file() method is called.

If there is an existing extract, an AlreadyExistsException will be raised. 

ex.

twb = TableauFile(u'My TWB.twb')
dses = twb.tableau_document.datasources
i = 1
for ds in dses:
    try:
        ds.add_extract(u'Extract {}.tde'.format(i))
        i += 1
    except AlreadyExistsException as e:
        # Skip any existing extracts in the workbook
        continue
new_filename = twb.save_new_file(u'Extract Workbooks')
print new_filename  # Extract Workbooks.twbx
        
6.8 Modifying Table JOIN Structure in a Connection

6.9 Creating a TableauDatasource from Scratch
If you intialized a TableauFile object with no filename, you will have "from scratch" TableauDatasource as your .tableau_document object. The TableauDatasource object contains all of the functionality from TableauDatasourceGenerator from a previous version of tableau_tools. 



6.10

6.8 Translating Columns
TableauDatasource.translate_columns(key_value_dict) will do a find/replace on the caption attribute of the column tags in the XML.

When you save the datasource (or workbook), the changed captions will be written into the new XML.

translate_columns actually calls translate_captions in the TableauColumns object, which follows the following rules for a match:

    If no caption is set, look for a dict key that matches the name attribute, and if it matches, create a caption attribute and give it the value from the dict
    If a caption is already set, look for a matching dict key for the existing caption.
        If matching caption exists, replace with the new value
        If matching caption does not exist, look for a matching name attribute, then replace the caption if one is found

This is why the best method is to set your tokens in Tableau Desktop, so that you know exactly the captions you want to match to.

Here is some example code in action (in an ideal world, you would pull your translations from a table and create the dicts programmatically):

logger = Logger('translate.log')
# Translation dictionaries (build automatically from a table)
translations = { 'en': {
                       '{Order Date}': 'Order Date',
                       '{Sales}': 'Sales'
                       },
                 'de': {
                        '{Order Date}': 'Auftragsdatum',
                        '{Sales}': 'Bestellungen'
                       },
                 'ru': {
                        '{Order Date}': u'Дата заказа',
                        '{Sales}': u'заказы'
                       },
                'th': {
                        '{Order Date}': u'วันสั่ง',
                        '{Sales}': u'คำสั่งซื้อ'
                      }
              }


for lang in translations:
    wb_obj = TableauWorkbook(wb_filename, logger_obj=logger)
    
    for ds in wb_obj.datasources.values():
        # Input the dict with translations
        ds.translate_columns(translations[lang])

    # Save to a new workbook with the correct language code appended
    wb_obj.save_workbook_xml('workbook_{}.twb'.format(lang))