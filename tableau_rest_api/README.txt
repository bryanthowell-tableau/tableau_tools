tableau_tools.tableau_rest_api sub-package

The tableau_rest_api sub-packaged was formerly a stand-alone package called tableau_rest_api

--- Version history ---
1.5.2: works with 9.1 and before. Python 2.7 compatible
2.0.0+: works with 9.2 (and previous versions as well). Python 2.7 compatible
2.1.0+: works with 9.3 (previous versions as well). Python 2.7 compatible
3.0.0+: tableau_rest_api library refactored and added to new tableau_tools package


1.1 TableauRestApiConnection class
The main class you need to get started is 

TableauRestApiConnection(server, username, password, site_content_url="")

You need to intialize at least one object of this class. 
Ex.:
t = TableauRestApiConnection(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd", site_content_url=u"site1")


1.2 Enabling logging for TableauRestApiConnection class

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

default = TableauRestApiConnection(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd")
default.signin()
site_content_urls = default.query_all_site_content_urls()

for site_content_url in site_content_urls:
    t = TableauRestApiConnection(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd", site_content_url=site_content_url)
    t.signin()
    ...
    
2. Basics and Querying

2.1 LUIDs - Locally Unique IDentifiers
The Tableau REST API represents each object on the server (project, workbook, user, group, etc.) with a Locally Unique Identifier (LUID). Every command other than the sign-in to a particular site (which uses the site_content_url) requires a LUID. LUIDs are returned when you create an object on the server, or they can be retrieved by the Query methods and then searched to find the matching LUID. tableau_rest_api has a large set of methods that perform lookup and translation so that you can work with the "pretty" names and let the library find the correct LUIDs for you. 


2.2 Plural querying methods
The simplest method for getting information from the REST API are the "plural" querying methods

TableauRestApiConnection.query_groups()
TableauRestApiConnection.query_users()
TableauRestApiConnection.query_workbooks_for_user_by_luid(user_luid)
TableauRestApiConnection.query_workbooks()
TableauRestApiConnection.query_projects()
TableauRestApiConnection.query_datasources()

These will all return an LXML object representing the results from the REST API call. This can be useful if you need all of the information returned, but most of your calls to these methods will be to get a dictionary of names : luids you can use for lookup. There is a simple static method for this conversion

TableauRestApiConnection.convert_xml_list_to_name_id_dict(lxml_obj)

Ex.
default = TableauRestApiConnection(u"http://127.0.0.1", u"admin", u"adminsp@ssw0rd")
default.signin()
groups = default.query_groups()
groups_dict = default.convert_xml_list_to_name_id_dict(groups)

for group_name in groups_dict:
    print "Group name {} is LUID {}".format(group_name, groups_dict[group_name])


2.3 LUID Lookup Methods
There are numerous methods for finding an LUID based on the name of a piece of content. An example would be:

TableauRestApiConnection.query_group_luid_by_name(name)

These methods are very useful when you need a LUID to generate another action.     


2.4 Singular querying methods
There are methods for getting the XML just for a single object, but they actually require calling to the plural methods internally in many cases where there is no singular method actuall implemented in Tableau Server. For example, the following two lookup methods: 

TableauRestApiConnection.query_project_by_luid(luid)
TableauRestApiConnection.query_project_by_name(name)

actually use query_projects() then use xpath querying to get just the single project that matches that LUID or name (by_name actually does the lookup for the LUID as well). 

The simplest methods follow this pattern:

TableauRestApiConnection.query_project(name_or_luid)

and accurately branch off depending on what is passed to them.  

2.5 Querying Permissions
There are methods to query permissions directly, but it's best to let them be handled via the PublishedContent class, in conjunction with the GranteeCapabilities class. This is documented further down

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

TableauRestApiConnection.add_users_to_group_by_luid(user_luid_s, group_luid)

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


TableauRestApiConnection.delete_workbooks_by_luid(wb_luid_s)
TableauRestApiConnection.delete_projects_by_luid(project_luid_s)
etc.

3.7 Deleting a site
The method for deleting a site requires that you first be signed into that site

TableauRestApiConnection.delete_current_site()

If you are testing a script that creates a new site, you might use the following pattern to delete the existing version before rebuilding it:

d = TableauRestApiConnection(server, username, password, site_content_url='default', tableau_server_version='9.1')
d.enable_logging(logger)
d.signin()

new_site_content_url = u"my_site_name"
try:
    print "Attempting to create site {}".format(new_site_content_url)
    d.create_site(new_site_content_url, new_site_content_url)
except AlreadyExistsException:
    print "Site replica already exists, deleting bad replica"
    t = TableauRestApiConnection(server, username, password, site_content_url=new_site_content_url, tableau_server_version='9.1')
    t.enable_logging(logger)
    t.signin()
    t.delete_current_site()

    d.signin()
    d.create_site(new_site_content_url, new_site_content_url)

print "Logging into {} site".format(new_site_content_url)
t = TableauRestApiConnection(server, username, password, site_content_url=new_site_content_url, tableau_server_version='9.1')
t.enable_logging(logger)
t.signin()


4. Permissions
The tableau_rest_api library handles permissions via the GranteeCapabilities and PublishedContent (Project, Workbook, Datasource) classes, encapsulating all of the necessary logic to make changes to permissions both easy and efficient.

Permissions are by far the most complex issue in the Tableau REST API. Every content object (Project, Workbook or Datasource) can have permissions (known as "capabilities" in the REST API) set for each member object (Group or User). This is represented in the REST API by granteeCapabilities XML, which is a relatively complex XML object. Capabilities can also be "unspecified", and if this is the case, they simply are missing from the granteeCapabilities XML.

Additionally, there is no "update" functionality for permissions capabilities -- if you want to submit changes, you must first delete out those permissions. Thus any "update" must involve determining the current state of the permissions on an object and removing those permissions before assigning the new permissions. 

The most efficient algorithm for sending an update is thus:

    a. For the given user or group to be updated, see if there are any existing permissions for that user or group
    b. If the existing permissions match exactly, do not make any changes (Otherwise, you'd have to delete out every permission only to reset it exactly as it was before)
    c. If the permissions do not match exactly, delete all of the existing permissions for that user or group (and only those that are set, therefore saving wasted deletion calls)
    d. Set the new permissions for that user or group

tableau_rest_api handles this through two concepts -- the GranteeCapabilities object that represents the permissions / capabiltiies


4.1 GranteeCapabilities class
Any time you want to set or change permissions, you should instantiate a GranteeCapabilities object to represent that set of permissions/capabilities. 

The "obj_type" argument takes either u"group" or u"user". It is highly recommended that you set your permissions using groups rather than users, and tableau_rest_api often defaults to group where possible on this recommendation. content_type is one of: u"project", u"workbook", or u"datasource". Giving the content_type allows for setting only applicable capabilities for the given object. The TableauRestApiConnection object has a method for generating a GranteeCapabilities object with the correct settings, and is preferable to initializing the object directly:

TableauRestApiConnection.get_grantee_capabilities_object(obj_type, luid, content_type=None)

Ex. 
team_group_luid = t.query_group_luid_by_name(u"Team Member")
team_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"project")

Internally, tableau_rest_api represents the full set of granteeCapabilities on a content object as a Python list of GranteeCapabilities objects. You will see this in some of the code as "gcap_obj_list". If you want to pass a single object to a method that takes a list, simply do the [gcap_obj, ] Python syntax.

In version 2.0 of the library, you must create the GranteeCapabilities object directly like this:

GranteeCapabilities(obj_type, luid, content_type=None, tableau_server_version=u"9.2")

The version matters for GranteeCapabilities, as project capabilities shifted significantly with Tableau Server 9.2.

4.2 Setting Capabilities
The GranteeCapabilities class has methods for setting capabilities individually, or matching the selectable "roles" in the Tableau Server UI. 

The two allowable modes are u"Allow" and u"Deny", whereas setting unspecified has its own method.

GranteeCapabilities.set_capability(capability_name, mode)
GranteeCapabilities.set_capability_to_unspecified(capability_name)

There are two quick methods for all to allow or all to deny:

GranteeCapabilities.set_all_to_deny()
GranteeCapabilities.set_all_to_allow()

There is also a method to match the roles from the Tableau Server UI. It is aware of both the api version and the content_type, and will give you an error if you choose a role that is not available for that content type ("Project Leader" on a Workbook, for example)

GranteeCapabilities.set_capabilities_to_match_role(role)

Ex. 
team_group_luid = t.query_group_luid_by_name(u"Team Member")
team_p_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"project")
team_p_gcap_obj.set_capabilities_to_match_role(u"Publisher")

team_wb_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"workbook")
team_wb_gcap_obj.set_capabilities_to_match_role(u"Interactor")


4.3 PublishedContent classes (Project, Workbook, Datasource)
There are three classes that represent the state of published content to a server; they all descend from the PublishedContent class, but there is no reason to ever access PublishedContent directly. Each of these require passing in an active and signed-in TableauRestApiConnection object so that they can perform actions against the Tableau Server.

Project obviously represents a project. In API Verison 2.1, a Project also contains a child Workbook and Datasource object that represent the Default Permissions that can be set for that project. In API Version 2.0, the Project simply has a full set of capabilities that include those that apply to a workbook or a datasource. This reflects the difference in Tableau Server itself. If you are still on 9.1 or before, make sure to set your tableau_server_version argument so that the Project class behaves correctly.

TableauRestApiConnection.get_project_object_by_luid(luid)
TableauRestApiConnection.get_datasource_object_by_luid(luid)
TableauRestApiConnection.get_workbook_object_by_luid(luid)

In version 2.0 of the library, you had to declare this separately:
Project(luid, tableau_rest_api_obj, tableau_server_version=u"9.2", logger_obj=None)

Project implements the lock and unlock methods that only work in API Version 2.1 (9.2 and on)

Project.lock_permissions()
Project.unlock_permission()

You access the default permissions objects with the following:

Project.workbook_default
Project.datasource_default

All three (Workbook, Project and Datasource) inherit all the following:

PublishedContent.set_permissions_by_gcap_obj(new_gcap_obj)
PublishedContent.get_gcap_obj_list()
PublishedContent.get_permissions_xml()

set_permission_by_gcap_obj does all of the necessary checks to send the simplest set of calls to update the content object. It takes a single GranteeCapabilities object at a time so that the comparisons are simple and easy to understand. 

Ex.
sandbox_proj_luid = t.query_project_luid_by_name(u"Sandbox")
sandbox_proj = t.get_project_object_by_luid(sandbox_proj_luid)
sandbox_proj.lock_permissions()

team_group_luid = t.query_group_luid_by_name(u"Team Member")
team_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"project")
team_gcap_obj.set_capabilities_to_match_role(u"Publisher")
sandbox_proj.set_permissions_by_gcap_obj(team_gcap_obj)

# Setting default permissions for workbook
team_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"workbook")
team_gcap_obj.set_capabilities_to_match_role(u"Interactor")
sandbox_proj.workbook_default.set_permissions_by_gcap_obj(team_gcap_obj)

# Setting default permissions for data source
team_gcap_obj = t.get_grantee_capabilities_object(u"group", team_group_luid, content_type=u"datasource")
team_gcap_obj.set_capabilities_to_match_role(u"Editor")
sandbox_proj.datasource_default.set_permissions_by_gcap_obj(team_gcap_obj)


5. Publishing Content
The Tableau REST API can publish both data sources and workbooks, either as TWB / TDS files or TWBX or TDSX files. It actually has two different methods of publishing; one as a single upload, and the other which chunks the upload. tableau_rest_api encapsulates all this into two methods that detect the right calls to make. The default threshold is 20 MB for a file before it switches to chunking. This is set by the "single_upload_limit" variable. 

If a workbook references a published data source, that data source must be published first. Additionally, unlike Tableau Desktop, the REST API will not find linked files and upload them. A workbook with a "live connection" to an Excel file, for example, must be saved as a TWBX rather than a TWB for an upload to work correctly. The error messages if you do not follow this order are not very clear. 

5.1 Publishing a Workbook or Datasource
The publish methods were orginally designed to upload directly from disk, and if you specify a text string for the filename argument, tableau_rest_api will attempt to open those files and then upload them. 

TableauRestApiConnection.publish_workbook(workbook_filename, workbook_name, project_luid, overwrite=False, connection_username=None, connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=False)

TableauRestApiConnection.publish_datasource(ds_filename, ds_name, project_luid, overwrite=False, connection_username=None, connection_password=None, save_credentials=True)

You can also pass in a TableauWorkbook object into publish_workbook, or a TableauDatasource object into publish_datasource. Information about these class types is further in this document. Simply put, they allow you to make certain changes to the workbook or datasource XML programmatically in-memory, without having to write to disk each time.

5.2 Publishing a Workbook Connected to a Published Datasource
Starting in Tableau Server 9.2, a slight modification must be made to a workbook that connects to a Published Datasource on a different site than the workbook was originally created on. The correct order for this type of publishing is:

1) Publish the Data Source to the Site, with the same name that was used on the first site that the workbook connected to. (So, if the workbook connected to "Data Source 1" on "Test Site", you need to publish the same data source (even if you've modified the connection information) to "Customer Site 1" with the name "Data Source 1".
2) Publish the workbook with the check_published_ds=True flag on, like so:

TableauRestApiConnection.publish_workbook(workbook_filename, workbook_name, project_luid, overwrite=True, check_published_ds=True)

check_published_ds is False by default because performing the check every time does add some time to the publishing process, particularly on very large workbooks. However, it must be turned to True if you are publishing a workbook that connects to a Published Data Source. (Implemented in library version 2.1.3 and beyond)

6. Advanced Features for Publishing from Templates
tableau_rest_api implements some features that go beyond the Tableau REST API, but are extremely useful when dealing with a large number of workbooks or datasources, particularly for tenented Sites. These methods actually allow unsupported changes to the Tableau workbook or datasource XML. If something breaks with them, blame the author of the library and not Tableau Support, who won't help you with them.

6.1 TableauWorkbook, TableauDatasource and TableauConnection classes
The TableauWorkbook and TableauDatasource classes are representations of the TWB and TDS XML files, and contain other sub-objects which allow them to change the XML of TWB or TDS to do things like changing the database name that a workbook is pointing to. 

TableauWorkbook.get_datasources()

returns a list of TableauDatasource objects.

TableauDatasource.get_datasource_name()

Each TableauDatasource contains a TableauConnection object, which is automatically created and parses the XML. You can make changes to the TableauConnection object like:

TableauConnection.set_dbname(new_db_name)
TableauConnection.get_dbname()
TableauConnection.set_server(new_server)
TableauConnection.get_server()
TableauConnection.set_username(new_username)
TableauConnection.set_port(new_port)
TableauConnection.get_port()
TableauConnection.get_connection_type()
TableauConnection.set_sslmode(new_ssl_mode)

'dbname' is the logical partition name -- this could be a "schema" on Oracle or a "database" on MS SQL Server or PostgreSQL. It is typically the only one that needs to be set.

Ex.
wb_filename = 'Viz.twb'
fh = open(wb_filename, 'rb')
wb = TableauWorkbook(fh.read(), logger)
dses = wb.get_datasources()
for ds in dses.values():
    if ds.connection.get_dbname() == 'demo':
        ds.connection.set_dbname('demo2')
        ds.connection.set_server('192.0.0.1')
        ds.connection.set_username(username)
        ds.connection.set_sslmode('require')
iv.publish_workbook(tc_wb, u'Magically Changed Viz', project_luid, overwrite=True, connection_username=username, connection_password=password)
fh.close()


6.2 TableauPackagedFile for TWBX and TDSX
The TableauPackagedFile class actually can read a TWBX or TDSX file, extract out the TWB or TDS and then creates a child object of the TableauWorkbook or TableauDatasource class.

TableauPackagedFile(zip_file_obj, logger_obj=None)

You can get the type and then the object, and that lets you manipulate the underlying TableauWorkbook or TableauDatasource as you would if it was not part of the packged file. You can even save your changes to a new TWBX or TDSX file (the file extension will be automatically determined).

TableauPackagedFile.get_type()
TableauPackagedFile.get_tableau_object()
TableauPackagedFile.save_new_packaged_file(new_filename_no_extension)

6.3 Translating Columns
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