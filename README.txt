tableau_tools contains many helpful programmatic tools for working with Tableau, including the tableau_rest_api library. 

Written by Bryant Howell (bhowell@tableau.com).

This is free and available for anyone to use and modify.

Not an official product or officially supported by Tableau Inc. in any way. 

Examples and explanations are at https://www.tableauandbehold.com

--- Version history ---
1.5.2: works with 9.1 and before. Python 2.7 compatible
2.0.0+: works with 9.2 (and previous versions as well). Python 2.7 compatible
2.1.0+: works with 9.3 (previous versions as well). Python 2.7 compatible
3.0.0: tableau_rest_api library refactored and added to new tableau_tools package

--- Usage Guide ---

1. Getting Started
All strings passed into tableau_tools should be Unicode. The library is completely Unicode throughout and passing text in this way ensures no issues with encoding. tableau_tools uses LXML library for all its XML parsing and generation. Some of the methods return LXML objects which can be manipulated via standard LXML methods. 

tableau_tools was programmed using PyCharm and works very well in that IDE. It is highly recommended if you are going to code with tableau_tools.

The TableauDatasourceGenerator class uses the TDEFileGenerator class, which requires the TableauSDK to be installed. You can find the SDK at https://onlinehelp.tableau.com/current/api/sdk/en-us/SDK/tableau_sdk_installing.htm#downloading


0.0 Library Structure
tableau_tools
    tableau_rest_api
        tableau_rest_api_server_connection
        published_content (Project, Workbook, Datasource)
        grantee_capabilities
        rest_xml_request
    tableau_documents
        tableau_columns
        tableau_connection
        tableau_datasource
        tableau_datasource_generator
        tableau_packaged_file
        tableau_repository_location
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

2. tableau_tools.tableau_documents sub-package
tableau_tools.tableau_documents sub-packaged implements methods for working directly with Tableau documents such as TWB, TDS, TDSX and TWBX files, which are extremely useful when dealing with a large number of workbooks or datasources, particularly for multi-tenented Sites. These methods actually allow unsupported changes to the Tableau workbook or datasource XML. If something breaks with them, blame the author of the library and not Tableau Support, who won't help you with them. Starting with Tableau 10, the Tableau Document API (https://github.com/tableau/document-api-python) is an official, supported method for doing some of this XML modification. 

2.1 TableauWorkbook, TableauDatasource and TableauConnection classes
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


2.2 TableauPackagedFile for TWBX and TDSX
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

2.4 TableauDatasourceGenerator class
The TableauDatasourceGenerator class allows for the programmatic creation of a TDS file or a TDSX file based on the programmatically generated TDS file. There is a script in the /examples directory called datasource_generation_example which outlines how to use it to use it for many different effects. 

TableauDatasourceGenerator(ds_type, ds_name, server, dbname, logger_obj, authentication=u'username-password',
                 initial_sql=None)
               
You start by adding the first table, which is the equivalent of the FROM clause in a SQL SELECT statement:               
                 
TableauDatasourceGenerator.add_first_table(db_table_name, table_alias)
TableauDatasourceGenerator.add_first_custom_sql(custom_sql, table_alias)

Then JOIN clauses can be defined to expand out the relations. You must define the ON clauses first, then pass the ON clauses as a list to the join_table method:

TableauDatasourceGenerator.define_join_on_clause(left_table_alias, left_field, operator, right_table_alias, right_field)
TableauDatasourceGenerator.join_table(join_type, db_table_name, table_alias, join_on_clauses, custom_sql=None)

Finally you can save the file.
TableauDatasourceGenerator.save_file(filename_no_extension, save_to_directory)

Example:
ds = TableauDatasourceGenerator(u'postgres', u'My DS', u'localhost', u'demo', logger)
ds.add_first_table(u'agency_sales', u'Super Store')
join_on = ds2.define_join_on_clause(u'Super Store', u'region', u'=', u'Entitled People', u'region')
ds.join_table(u'Inner', u'superstore_entitlements', u'Entitled People', [join_on, ])

2.4.1 Adding Data Source Filters
You can add Data Source Filters to your TDS file programmatically as well.

TableauDatasourceGenerator.add_dimension_datasource_filter(column_name, values, include_or_exclude=u'include', custom_value_list=False)

TableauDatasourceGenerator.add_continuous_datasource_filter(column_name, min_value=None, max_value=None, date=False)

TableauDatasourceGenerator.add_relative_date_datasource_filter(column_name, period_type, number_of_periods=None, previous_next_current=u'previous', to_date=False)

There is an equivalent method for each of these for adding filters to extracts

2.4.2 Defining calculations
TableauDatasourceGenerator.add_calculation(calculation, calculation_name, dimension_or_measure, discrete_or_continuous, datatype)

This method returns the internally defined name for the calculation, which is necessary if you want to define a Data Source filter against it. This is particularly useful for creating Row Level Security calculations programmatically. The following is an example:

# Add a calculation (this one does row level security
calc_id = ds3.add_calculation(u'IIF([salesperson_user_id]=USERNAME(),1,0) ', u'Row Level Security',
                              u'dimension', u'discrete', u'integer')
# Create a data source filter that references the calculation
ds3.add_dimension_datasource_filter(calc_id, [1, ], custom_value_list=True)

2.4.3 Adding an extract
Any data source can be turned into a TDSX with a defined extract using 

TableauDatasourceGenerator.add_extract(tde_filename, incremental_refresh_field=None)

The TDE filename is just the name that will be used inside the TDSX file. This method actually calls out to the TDEFileGenerator method eventually, which uses the Tableau SDK to generate an extract with the minimum defined to work correctly. The extract neesd to be refreshed, either in Desktop or Server, to generate and have data.

Extracts can have filters set as well:

TableauDatasourceGenerator.add_dimension_extract_filter(column_name, values, include_or_exclude=u'include', custom_value_list=False)

TableauDatasourceGenerator.add_continuous_extract_filter(column_name, min_value=None, max_value=None, date=False)

TableauDatasourceGenerator.add_relative_date_extract_filter(column_name, period_type, number_of_periods=None, previous_next_current=u'previous', to_date=False)

3. Tabcmd class

4. TableauHTTP class

5. TableauRepository class
