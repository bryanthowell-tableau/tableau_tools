# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tableau_documents import *
from tableau_tools.tableau_documents.hyper_file_generator import HyperFileGenerator

import pyodbc
import sys

# Extract files (.hyper and .tde) have very little metadata in them
# They are most useful when combined with <datasource> XML in a TDS file, wrapped together as a TDSX
# or with that same XML in a TWB file, packaged together as a TWBX

# Two ways to handle the process of attaching the data source XML to the updated Extract file:
# 1. Substitution: Create a valid TDSX/TWBX in Tableau Desktop, then use that file as a base for substituting updated
# Extract files. As long as structure of Extract does not change, should work all the time
# 2. Fom Scratch: If the structure of the Extract can vary, then you cannot pre-build the XML.
# tableau_tools has the necessary methods to build the data source XML from scratch, including relationships


#
# Getting Data Into an Extract / Building the Extract
#

# One way to create the Extract is to pass a pyoodbc cursor
# This function handles some of the encoding parameters correctly for you since Extracts are unicode
def pyodbc_connect_and_query(odbc_connect_string: str, query: str) -> pyodbc.Cursor:

    try:
        conn = pyodbc.connect(odbc_connect_string)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(str, encoding='utf-8')
        conn.setencoding(str, encoding='utf-8', ctype=pyodbc.SQL_CHAR)

        # https://github.com/mkleehammer/pyodbc/issues/194 for this encoding fix

        conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-32le')
    except pyodbc.Error as e:
        print("ODBC Connection Error Message:\n")
        print(e)
        print("ODBC error, exiting...\n")
        sys.exit()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
    except pyodbc.ProgrammingError as e:
        print("\nODBC Query Error Message:\n")
        print(e)
        print("\nODBC error, exiting...\n")
        sys.exit()
    return cursor

# Example of creating a single Hyper file
def hyper_create_one_table_from_pyodbc_cursor(new_hyper_filename: str, table_1_pyodbc_conn_string: str,
                                              table_1_query: str, table_1_tableau_name: str):

    # HyperFileGenerator is a wrapper class for the Extract API 2.0
    # Use Hyper API (3.0) instead if you want the latest and greatest
    h = HyperFileGenerator()

    # Table 1
    filled_cursor = pyodbc_connect_and_query(table_1_pyodbc_conn_string, table_1_query)

    # This method gets all of the info from the table schema and correctly creates the extract schema
    h.create_table_definition_from_pyodbc_cursor(filled_cursor)
    # This takes the cursor, reads through all the rows, and ads them into the extract
    h.create_extract(new_hyper_filename, append=True, table_name=table_1_tableau_name, pyodbc_cursor=filled_cursor)

    print('Table 1 added to file {}'.format(new_hyper_filename))

# Example of creating two tables in a single Hyper file
def hyper_create_two_tables(new_hyper_filename: str, table_1_pyodbc_conn_string: str, table_1_query: str,
                            table_1_tableau_name: str,
                            table_2_pyodbc_conn_string: str, table_2_query: str, table_2_tableau_name: str):

    # HyperFileGenerator is a wrapper class for the Extract API
    h = HyperFileGenerator()

    # Table 1
    filled_cursor = pyodbc_connect_and_query(table_1_pyodbc_conn_string, table_1_query)

    # This method gets all of the info from the table schema and correctly creates the extract schema
    h.create_table_definition_from_pyodbc_cursor(filled_cursor)
    # This takes the cursor, reads through all the rows, and ads them into the extract
    h.create_extract(new_hyper_filename, append=True, table_name=table_1_tableau_name, pyodbc_cursor=filled_cursor)

    print('Table 1 added to file {}'.format(new_hyper_filename))

    # Table 2
    filled_cursor_2 = pyodbc_connect_and_query(table_2_pyodbc_conn_string, table_2_query)
    # This method gets all of the info from the table schema and correctly creates the extract schema
    h.create_table_definition_from_pyodbc_cursor(filled_cursor_2)
    # This takes the cursor, reads through all the rows, and ads them into the extract
    h.create_extract(new_hyper_filename, append=True, table_name=table_2_tableau_name, pyodbc_cursor=filled_cursor_2)
    print('Table 2 added to file {}'.format(new_hyper_filename))

    print('All Done with Hyper create')
    return True

#
# Substitution
#


# Specify the existing Extract file you want replaced
# It should match exactly, so it's best to do this after building the TDSX/TWBX the first time in Desktop using
# an Extract file you created programmatically using the same code as the one you will substitute in here
def substitute_an_existing_extract(tableau_packaged_filename: str, new_extract_filename: str,
                                   previous_extract_flename_in_packaged_file: Optional[str] = None) -> str:
    t_file = TableauFileManager.open(filename=tableau_packaged_filename)
    # See what files might be in there if you need
    files_in_there = t_file.get_filenames_in_package()

    if previous_extract_flename_in_packaged_file is not None:
        existing_hyper_filename = previous_extract_flename_in_packaged_file
    else:
        existing_hyper_filename = None
        # This is simple test on expectation of single Hyper file but you could probably do more
        for file in files_in_there:
            if file.lower().find('.hyper') != -1:
                existing_hyper_filename = file
                break
    # Make sure we actually found a Hyper file to replace
    if existing_hyper_filename is None:
        print('No Hyper file found to replace, breaking')
        exit()

    t_file.set_file_for_replacement(filename_in_package=existing_hyper_filename,
                                    replacement_filname_on_disk=new_extract_filename)
    newly_saved_filename = t_file.save_new_file(filename_no_extension='Swapped Out Hyper')
    return newly_saved_filename


# DISABLED IN INITIAL 5.0.0 release, could come back later. Better off building a template in Desktop and substituting
#
# Creating From Scratch
#
#def create_new_tds_for_two_table_extract(new_tds_filename, hyper_filename):
#    t_file = TableauFile(new_tds_filename, create_new=True, ds_version='10.5')
#    ds = t_file.tableau_document.datasources[0]  # type: TableauDatasource
#    conn = ds.add_new_connection(ds_type='hyper', db_or_schema_name='Data/{}'.format(hyper_filename),
#                                 authentication='auth-none')
#    conn_obj = ds.connections[0]  # type: TableauConnection
#    conn_obj.username = 'tableau_internal_user'

#    # Your actual logic here will vary depending on what you have named the tables and what they join on
#    ds.set_first_table('First Table', 'First Table', connection=conn, extract=True)
#    join_clause = ds.define_join_on_clause('First Table', 'join_key_id', '=', 'Second Table', 'join_key_id')
#    ds.join_table('Inner', 'Second Table', 'Second Table', [join_clause, ])
#    new_filename = t_file.save_new_file('Generated Hyper Final')
#    return new_filename


# This is the functional example using the functions above
def build_row_level_security_extract_file():

    # Definition of ODBC connection string, this one for PostgreSQL
    # pyodbc is generic and should work with anything

    db_server = 'a.postgres.lan'
    db_port = '5432'
    db_db = 'database_name'
    db_username = 'username'
    db_password = 'password'
    pg_conn_string = 'Driver={{PostgreSQL Unicode}};Server={};Port={};Database={};Uid={};Pwd={};'.format(
        db_server, db_port, db_db, db_username, db_password)

    fact_table_query = 'SELECT * FROM fact_table;'

    entitlement_table_query = 'SELECT * FROM entitlements;'

    # Create the Hyper File
    new_hyper_file = 'New Hyper File.hyper'
    results = hyper_create_two_tables(new_hyper_filename='New Hyper File.hyper', table_1_pyodbc_conn_string=pg_conn_string,
                                      table_1_query=fact_table_query, table_1_tableau_name='Facts',
                                      table_2_pyodbc_conn_string=pg_conn_string, table_2_query=entitlement_table_query,
                                      table_2_tableau_name='Entitlements')

    # Now you have a Hyper file which you can connect to in Desktop, which can then
    # define the JOINs to create a Tableau Data Source, including the JOIN definitions and calculations.
    # You can then publish that data source. But if you want to keep it updated, you need to push the updated Hyper file
    # into the TDSX file.

    new_packaged_filename = substitute_an_existing_extract(tableau_packaged_filename='My template file.tdsx',
                                   new_extract_filename=new_hyper_file)

    # Now publish it up to the Server
    t = TableauServerRest33(server='http://myTableauServer', username='', password='', site_content_url='some_site')
    t.signin()
    project_to_publish_to = t.projects.query_project(project_name_or_luid='Default')
    ds_luid = t.datasources.publish_datasource(ds_filename=new_packaged_filename, ds_name='My Datasource Name',
                                     project_obj=project_to_publish_to,overwrite=True, save_credentials=False)
    print('Published to server with LUID {}'.format(ds_luid))

