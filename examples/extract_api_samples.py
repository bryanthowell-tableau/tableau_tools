# -*- coding: utf-8 -*-
from tableau_tools import *
from tableau_tools.tableau_rest_api import *
from tableau_tools.tableau_documents.hyper_file_generator import HyperFileGenerator
from tableau_tools.tableau_documents.tableau_file import TableauFile
from tableau_tools.tableau_documents.tableau_datasource import TableauDatasource, TableauConnection
import pyodbc
import sys


def pyodbc_connect_and_query(odbc_connect_string, query):
    try:
        conn = pyodbc.connect(odbc_connect_string)
        conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        conn.setencoding(str, encoding='utf-8')
        conn.setencoding(unicode, encoding='utf-8', ctype=pyodbc.SQL_CHAR)

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


# Example of creating two tables in a single Hyper file
def hyper_create_two_tables(new_hyper_filename, table_1_pyodbc_conn_string, table_1_query, table_1_tableau_name,
                            table_2_pyodbc_conn_string, table_2_query, table_2_tableau_name):

    h = HyperFileGenerator(logger)

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


def create_new_tds_for_two_table_extract(new_tds_filename, hyper_filename):
    t_file = TableauFile(new_tds_filename, create_new=True, ds_version=u'10.5')
    ds = t_file.tableau_document.datasources[0]  # type: TableauDatasource
    conn = ds.add_new_connection(ds_type=u'hyper', db_or_schema_name=u'Data/Test File.hyper',
                                 authentication=u'auth-none')
    conn_obj = ds.connections[0]  # type: TableauConnection
    conn_obj.username = u'tableau_internal_user'
    ds.set_first_table(u'First Table', u'First Table', connection=conn, extract=True)
    # join_clause = ds.define_join_on_clause(u'First Table', u'agent_id', u'=', u'Second Table', u'agent_id')
    # ds.join_table(u'Inner', u'Second Table', u'Second Table', [join_clause, ])
    t_file.save_new_file(u'Generated Hyper Final')
# extract_api_with_tds()


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

    # You can also create a simple TDS that defines the JOINs programmatically using tableau_tools

    # Example forthcoming
