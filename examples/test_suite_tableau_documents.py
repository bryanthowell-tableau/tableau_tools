from tableau_tools import *
from tableau_documents import *

#
# WIP and will be more fully built out in the future. See template_publish_sample.py for other uses as well as the README
#

def live_db_connection_changes():
    t_file: TDS = TableauFileManager.open(filename='Live PostgreSQL.tds')
    if isinstance(t_file, DatasourceFileInterface):
        print('Yeah I got data sources')
    print(t_file.file_type)
    # print(t_file.tableau_xml_file)
    print(t_file.tableau_document)
    print(t_file.datasources)
    for ds in t_file.datasources:
        print(ds)
        print(ds.connections)
        ds.ds_name = 'New Datasource Name'
        if ds.is_stored_proc is False:
            if ds.main_table_type == 'table':
                ds.tables.main_table_name = '[some other table]'
        for conn in ds.connections:
            conn.connection_name = 'Changed Connection Name'
    t_file.save_new_file('New TDS')

    t_file_2: TDSX = TableauFileManager.open(filename='First Datasource Revision.tdsx')
    print(t_file_2.file_type)
    print(t_file_2.tableau_xml_file)
    print(t_file_2.tableau_document)
    print(t_file_2.datasources)
    for ds in t_file_2.datasources:
        print(ds)
        print(ds.connections)
    t_file_2.save_new_file('New TDSX')

    t_file_3: TWBX = TableauFileManager.open(filename='First Workbook Revision.twbx')
    print(t_file_3.file_type)
    print(t_file_3.tableau_xml_file)
    print(t_file_3.tableau_document)
    print(t_file_3.datasources)
    for ds in t_file_3.datasources:
        print(ds)
        print(ds.connections)
        print(ds.ds_name)
        print(ds.is_published)
        for conn in ds.connections:
            print(conn.connection_name)
            print(conn.connection_type)
    t_file_3.save_new_file('New TWBX')

def flat_file_connection_changes():
    pass
def hyper_api_swap():
    pass

