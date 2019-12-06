from ...tableau_tools import *
from tableau_documents import *

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
        print(ds.published)
        for conn in ds.connections:
            print(conn.connection_name)
            print(conn.connection_type)
    t_file_3.save_new_file('New TWBX')

def flat_file_connection_changes():
    pass
def hyper_api_swap():
    pass

