from ...tableau_tools import *
from tableau_documents import *

def live_db_connection_changes():
    t_file: TDS = TableauFileManager.open(filename='Live PostgreSQL.tds')
    print(t_file.file_type)
    print(t_file.tableau_xml_file)
    print(t_file.tableau_document)
    print(t_file.datasources)

    t_file_2: TDSX = TableauFileManager.open(filename='First Datasource Revision.tdsx')
    print(t_file_2.file_type)
    print(t_file_2.tableau_xml_file)
    print(t_file_2.tableau_document)
    print(t_file_2.datasources)

def flat_file_connection_changes():
    pass
def hyper_api_swap():
    pass

