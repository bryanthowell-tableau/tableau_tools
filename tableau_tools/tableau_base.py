import random
from .tableau_exceptions import *
from .logger import Logger
import re

from io import StringIO
import xml.etree.cElementTree as etree


class TableauBase(object):
    def __init__(self):
        # In reverse order to work down until the acceptable version is found on the server, through login process
        self.supported_versions = ('2018.1', "10.5", "10.4", "10.3", "10.2", "10.1", "10.0", "9.3", "9.2", "9.1", "9.0")
        self.logger = None
        self.luid_pattern = r"[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*"

        # Defaults, will get updated with each update. Overwritten by set_tableau_server_version
        self.version = "10.5"
        self.api_version = "2.8"
        self.tableau_namespace = 'http://tableau.com/api'
        self.ns_map = {'t': 'http://tableau.com/api'}
        self.ns_prefix = '{' + self.ns_map['t'] + '}'
        etree.register_namespace('t', self.ns_map['t'])

        self.site_roles = (
            'Interactor',
            'Publisher',
            'SiteAdministrator',
            'Unlicensed',
            'UnlicensedWithPublish',   # This was sunset at some point
            'Viewer',
            'ViewerWithPublish',
            'ServerAdministrator',
            'ReadOnly',
            'Explorer',
            'ExplorerCanPublish',
            'SiteAdministratorExplorer',
            'Creator',
            'SiteAdministratorCreator'
        )

        server_content_roles_2_0 = {
                "project": (
                    'Viewer',
                    'Interactor',
                    'Editor',
                    'Data Source Connector',
                    'Data Source Editor',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Data Source Connector',
                    'Data Source Editor'
                )
            }

        server_content_roles_2_1 = {
                "project": (
                    'Viewer',
                    'Publisher',
                    'Project Leader'
                ),
                "workbook": (
                    'Viewer',
                    'Interactor',
                    'Editor'
                ),
                "datasource": (
                    'Editor',
                    'Connector'
                )
            }

        self.server_content_roles = {
            "2.0": server_content_roles_2_0,
            "2.1": server_content_roles_2_1,
            "2.2": server_content_roles_2_1,
            "2.3": server_content_roles_2_1,
            "2.4": server_content_roles_2_1,
            "2.5": server_content_roles_2_1,
            "2.6": server_content_roles_2_1,
            "2.7": server_content_roles_2_1,
            "2.8": server_content_roles_2_1,
            '3.0': server_content_roles_2_1,
            '3.1': server_content_roles_2_1
        }

        self.server_to_rest_capability_map = {
            'Add Comment': 'AddComment',
            'Move': 'ChangeHierarchy',
            'Set Permissions': 'ChangePermissions',
            'Connect': 'Connect',
            'Delete': 'Delete',
            'View Summary Data': 'ExportData',
            'Download Summary Data': 'ExportData',
            'Export Image': 'ExportImage',
            'Download Image/PDF': 'ExportImage',
            'Download': 'ExportXml',
            'Download Workbook/Save As': 'ExportXml',
            'Filter': 'Filter',
            'Project Leader': 'ProjectLeader',
            'View': 'Read',
            'Share Customized': 'ShareView',
            'View Comments': 'ViewComments',
            'View Underlying Data': 'ViewUnderlyingData',
            'Download Full Data' : 'ViewUnderlyingData',
            'Web Edit': 'WebAuthoring',
            'Save': 'Write',
            'Inherited Project Leader': 'InheritedProjectLeader',
            'all': 'all'  # special command to do everything
        }

        capabilities_2_0 = {
                "project": (
                    'AddComment',
                    'ChangeHierarchy',
                    'ChangePermissions',
                    'Connect',
                    'Delete',
                    'ExportData',
                    'ExportImage',
                    'ExportXml',
                    'Filter',
                    'ProjectLeader',
                    'Read',
                    'ShareView',
                    'ViewComments',
                    'ViewUnderlyingData',
                    'WebAuthoring',
                    'Write'
                ),
                "workbook": (
                    'AddComment',
                    'ChangeHierarchy',
                    'ChangePermissions',
                    'Delete',
                    'ExportData',
                    'ExportImage',
                    'ExportXml',
                    'Filter',
                    'Read',
                    'ShareView',
                    'ViewComments',
                    'ViewUnderlyingData',
                    'WebAuthoring',
                    'Write'
                ),
                "datasource": (
                    'ChangePermissions',
                    'Connect',
                    'Delete',
                    'ExportXml',
                    'Read',
                    'Write'
                )
            }

        capabilities_2_1 = {
                "project": ("Read", "Write", 'ProjectLeader'),
                "workbook": (
                    'Read',
                    'ExportImage',
                    'ExportData',
                    'ViewComments',
                    'AddComment',
                    'Filter',
                    'ViewUnderlyingData',
                    'ShareView',
                    'WebAuthoring',
                    'Write',
                    'ExportXml',
                    'ChangeHierarchy',
                    'Delete',
                    'ChangePermissions',

                ),
                "datasource": (
                    'Read',
                    'Connect',
                    'Write',
                    'ExportXml',
                    'Delete',
                    'ChangePermissions'
                )
            }

        capabilities_2_8 = {
                "project": ("Read", "Write", 'ProjectLeader', 'InheritedProjectLeader'),
                "workbook": (
                    'Read',
                    'ExportImage',
                    'ExportData',
                    'ViewComments',
                    'AddComment',
                    'Filter',
                    'ViewUnderlyingData',
                    'ShareView',
                    'WebAuthoring',
                    'Write',
                    'ExportXml',
                    'ChangeHierarchy',
                    'Delete',
                    'ChangePermissions',

                ),
                "datasource": (
                    'Read',
                    'Connect',
                    'Write',
                    'ExportXml',
                    'Delete',
                    'ChangePermissions'
                )
            }

        self.available_capabilities = {
            "2.0": capabilities_2_0,
            "2.1": capabilities_2_1,
            "2.2": capabilities_2_1,
            '2.3': capabilities_2_1,
            '2.4': capabilities_2_1,
            '2.5': capabilities_2_1,
            '2.6': capabilities_2_1,
            '2.7': capabilities_2_1,
            '2.8': capabilities_2_8,
            '3.0': capabilities_2_8,
            '3.1': capabilities_2_8

        }

        self.datasource_class_map = {
            "Actian Vectorwise": "vectorwise",
            "Amazon EMR": "awshadoophive",
            "Amazon Redshift": "redshift",
            "Aster Database": "asterncluster",
            "Cloudera Hadoop": "hadoophive",
            "DataStax Enterprise": "datastax",
            "EXASolution": "exasolution",
            "Firebird": "firebird",
            "Generic ODBC": "genericodbc",
            "Google Analytics": "google-analytics",
            "Google BigQuery": "bigquery",
            "Hortonworks Hadooop Hive": "hortonworkshadoophive",
            "HP Vertica": "vertica",
            "IBM BigInsights": "bigsql",
            "IBM DB2": "db2",
            "JavaScript Connector": "jsconnector",
            "MapR Hadoop Hive": "maprhadoophive",
            "MarkLogic": "marklogic",
            "Microsoft Access": "msaccess",
            "Microsoft Analysis Services": "msolap",
            "Microsoft Excel": "excel-direct",
            "Microsoft PowerPivot": "powerpivot",
            "Microsoft SQL Server": "sqlserver",
            "MySQL": "mysql",
            "IBM Netezza": "netezza",
            "OData": "odata",
            "Oracle": "oracle",
            "Oracle Essbase": "essbase",
            "ParAccel": "paraccel",
            "Pivotal Greenplum": "greenplum",
            "PostgreSQL": "postgres",
            "Progress OpenEdge": "progressopenedge",
            "SAP HANA": "saphana",
            "SAP Netweaver Business Warehouse": "sapbw",
            "SAP Sybase ASE": "sybasease",
            "SAP Sybase IQ": "sybaseiq",
            "Salesforce": "salesforce",
            "Spark SQL": "spark",
            "Splunk": "splunk",
            "Statistical File": "",
            "Tableau Data Extract": "dataengine",
            "Teradata": "teradata",
            "Text file": "textscan",
            "Hyper": 'hyper'
        }

        self.permissionable_objects = ('datasource', 'project', 'workbook')

    def set_tableau_server_version(self, tableau_server_version):
        """
        :type tableau_server_version: unicode
        """
        # API Versioning (starting in 9.2)
        if str(tableau_server_version)in ["9.2", "9.3", "10.0", "10.1", "10.2", "10.3", "10.4", "10.5",
                                              '2018.1', '2018.2']:
            if str(tableau_server_version) == "9.2":
                self.api_version = "2.1"
            elif str(tableau_server_version) == "9.3":
                self.api_version = "2.2"
            elif str(tableau_server_version) == '10.0':
                self.api_version = '2.3'
            elif str(tableau_server_version) == '10.1':
                self.api_version = '2.4'
            elif str(tableau_server_version) == '10.2':
                self.api_version = '2.5'
            elif str(tableau_server_version) == '10.3':
                self.api_version = '2.6'
            elif str(tableau_server_version) == '10.4':
                self.api_version = '2.7'
            elif str(tableau_server_version) == '10.5':
                self.api_version = '2.8'
            elif str(tableau_server_version) == '2018.1':
                self.api_version = '3.0'
            elif str(tableau_server_version) == '2018.2':
                self.api_version = '3.1'
            self.tableau_namespace = 'http://tableau.com/api'
            self.ns_map = {'t': 'http://tableau.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
            return self.api_version
        elif str(tableau_server_version) in ["9.0", "9.1"]:
            self.api_version = "2.0"
            self.tableau_namespace = 'http://tableausoftware.com/api'
            self.ns_map = {'t': 'http://tableausoftware.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
            return self.api_version
        else:
            raise InvalidOptionException("Please specify tableau_server_version as a string. '9.0' or '9.2' etc...")

    # Logging Methods
    def enable_logging(self, logger_obj):
        """
        :type logger_obj: Logger
        :return:
        """
        if isinstance(logger_obj, Logger):
            self.logger = logger_obj

    def log(self, l):
        if self.logger is not None:
            self.logger.log(l)

    def log_debug(self, l):
        if self.logger is not None:
            self.logger.log_debug(l)

    def start_log_block(self):
        if self.logger is not None:
            self.logger.start_log_block()

    def end_log_block(self):
        if self.logger is not None:
            self.logger.end_log_block()

    def log_uri(self, uri, verb):
        if self.logger is not None:
            self.logger.log_uri(verb, uri)

    def log_xml_request(self, xml, verb):
        if self.logger is not None:
            self.logger.log_xml_request(verb, xml)

    # Method to handle single str or list and return a list
    @staticmethod
    def to_list(x):
        if isinstance(x, str):
            l = [x]  # Make single into a collection
        else:
            l = x
        return l

    # Method to read file in x MB chunks for upload, 10 MB by default (1024 bytes = KB, * 1024 = MB, * 10)
    @staticmethod
    def read_file_in_chunks(file_object, chunk_size=(1024 * 1024 * 10)):
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data

    # You must generate a boundary string that is used both in the headers and the generated request that you post.
    # This builds a simple 30 hex digit string
    @staticmethod
    def generate_boundary_string():
        """
        :return: unicode
        """
        random_digits = [random.SystemRandom().choice('0123456789abcdef') for n in range(30)]
        s = "".join(random_digits)
        return s

    # URI is different form actual URL you need to load a particular view in iframe
    @staticmethod
    def convert_view_content_url_to_embed_url(content_url):
        """
        :type content_url: unicode
        :return: unicode
        """
        split_url = content_url.split('/')
        return 'views/{}/{}'.format(split_url[0], split_url[2])

    # Generic method for XML lists for the "query" actions to name -> id dict
    @staticmethod
    def convert_xml_list_to_name_id_dict(lxml_obj):
        """
        :type lxml_obj: etree.Element
        :return: dict
        """
        d = {}
        for element in lxml_obj:
            e_id = element.get("id")
            # If list is collection, have to run one deeper
            if e_id is None:
                for list_element in element:
                    e_id = list_element.get("id")
                    name = list_element.get("name")
                    d[name] = e_id
            else:
                name = element.get("name")
                d[name] = e_id
        return d

    # Convert a permission
    def convert_server_permission_name_to_rest_permission(self, permission_name):
        if permission_name in self.server_to_rest_capability_map:
            return self.server_to_rest_capability_map[permission_name]
        else:
            raise InvalidOptionException('{} is not a permission name on the Tableau Server'.format(permission_name))

    # 32 hex characters with 4 dashes
    def is_luid(self, val):
        """
        :type val: unicode
        :return: bool
        """
        if len(val) == 36:
            if re.match(self.luid_pattern, val) is not None:
                return True
            else:
                return False
        else:
            return False



