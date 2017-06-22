import random
from tableau_exceptions import *
from logger import Logger
import re
from lxml import etree
from StringIO import StringIO


class TableauBase(object):
    def __init__(self):
        # In reverse order to work down until the acceptable version is found on the server, through login process
        self.supported_versions = (u"10.0", u"9.3", u"9.2", u"9.1", u"9.0")
        self.logger = None
        self.luid_pattern = r"[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*-[0-9a-fA-F]*"

        # Defaults, will get updated with each update. Overwritten by set_tableau_server_version
        self.version = u"10.0"
        self.api_version = u"2.3"
        self.tableau_namespace = u'http://tableau.com/api'
        self.ns_map = {'t': 'http://tableau.com/api'}
        self.ns_prefix = '{' + self.ns_map['t'] + '}'

        self.site_roles = (
            u'Interactor',
            u'Publisher',
            u'SiteAdministrator',
            u'Unlicensed',
            u'UnlicensedWithPublish',
            u'Viewer',
            u'ViewerWithPublish',
            u'ServerAdministrator'
        )

        server_content_roles_2_0 = {
                u"project": (
                    u'Viewer',
                    u'Interactor',
                    u'Editor',
                    u'Data Source Connector',
                    u'Data Source Editor',
                    u'Publisher',
                    u'Project Leader'
                ),
                u"workbook": (
                    u'Viewer',
                    u'Interactor',
                    u'Editor'
                ),
                u"datasource": (
                    u'Data Source Connector',
                    u'Data Source Editor'
                )
            }

        server_content_roles_2_1 = {
                u"project": (
                    u'Viewer',
                    u'Publisher',
                    u'Project Leader'
                ),
                u"workbook": (
                    u'Viewer',
                    u'Interactor',
                    u'Editor'
                ),
                u"datasource": (
                    u'Editor',
                    u'Connector'
                )
            }

        self.server_content_roles = {
            u"2.0": server_content_roles_2_0,
            u"2.1": server_content_roles_2_1,
            u"2.2": server_content_roles_2_1,
            u"2.3": server_content_roles_2_1
        }

        self.server_to_rest_capability_map = {
            u'Add Comment': u'AddComment',
            u'Move': u'ChangeHierarchy',
            u'Set Permissions': u'ChangePermissions',
            u'Connect': u'Connect',
            u'Delete': u'Delete',
            u'View Summary Data': u'ExportData',
            u'Export Image': u'ExportImage',
            u'Download': u'ExportXml',
            u'Filter': u'Filter',
            u'Project Leader': u'ProjectLeader',
            u'View': u'Read',
            u'Share Customized': u'ShareView',
            u'View Comments': u'ViewComments',
            u'View Underlying Data': u'ViewUnderlyingData',
            u'Web Edit': u'WebAuthoring',
            u'Save': u'Write',
            u'all': u'all'  # special command to do everything
        }

        capabilities_2_0 = {
                u"project": (
                    u'AddComment',
                    u'ChangeHierarchy',
                    u'ChangePermissions',
                    u'Connect',
                    u'Delete',
                    u'ExportData',
                    u'ExportImage',
                    u'ExportXml',
                    u'Filter',
                    u'ProjectLeader',
                    u'Read',
                    u'ShareView',
                    u'ViewComments',
                    u'ViewUnderlyingData',
                    u'WebAuthoring',
                    u'Write'
                ),
                u"workbook": (
                    u'AddComment',
                    u'ChangeHierarchy',
                    u'ChangePermissions',
                    u'Delete',
                    u'ExportData',
                    u'ExportImage',
                    u'ExportXml',
                    u'Filter',
                    u'Read',
                    u'ShareView',
                    u'ViewComments',
                    u'ViewUnderlyingData',
                    u'WebAuthoring',
                    u'Write'
                ),
                u"datasource": (
                    u'ChangePermissions',
                    u'Connect',
                    u'Delete',
                    u'ExportXml',
                    u'Read',
                    u'Write'
                )
            }

        capabilities_2_1 = {
                u"project": (u"Read", u"Write", u'ProjectLeader'),
                u"workbook": (
                    u'Read',
                    u'ExportImage',
                    u'ExportData',
                    u'ViewComments',
                    u'AddComment',
                    u'Filter',
                    u'ViewUnderlyingData',
                    u'ShareView',
                    u'WebAuthoring',
                    u'Write',
                    u'ExportXml',
                    u'ChangeHierarchy',
                    u'Delete',
                    u'ChangePermissions',

                ),
                u"datasource": (
                    u'Read',
                    u'Connect',
                    u'Write',
                    u'ExportXml',
                    u'Delete',
                    u'ChangePermissions'
                )
            }

        self.available_capabilities = {
            u"2.0": capabilities_2_0,
            u"2.1": capabilities_2_1,
            u"2.2": capabilities_2_1,
            u'2.3': capabilities_2_1
        }

        self.datasource_class_map = {
            u"Actian Vectorwise": u"vectorwise",
            u"Amazon EMR": u"awshadoophive",
            u"Amazon Redshift": u"redshift",
            u"Aster Database": u"asterncluster",
            u"Cloudera Hadoop": u"hadoophive",
            u"DataStax Enterprise": u"datastax",
            u"EXASolution": u"exasolution",
            u"Firebird": u"firebird",
            u"Generic ODBC": u"genericodbc",
            u"Google Analytics": u"google-analytics",
            u"Google BigQuery": u"bigquery",
            u"Hortonworks Hadooop Hive": u"hortonworkshadoophive",
            u"HP Vertica": u"vertica",
            u"IBM BigInsights": u"bigsql",
            u"IBM DB2": u"db2",
            u"JavaScript Connector": u"jsconnector",
            u"MapR Hadoop Hive": u"maprhadoophive",
            u"MarkLogic": u"marklogic",
            u"Microsoft Access": u"msaccess",
            u"Microsoft Analysis Services": u"msolap",
            u"Microsoft Excel": u"",
            u"Microsoft PowerPivot": u"powerpivot",
            u"Microsoft SQL Server": u"sqlserver",
            u"MySQL": u"mysql",
            u"IBM Netezza": u"netezza",
            u"OData": u"odata",
            u"Oracle": u"oracle",
            u"Oracle Essbase": u"essbase",
            u"ParAccel": u"paraccel",
            u"Pivotal Greenplum": u"greenplum",
            u"PostgreSQL": u"postgres",
            u"Progress OpenEdge": u"progressopenedge",
            u"SAP HANA": u"saphana",
            u"SAP Netweaver Business Warehouse": u"sapbw",
            u"SAP Sybase ASE": u"sybasease",
            u"SAP Sybase IQ": u"sybaseiq",
            u"Salesforce": u"salesforce",
            u"Spark SQL": u"spark",
            u"Splunk": u"splunk",
            u"Statistical File": u"",
            u"Tableau Data Extract": u"dataengine",
            u"Teradata": u"teradata",
            u"Text file": u"csv"
        }

        self.permissionable_objects = (u'datasource', u'project', u'workbook')

    def set_tableau_server_version(self, tableau_server_version):
        """
        :type tableau_server_version: unicode
        """
        # API Versioning (starting in 9.2)
        if unicode(tableau_server_version)in [u"9.2", u"9.3", u"10.0"]:
            if unicode(tableau_server_version) == u"9.2":
                self.api_version = u"2.1"
            elif unicode(tableau_server_version) == u"9.3":
                self.api_version = u"2.2"
            elif unicode(tableau_server_version) == u'10.0':
                self.api_version = u'2.3'
            self.tableau_namespace = u'http://tableau.com/api'
            self.ns_map = {'t': 'http://tableau.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
        elif unicode(tableau_server_version) in [u"9.0", u"9.1"]:
            self.api_version = u"2.0"
            self.tableau_namespace = u'http://tableausoftware.com/api'
            self.ns_map = {'t': 'http://tableausoftware.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
        else:
            raise InvalidOptionException(u"Please specify tableau_server_version as a string. '9.0' or '9.2' etc...")

    # Logging Methods
    def enable_logging(self, logger_obj):
        if isinstance(logger_obj, Logger):
            self.logger = logger_obj

    def log(self, l):
        if self.logger is not None:
            self.logger.log(l)

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
        if isinstance(x, (str, unicode)):
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
        random_digits = [random.SystemRandom().choice('0123456789abcdef') for n in xrange(30)]
        s = "".join(random_digits)
        return s

    # URI is different form actual URL you need to load a particular view in iframe
    @staticmethod
    def convert_view_content_url_to_embed_url(content_url):
        split_url = content_url.split('/')
        return 'views/' + split_url[0] + "/" + split_url[2]

    # Generic method for XML lists for the "query" actions to name -> id dict
    @staticmethod
    def convert_xml_list_to_name_id_dict(lxml_obj):
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
            raise InvalidOptionException(u'{} is not a permission name on the Tableau Server'.format(permission_name))

    # 32 hex characters with 4 dashes
    def is_luid(self, val):
        if len(val) == 36:
            if re.match(self.luid_pattern, val) is not None:
                return True
            else:
                return False
        else:
            return False

    # Looks at LUIDs in new_obj_list, if they exist in the dest_obj, compares their gcap objects, if match returns True
    def are_capabilities_objs_identical_for_matching_luids(self, new_obj_list, dest_obj_list):
        self.start_log_block()
        # Create a dict with the LUID as the keys for sorting and comparison
        new_obj_dict = {}
        for obj in new_obj_list:
            new_obj_dict[obj.get_luid()] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.get_luid()] = obj

        new_obj_luids = new_obj_dict.keys()
        dest_obj_luids = dest_obj_dict.keys()

        if set(dest_obj_luids).issuperset(new_obj_luids):
            # At this point, we know the new_objs do exist on the current obj, so let's see if they are identical
            for luid in new_obj_luids:
                new_obj = new_obj_dict.get(luid)
                dest_obj = dest_obj_dict.get(luid)

                self.log(u"Capabilities to be set:")
                new_obj_cap_dict = new_obj.get_capabilities_dict()
                self.log(unicode(new_obj_cap_dict))
                self.log(u"Capabilities that were originally set:")
                dest_obj_cap_dict = dest_obj.get_capabilities_dict()
                self.log(unicode(dest_obj_cap_dict))
                if new_obj_cap_dict == dest_obj_cap_dict:
                    self.end_log_block()
                    return True
                else:
                    self.end_log_block()
                    return False
        else:
            self.end_log_block()
            return False

    # Determine if capabilities are already set identically (or identically enough) to skip
    def are_capabilities_obj_lists_identical(self, new_obj_list, dest_obj_list):
        # Grab the LUIDs of each, determine if they match in the first place

        # Create a dict with the LUID as the keys for sorting and comparison
        new_obj_dict = {}
        for obj in new_obj_list:
            new_obj_dict[obj.get_luid()] = obj

        dest_obj_dict = {}
        for obj in dest_obj_list:
            dest_obj_dict[obj.get_luid()] = obj
            # If lengths don't match, they must differ
            if len(new_obj_dict) != len(dest_obj_dict):
                return False
            else:
                # If LUIDs don't match, they must differ
                new_obj_luids = new_obj_dict.keys()
                dest_obj_luids = dest_obj_dict.keys()
                new_obj_luids.sort()
                dest_obj_luids.sort()
                if cmp(new_obj_luids, dest_obj_luids) != 0:
                    return False
                for luid in new_obj_luids:
                    new_obj = new_obj_dict.get(luid)
                    dest_obj = dest_obj_dict.get(luid)
                    return self.are_capabilities_obj_dicts_identical(new_obj.get_capabilities_dict(),
                                                                     dest_obj.get_capabilities_dict())

    @staticmethod
    def are_capabilities_obj_dicts_identical(new_obj_dict, dest_obj_dict):
        if cmp(new_obj_dict, dest_obj_dict) == 0:
            return True
        else:
            return False

    # Dict { capability_name : mode } into XML with checks for validity. Set type to 'workbook' or 'datasource'
    def build_capabilities_xml_from_dict(self, capabilities_dict, obj_type):
        if obj_type not in self.permissionable_objects:
            error_text = u'objtype can only be "project", "workbook" or "datasource", was given {}'
            raise InvalidOptionException(error_text.format(u'obj_type'))
        xml = u'<capabilities>\n'
        for cap in capabilities_dict:
            # Skip if the capability is set to None
            if capabilities_dict[cap] is None:
                continue
            if capabilities_dict[cap] not in [u'Allow', u'Deny']:
                raise InvalidOptionException(u'Capability mode can only be "Allow",  "Deny" (case-sensitive)')
            if obj_type == u'project':
                if cap not in self.available_capabilities[self.api_version][u"project"]:
                    raise InvalidOptionException(u'{} is not a valid capability for a project'.format(cap))
            if obj_type == u'datasource':
                # Ignore if not available for datasource
                if cap not in self.available_capabilities[self.api_version][u"datasource"]:
                    self.log(u'{} is not a valid capability for a datasource'.format(cap))
                    continue
            if obj_type == u'workbook':
                # Ignore if not available for workbook
                if cap not in self.available_capabilities[self.api_version][u"workbook"]:
                    self.log(u'{} is not a valid capability for a workbook'.format(cap))
                    continue
            xml += u'<capability name="{}" mode="{}" />'.format(cap, capabilities_dict[cap])
        xml += u'</capabilities>'
        return xml

