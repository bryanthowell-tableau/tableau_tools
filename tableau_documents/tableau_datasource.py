import xml.etree.ElementTree as ET
import copy
from xml.sax.saxutils import quoteattr
import datetime

import random
from typing import Union, Any, Optional, List, Dict, Tuple

# from ..tableau_base import *
from tableau_tools.tableau_exceptions import *
from tableau_tools.logger import Logger
from tableau_tools.logging_methods import LoggingMethods
from .tableau_connection import TableauConnection
from .tableau_document import TableauDocument
from .tableau_columns import TableauColumns
from .table_relations import TableRelations


# Meant to represent a TDS file, does not handle the file opening
class TableauDatasource(LoggingMethods, TableauDocument):
    def __init__(self, datasource_xml: Optional[ET.Element] = None, logger_obj: Optional[Logger] = None,
                 ds_version: Optional[str] = None):
        # TableauDocument.__init__(self)
        self.document_type = 'datasource'

        self.logger = logger_obj
        self._connections = []
        self._ds_name = None
        self.ds_version_type = None
        self._ds_version = None
        self._published = False
        self.relation_xml_obj = None
        self.existing_tde_filename = None
        self.ns_map = {"user": 'http://www.tableausoftware.com/xml/user', 't': 'http://tableau.com/api'}
        ET.register_namespace('t', self.ns_map['t'])
        self.parameters = None

        # All used for creating from scratch
        self._extract_filename = None
        self.incremental_refresh_field = None
        self.join_relations = []
        self.column_mapping = {}
        self.column_aliases = {}
        self.datasource_filters = []
        self.extract_filters = []
        self.initial_sql = None
        self.column_instances = []
        self.main_table_relation = None
        self.main_table_name = None
        self._table_relations = None
        self._connection_root = None
        self._stored_proc_parameters_xml = None

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
            "Hyper": 'hyper',
        }

        # Create from new. Only doing 10.5 style for now.
        if datasource_xml is None:
            self.ds_version_type = '10.5'
            self.xml = self.create_new_datasource_xml(ds_version)
        # Read existing data source
        else:
            self.xml = datasource_xml
            if self.xml.get("caption"):
                self._ds_name = self.xml.attrib["caption"]
            elif self.xml.get("name"):
                self._ds_name = self.xml.attrib['name']
            xml_version = self.xml.attrib['version']
            # Determine whether it is a 9 style or 10 style federated datasource
            if xml_version in ['9.0', '9.1', '9.2', '9.3']:
                self.ds_version_type = '9'
            else:
                version_split = xml_version.split('.')
                if int(version_split[0]) >= 18:
                    self.ds_version_type = '10.5'
                elif int(version_split[1]) < 5:
                    self.ds_version_type = '10'
                else:
                    self.ds_version_type = '10.5'
            self.log('Data source is Tableau {} style'.format(self.ds_version_type))

            # Create Connections
            # 9.0 style
            if self.ds_version_type == '9':
                connection_xml_obj = self.xml.find('.//connection', self.ns_map)
                # Skip the relation if it is a Parameters datasource. Eventually, build out separate object
                if connection_xml_obj is None:
                    self.log('Found a Parameters datasource')
                else:
                    self.log('connection tags found, building a TableauConnection object')
                    new_conn = TableauConnection(connection_xml_obj)
                    self.connections.append(new_conn)
                    if new_conn.connection_type == 'sqlproxy':
                        self._published = True
                        repository_location_xml = self.xml.find('repository-location')
                        self.repository_location = repository_location_xml

            # Grab the relation
            elif self.ds_version_type in ['10', '10.5']:
                named_connections = self.xml.findall('.//named-connection', self.ns_map)
                for named_connection in named_connections:
                    self.log('connection tags found, building a TableauConnection object')
                    self.connections.append(TableauConnection(named_connection))
                # Check for published datasources, which look like 9.0 style still
                published_datasources = self.xml.findall('.//connection[@class="sqlproxy"]', self.ns_map)
                for published_datasource in published_datasources:
                    self.log('Published Datasource connection tags found, building a TableauConnection object')
                    self.connections.append(TableauConnection(published_datasource))
                    self._published = True
                    repository_location_xml = self.xml.find('repository-location')
                    self.repository_location = repository_location_xml

            # Skip the relation if it is a Parameters datasource. Eventually, build out separate object
            if self.xml.get('name') != 'Parameters':
                self.relation_xml_obj = self.xml.find('.//relation', self.ns_map)
                self._table_relations = TableRelations(relation_xml_obj=self.relation_xml_obj)
            else:
                self.log('Found a Parameters datasource')

        # Grab the extract filename if there is an extract section
        if self.xml.find('extract') is not None:
            e = self.xml.find('extract')
            c = e.find('connection')
            self.existing_tde_filename = c.get('dbname')

        self._columns = None
        # Possible, though unlikely, that there would be no columns
        if self.xml.find('column') is not None:
            columns_list = self.xml.findall('column')
            self._columns = TableauColumns(columns_list, self.logger)

        self._extract_filename = None
        self.ds_generator = None

    @property
    def ds_name(self) -> str:
        return self._ds_name

    @ds_name.setter
    def ds_name(self, new_name: str):
        self.xml.set("caption", new_name)

    @property
    def tde_filename(self) -> str:
        return self._extract_filename

    @tde_filename.setter
    def tde_filename(self, tde_filename: str):
        self._extract_filename = tde_filename

    @property
    def connections(self) -> List[TableauConnection]:
        return self._connections

    @property
    def columns(self) -> TableauColumns:
        return self._columns

    @property
    def is_published(self) -> bool:
        return self._published

    @property
    def tables(self):
        return self._table_relations

    @property
    def is_stored_proc(self) -> bool:
        if self.tables.main_table.get('type') == 'stored-proc':
            return True
        else:
            return False

    @property
    def main_table_type(self) -> str:
        raw_type = self.tables.main_table.get('type')
        if raw_type == 'stored-proc':
            return 'stored-proc'
        elif raw_type == 'text':
            return 'custom-sql'
        elif raw_type == 'table':
            return 'table'

    @property
    def published_ds_site(self) -> str:
        if self.repository_location.get("site"):
            return self.repository_location.get("site")
        else:
            return 'default'

    @published_ds_site.setter
    def published_ds_site(self, new_site_content_url: str):
        self.start_log_block()
        # If it was originally the default site, you need to add the site name in front
        if self.repository_location.get("site") is None:
            self.repository_location.attrib["path"] = '/t/{}{}'.format(new_site_content_url, self.repository_location.get("path"))
        # Replace the original site_content_url with the new one
        elif self.repository_location.get("site") is not None:
            self.repository_location.attrib["path"] = self.repository_location.get("path").replace(self.repository_location.get("site"), new_site_content_url)
        self.repository_location.attrib['site'] = new_site_content_url
        self.end_log_block()

    @property
    def published_ds_content_url(self) -> Optional[str]:
        if self.repository_location.get("id"):
            return self.repository_location.get("id")
        else:
            return None

    @published_ds_content_url.setter
    def published_ds_content_url(self, new_content_url: str):
        if self.is_published is False:
            return
        else:
            self.repository_location.attrib['id'] = new_content_url
            self.connections[0].dbname = new_content_url

    # It seems some databases like Oracle and Teradata need this as well to swap a database
    def update_tables_with_new_database_or_schema(self, original_db_or_schema: str, new_db_or_schema: str):
        for relation in self.tables.table_relations:
            if relation.get('type') == "table":
                relation.set('table', relation.get('table').replace("[{}]".format(original_db_or_schema),
                                                                      "[{}]".format(new_db_or_schema)))

    # Start of data sources creation methods (from scratch)
    # Need considerable review and testing

    @staticmethod
    def create_new_datasource_xml(version: str) -> ET.Element:
        # nsmap = {u"user": u'http://www.tableausoftware.com/xml/user'}
        # The most basic component is just a datasource element with a version.
        ds_xml = ET.Element("datasource")
        ds_xml.set('version', version)
        # Unclear if this is even necessary. May only appear in TWBXs
        ds_xml.set('inline', "true")
        return ds_xml

    @staticmethod
    def create_new_connection_xml(ds_version: str, ds_type: str, server: str, db_name: str,
                                  authentication: Optional[str] = None,
                                  initial_sql: Optional[str] = None) -> ET.Element:
        connection = ET.Element("connection")
        if ds_version in ['10', '10.5']:
            nc = ET.Element('named-connection')
            nc.set('caption', 'Connection')
            # Connection has a random number of 20 digits appended
            rnumber = random.randrange(10**20, 10**21)
            nc.set('name', 'connection.{}'.format(rnumber))
            nc.append(connection)
            c = nc
        else:
            raise InvalidOptionException('ds_version must be either "9" or "10"')
        connection.set('class', ds_type)

        connection.set('dbname', db_name)
        connection.set('odbc-native-protocol', 'yes') # is this always necessary, or just PostgreSQL?
        if server is not None:
            connection.set('server', server)
        if authentication is not None:
            connection.set('authentication', authentication)
        if initial_sql is not None:
            connection.set('one-time-sql', initial_sql)
        return c

    def add_new_connection(self, ds_type: str, server: Optional[str] = None,
                           db_or_schema_name: Optional[str] = None, authentication: Optional[str] = None,
                           initial_sql: Optional[str] = None) -> TableauConnection:
        self.start_log_block()
        self.ds_generator = True
        conn = self.create_new_connection_xml(self.ds_version_type, ds_type, server,
                                              db_or_schema_name, authentication, initial_sql)
        print((ET.tostring(conn)))
        if self.ds_version_type == '9':
            self.xml.append(conn)
            self._connection_root = conn
        elif self.ds_version_type in ['10', '10.5']:
            c = ET.Element('connection')
            c.set('class', 'federated')
            ncs = ET.Element('named-connections')
            ncs.append(copy.deepcopy(conn))
            c.append(ncs)
            self.xml.append(c)
            self._connection_root = c
        else:
            raise InvalidOptionException('ds_version of TableauDatasource must be u"9" or u"10" ')
        new_conn = TableauConnection(conn)
        self.connections.append(new_conn)

        self.end_log_block()
        return new_conn

    def add_new_hyper_file_connection(self, hyper_filename_no_path: str) -> TableauConnection:
        conn_obj = self.add_new_connection(ds_type='hyper', db_or_schema_name='Data/{}'.format(hyper_filename_no_path),
                                     authentication='auth-none')
        conn_obj.username = 'tableau_internal_user'
        return conn_obj

    def get_xml_string(self) -> str:
        # The TableauDatasource object basically stores all properties separately and doesn't actually create
        # the final XML until this function is called.
        self.start_log_block()
        self.log('Generating datasource xml')

        # Generate a copy of the XML object that was originally passed in

        if self.xml is not None:
           new_xml = copy.deepcopy(self.xml)
        # Run through and generate any new sections to be added from the datasource_generator

        # Column Mappings

        # Column Aliases
        if self.ds_generator is not None:
            connection_root = new_xml.find('.//connection', self.ns_map)
            named_connection = connection_root.find('.//named-connection', self.ns_map)
            connection_name = named_connection.get('name')
            new_rel_xml = self.tables.generate_relation_section(connection_name=connection_name)
            connection_root.append(new_rel_xml)
            cas = self.generate_aliases_column_section()
            # If there is no existing aliases tag, gotta add one. Unlikely but safety first
            if len(cas) > 0 and new_xml.find('aliases') is False:
                new_xml.append(self.generate_aliases_tag())
            for c in cas:
                self.log('Appending the column alias XML')
                new_xml.append(c)
            # Column Instances
            cis = self.generate_column_instances_section()
            for ci in cis:
                self.log('Appending the column-instances XML')
                new_xml.append(ci)

        # Datasource Filters Can be added no matter what
        dsf = self.generate_datasource_filters_section()
        self.log('Appending the ds filters to existing XML')
        for f in dsf:
            new_xml.append(f)
        # Extracts
        if self.tde_filename is not None:
            # Extract has to be in a sort of order it appears, before the layout and semantic-values nodes
            # Try and remove them if possible
            l = new_xml.find("layout")
            new_l = copy.deepcopy(l)
            new_xml.remove(l)

            sv = new_xml.find('semantic-values')
            new_sv = copy.deepcopy(sv)
            new_xml.remove(sv)
            # chop, then readd

            self.log('Generating the extract and XML object related to it')
            extract_xml = self.generate_extract_section()
            self.log('Appending the new extract XML to the existing XML')
            new_xml.append(extract_xml)

            new_xml.append(new_l)
            new_xml.append(new_sv)

        xmlstring = ET.tostring(new_xml)
        self.end_log_block()
        return xmlstring


    def translate_columns(self, translation_dict: Dict):
        self.start_log_block()
        self.columns.translate_captions(translation_dict=translation_dict)
        self.end_log_block()

    def generate_extract_section(self) -> Union[ET.Element, bool]:
        # Short circuit if no extract had been set
        if self._extract_filename is None:
            self.log('No extract_filename, no extract being added')
            return False
        self.log('Importing the Tableau SDK to build the extract')

        # Import only if necessary
        self.log('Building the extract Element object')
        try:
            if self.ds_version_type == '10.5':
                from .hyper_file_generator import HyperFileGenerator
            else:
                raise InvalidOptionException(
                    'tableau_tools 5.0 is Python 3.6+ compatible, but the TDE generating library is only available for Python 2.7. Please upgrade to a newer version of Tableau with Hyper extract engine (10.5+)')
        except Exception as ex:
            print("Must have correct install of Tableau Extract SDK to add extracts")
            print('Exception arising from the Tableau Extract SDK itself')
            raise
        e = ET.Element('extract')
        e.set('count', '-1')
        e.set('enabled', 'true')
        e.set('units', 'records')

        c = ET.Element('connection')
        if self.ds_version_type in ['9', '10']:
            c.set('class', 'dataengine')
        # 10.5 has hyper data sources instead
        if self.ds_version_type in ['10.5']:
            c.set('class', 'hyper')
            #c.set(u'class', u'tdescan')
            c.set('username', 'tableau_internal_user')
            c.set('access_mode', 'readonly')
            c.set('authentication', 'auth-none')
            c.set('default-settings', 'yes')
            c.set('sslmode', '')
        c.set('dbname', 'Data/Datasources/{}'.format(self._extract_filename))
        c.set('schema', 'Extract')
        c.set('tablename', 'Extract')
        right_now = datetime.datetime.now()
        pretty_date = right_now.strftime("%m/%d/%Y %H:%M:%S %p")
        c.set('update-time', pretty_date)

        r = ET.Element('relation')
        r.set("name", "Extract")
        r.set("table", "[Extract].[Extract]")
        r.set("type", "table")
        c.append(r)

        calcs = ET.Element("calculations")
        calc = ET.Element("calculation")
        calc.set("column", "[Number of Records]")
        calc.set("formula", "1")
        calcs.append(calc)
        c.append(calcs)

        ref = ET.Element('refresh')
        if self.incremental_refresh_field is not None:
            ref.set("increment-key", self.incremental_refresh_field)
            ref.set("incremental-updates", 'true')
        elif self.incremental_refresh_field is None:
            ref.set("increment-key", "")
            ref.set("incremental-updates", 'false')

        c.append(ref)

        e.append(c)

        tde_columns = {}
        self.log('Creating the extract filters')
        if len(self.extract_filters) > 0:
            filters = self.generate_filters(self.extract_filters)
            for f in filters:
                e.append(f)
            # Any column in the extract filters needs to exist in the TDE file itself
            if len(self.extract_filters) > 0:
                for f in self.extract_filters:
                    # Check to see if column_name is actually an instance
                    field_name = f['column_name']
                    for col_instance in self.column_instances:
                        if field_name == col_instance['name']:
                            field_name = col_instance['column']
                    # Simple heuristic for determining type from the first value in the values array
                    if f['type'] == 'categorical':
                        first_value = f['values'][0]
                        if isinstance(first_value, str):
                            filter_column_tableau_type = 'str'
                        else:
                            filter_column_tableau_type = 'int'
                    elif f['type'] == 'relative-date':
                        filter_column_tableau_type = 'datetime'
                    elif f['type'] == 'quantitative':
                        # Time values passed in with strings
                        if isinstance(f['max'], str) or isinstance(f['min'], str):
                            filter_column_tableau_type = 'datetime'
                        else:
                            filter_column_tableau_type = 'int'
                    else:
                        raise InvalidOptionException('{} is not a valid type'.format(f['type']))
                    tde_columns[field_name[1:-1]] = filter_column_tableau_type
        else:
            self.log('Creating TDE with only one field, "Generic Field", of string type')
            tde_columns['Generic Field'] = 'str'

        self.log('Using the Extract SDK to build an empty extract file with the right definition')
        if self.ds_version_type == '10.5':
            from .hyper_file_generator import HyperFileGenerator
            extract_file_generator = HyperFileGenerator(self.logger)
            extract_file_generator.set_table_definition(tde_columns)
            extract_file_generator.create_extract(self.tde_filename)
        else:
            raise InvalidOptionException('tableau_tools 5.0 is Python 3.6+ compatible, but the TDE generating library is only available for Python 2.7. Please upgrade to a newer version of Tableau with Hyper extract engine (10.5+)')
        return e



    def add_table_column(self, table_alias: str, table_field_name: str, tableau_field_alias: str):
        # Check to make sure the alias has been added

        # Check to make sure the tableau_field_alias hasn't been used already

        self.column_mapping[tableau_field_alias] = "[{}].[{}]".format(table_alias, table_field_name)

    def add_column_alias(self, tableau_field_alias: str, caption: Optional[str] = None,
                         dimension_or_measure: Optional[str] = None,
                         discrete_or_continuous: Optional[str] = None, datatype: Optional[str] = None,
                         calculation: Optional[str] = None):
        if dimension_or_measure.lower() in ['dimension', 'measure']:
            role = dimension_or_measure.lower()
        else:
            raise InvalidOptionException("{} should be either measure or dimension".format(dimension_or_measure))

        if discrete_or_continuous.lower() in ['discrete', 'continuous']:
            if discrete_or_continuous.lower() == 'discrete':
                if datatype.lower() in ['string']:
                    t_type = 'nominal'
                else:
                    t_type = 'ordinal'
            elif discrete_or_continuous.lower() == 'continuous':
                t_type = 'quantitative'
        else:
            raise InvalidOptionException("{} should be either discrete or continuous".format(discrete_or_continuous))

        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))

        self.column_aliases[tableau_field_alias] = {"caption": caption,
                                                    "type": t_type,
                                                    "datatype": datatype.lower(),
                                                    "role": role,
                                                    "calculation": calculation}

    def add_calculation(self, calculation: str, calculation_name: str, dimension_or_measure: str,
                        discrete_or_continuous: str, datatype: str):
        internal_calc_name = self.create_random_calculation_name()
        self.add_column_alias(internal_calc_name, calculation_name, dimension_or_measure, discrete_or_continuous,
                              datatype, calculation)
        # internal_calc_name allows you to create a filter on this
        return internal_calc_name

    @staticmethod
    def create_dimension_filter(column_name: str, values: List[str], include_or_exclude: str = 'include',
                                custom_value_list: bool = False):
        # Check if column_name is actually the alias of a calc, if so, replace with the random internal calc name

        if include_or_exclude.lower() in ['include', 'exclude']:
            if include_or_exclude.lower() == 'include':
                ui_enumeration = 'inclusive'
            elif include_or_exclude.lower() == 'exclude':
                ui_enumeration = 'exclusive'
            else:
                ui_enumeration = 'inclusive'
        else:
            raise InvalidOptionException('{} is not valid, must be include or exclude'.format(include_or_exclude))
        ds_filter = {
                     "type": 'categorical',
                     'column_name': '[{}]'.format(column_name),
                     "values": values,
                     'ui-enumeration': ui_enumeration,
                     'ui-manual-selection': custom_value_list
                    }
        return ds_filter

    def add_dimension_datasource_filter(self, column_name, values, include_or_exclude='include',
                                        custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.datasource_filters.append(ds_filter)

    def add_dimension_extract_filter(self, column_name, values, include_or_exclude='include', custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.extract_filters.append(ds_filter)

    def add_continuous_datasource_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value, date=date)
        self.datasource_filters.append(ds_filter)

    def add_continuous_extract_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value ,date=date)
        self.extract_filters.append(ds_filter)

    def add_relative_date_datasource_filter(self, column_name, period_type, number_of_periods=None,
                                            previous_next_current='previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current
                                                     , to_date)
        self.datasource_filters.append(ds_filter)

    def add_relative_date_extract_filter(self, column_name, period_type, number_of_periods=None,
                                         previous_next_current='previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current,
                                                     to_date)
        self.extract_filters.append(ds_filter)

    def create_continuous_filter(self, column_name: str, min_value=None, max_value=None, date=False) -> Dict:
        # Dates need to be wrapped in # #
        if date is True:
            if min_value is not None:
                min_value = '#{}#'.format(str(min_value))
            if max_value is not None:
                max_value = '#{}#'.format(str(max_value))
            final_column_name = '[none:{}:qk]'.format(column_name)
            # Need to create a column-instance tag in the columns section
            self.column_instances.append(
                {
                    'column': '[{}]'.format(column_name),
                    'name': final_column_name,
                    'type': 'quantitative'
                }
            )
        else:
            final_column_name = '[{}]'.format(column_name)
        ds_filter = {
            'type': 'quantitative',
            'min': min_value,
            'max': max_value,
            'column_name': final_column_name,
        }
        return ds_filter

    def create_relative_date_filter(self, column_name: str, period_type, number_of_periods,
                                    previous_next_current='previous', to_date=False):
        if period_type.lower() not in ['quarter', 'year', 'month', 'day', 'hour', 'minute']:
            raise InvalidOptionException('period_type must be one of : quarter, year, month, day, hour, minute')
        # Need to create a column-instance tag in the columns section
        final_column_name = '[none:{}:qk]'.format(column_name)
        # Need to create a column-instance tag in the columns section
        self.column_instances.append(
            {
                'column': '[{}]'.format(column_name),
                'name': final_column_name,
                'type': 'quantitative'
            }
        )
        if previous_next_current.lower() == 'previous':
            first_period = '-{}'.format(str(number_of_periods))
            last_period = '0'
        elif previous_next_current.lower() == 'next':
            first_period = '1'
            last_period = '{}'.format(str(number_of_periods))
        elif previous_next_current.lower() == 'current':
            first_period = '0'
            last_period = '0'
        else:
            raise InvalidOptionException('You must use "previous", "next" or "current" for the period selections')

        if to_date is False:
            include_future = 'true'
        elif to_date is True:
            include_future = 'false'

        ds_filter = {
            'type': 'relative-date',
            'column_name': final_column_name,
            'first-period': first_period,
            'last-period': last_period,
            'period-type': period_type.lower(),
            'include-future': include_future
        }
        return ds_filter

    def generate_filters(self, filter_list: List[Dict]) -> List[ET.Element]:
        return_array = []
        for filter_def in filter_list:
            f = ET.Element('filter')
            f.set('class', filter_def['type'])
            f.set('column', filter_def['column_name'])
            f.set('filter-group', '2')
            if filter_def['type'] == 'quantitative':
                f.set('include-values', 'in-range')
                if filter_def['min'] is not None:
                    m = ET.Element('min')
                    m.text = str(filter_def['min'])
                    f.append(m)
                if filter_def['max'] is not None:
                    m = ET.Element('max')
                    m.text = str(filter_def['max'])
                    f.append(m)
            elif filter_def['type'] == 'relative-date':
                f.set('first-period', filter_def['first-period'])
                f.set('include-future', filter_def['include-future'])
                f.set('last-period', filter_def['last-period'])
                f.set('include-null', 'false')
                f.set('period-type', filter_def['period-type'])

            elif filter_def['type'] == 'categorical':
                gf = ET.Element('groupfilter')
                # This attribute has a user namespace
                gf.set('{' + '{}'.format(self.ns_map['user']) + '}ui-domain', 'database')
                gf.set('{' + '{}'.format(self.ns_map['user']) + '}ui-enumeration', filter_def['ui-enumeration'])
                gf.set('{' + '{}'.format(self.ns_map['user']) + '}ui-marker', 'enumerate')
                if filter_def['ui-manual-selection'] is True:
                    gf.set('{' + '{}'.format(self.ns_map['user']) + '}ui-manual-selection', 'true')
                if len(filter_def['values']) == 1:
                    if filter_def['ui-enumeration'] == 'exclusive':
                        gf.set('function', 'except')
                        gf1 = ET.Element('groupfilter')
                        gf1.set('function', 'member')
                        gf1.set('level', filter_def['column_name'])
                    else:
                        gf.set('function', 'member')
                        gf.set('level', filter_def['column_name'])
                        gf1 = gf
                    # strings need the &quot;, ints do not
                    if isinstance(filter_def['values'][0], str):
                        gf1.set('member', quoteattr(filter_def['values'][0]))
                    else:
                        gf1.set('member', str(filter_def['values'][0]))
                    if filter_def['ui-enumeration'] == 'exclusive':
                        # Single exclude filters include an extra groupfilter set with level-members function
                        lm = ET.Element('groupfilter')
                        lm.set('function', 'level-members')
                        lm.set('level', filter_def['column_name'])
                        gf.append(lm)
                        gf.append(gf1)
                    f.append(gf)
                else:
                    if filter_def['ui-enumeration'] == 'exclusive':
                        gf.set('function', 'except')
                    else:
                        gf.set('function', 'union')
                    for val in filter_def['values']:
                        gf1 = ET.Element('groupfilter')
                        gf1.set('function', 'member')
                        gf1.set('level', filter_def['column_name'])
                        # String types need &quot; , ints do not
                        if isinstance(val, str):
                            gf1.set('member', quoteattr(val))
                        else:
                            gf1.set('member', str(val))
                        gf.append(gf1)
                    f.append(gf)
            return_array.append(f)
        return return_array

    def generate_datasource_filters_section(self) -> List[ET.Element]:
        filters = self.generate_filters(self.datasource_filters)
        filters_array = []
        for f in filters:
            filters_array.append(f)
        return filters_array

    def generate_cols_map_section(self) -> bool:
        if len(self.column_mapping) == 0:
            return False
        c = ET.Element("cols")
        for key in self.column_mapping:
            m = ET.Element("map")
            m.set("key", "[{}]".format(key))
            m.set("value", self.column_mapping[key])
            c.append(m)
        self.xml.append(c)
        return True

    @staticmethod
    def generate_aliases_tag() -> ET.Element:
        # For whatever reason, the aliases tag does not contain the columns, but it always precedes it
        a = ET.Element("aliases")
        a.set("enabled", "yes")
        return a

    def generate_aliases_column_section(self) -> List[ET.Element]:
        column_aliases_list = []

        # Now to put in each column tag
        for column_alias in self.column_aliases:
            c = ET.Element("column")
            print("Column section")
            print((ET.tostring(c)))
            # Name is the Tableau Field Alias, always surrounded by brackets SQL Server style
            c.set("name", "[{}]".format(column_alias))
            print("Column section 2")
            print((ET.tostring(c)))
            if self.column_aliases[column_alias]["datatype"] is not None:
                c.set("datatype", self.column_aliases[column_alias]["datatype"])
            if self.column_aliases[column_alias]["caption"] is not None:
                c.set("caption", self.column_aliases[column_alias]["caption"])
            if self.column_aliases[column_alias]["role"] is not None:
                c.set("role", self.column_aliases[column_alias]["role"])
            if self.column_aliases[column_alias]["type"] is not None:
                c.set("type", self.column_aliases[column_alias]["type"])
            if self.column_aliases[column_alias]['calculation'] is not None:
                calc = ET.Element('calculation')
                calc.set('class', 'tableau')
                # quoteattr adds an extra real set of quotes around the string, which needs to be sliced out
                calc.set('formula', quoteattr(self.column_aliases[column_alias]['calculation'])[1:-1])
                c.append(calc)
            print("Column section at end")
            print((ET.tostring(c)))
            column_aliases_list.append(c)
        return column_aliases_list

    def generate_column_instances_section(self) -> List[ET.Element]:
        column_instances_list = []
        for column_instance in self.column_instances:
            ci = ET.Element('column-instance')
            ci.set('column', column_instance['column'])
            ci.set('derivation', 'None')
            ci.set('name', column_instance['name'])
            ci.set('pivot', 'key')
            ci.set('type', column_instance['type'])
            column_instances_list.append(ci)
        return column_instances_list
