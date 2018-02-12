from ..tableau_base import *
from tableau_connection import TableauConnection
from tableau_document import TableauColumns, TableauDocument

import xml.etree.cElementTree as etree
from ..tableau_exceptions import *
import zipfile
import os
import copy
from xml.sax.saxutils import quoteattr
import datetime
import codecs


# Meant to represent a TDS file, does not handle the file opening
class TableauDatasource(TableauDocument):
    def __init__(self, datasource_xml=None, logger_obj=None, ds_version=None):
        """
        :type datasource_xml: etree.Element
        :type logger_obj: Logger
        """
        TableauDocument.__init__(self)
        self._document_type = u'datasource'
        etree.register_namespace(u't', self.ns_map['t'])
        self.logger = logger_obj
        self._connections = []
        self.ds_name = None
        self.ds_version = None
        self._published = False
        self.relation_xml_obj = None
        self.existing_tde_filename = None
        self.nsmap = {u"user": u'http://www.tableausoftware.com/xml/user'}

        # All used for creating from scratch
        self._tde_filename = None
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

        # Create from new or from existing object
        if datasource_xml is None:
            self.xml = self.create_new_datasource_xml()
            if ds_version is None:
                self.ds_version = u'10'
            else:
                self.ds_version = ds_version
        else:
            self.xml = datasource_xml
            if self.xml.get(u"caption"):
                self.ds_name = self.xml.attrib[u"caption"]
            elif self.xml.get(u"name"):
                self.ds_name = self.xml.attrib[u'name']
            xml_version = self.xml.attrib[u'version']
            # Determine whether it is a 9 style or 10 style federated datasource
            if xml_version in [u'9.0', u'9.1', u'9.2', u'9.3']:
                self.ds_version = u'9'
            else:
                self.ds_version = u'10'
            self.log(u'Data source is Tableau {} style'.format(self.ds_version))

            # Create Connections
            # 9.0 style
            if self.ds_version == u'9':

                connection_xml_obj = self.xml.find(u'.//connection', self.ns_map)
                self.log(u'connection tags found, building a TableauConnection object')
                new_conn = TableauConnection(connection_xml_obj)
                self.connections.append(new_conn)

                # Grab the relation
            elif self.ds_version == u'10':
                named_connections = self.xml.findall(u'.//named-connection', self.ns_map)
                for named_connection in named_connections:
                    self.log(u'connection tags found, building a TableauConnection object')
                    self.connections.append(TableauConnection(named_connection))
                # Check for published datasources, which look like 9.0 style still
                published_datasources = self.xml.findall(u'.//connection[@class="sqlproxy"]', self.ns_map)
                for published_datasource in published_datasources:
                    self.log(u'Published Datasource connection tags found, building a TableauConnection object')
                    self.connections.append(TableauConnection(published_datasource))
            self.relation_xml_obj = self.xml.find(u'.//relation', self.ns_map)
            self.read_existing_relations()

        self.repository_location = None

        if self.xml.find(u'repository-location') is not None:
            if len(self.xml.find(u'repository-location')) == 0:
                self._published = True
                repository_location_xml = self.xml.find(u'repository-location')
                self.repository_location = repository_location_xml

        # Grab the extract filename if there is an extract section
        if self.xml.find(u'extract') is not None:
            e = self.xml.find(u'extract')
            c = e.find(u'connection')
            self.existing_tde_filename = c.get(u'dbname')

        # To make work as tableau_document from TableauFile
        self._datasources.append(self)

        self._columns = None
        # Possible, though unlikely, that there would be no columns
        if self.xml.find(u'column') is not None:
            columns_list = self.xml.findall(u'column')
            self._columns = TableauColumns(columns_list, self.logger)

        self._tde_filename = None
        self.ds_generator = None

    @property
    def tde_filename(self):
        """
        :rtype: unicode
        """
        return self._tde_filename

    @tde_filename.setter
    def tde_filename(self, tde_filename):
        self._tde_filename = tde_filename

    @property
    def connections(self):
        """
        :rtype: list[TableauConnection]
        """
        return self._connections

    @property
    def columns(self):
        """
        :rtype: TableauColumns
        """
        return self._columns

    @property
    def published(self):
        """
        :rtype: bool
        """
        return self._published

    @property
    def published_ds_site(self):
        """
        :rtype: unicode
        """
        if self.repository_location.get(u"site"):
            return self.repository_location.get(u"site")
        else:
            return u'default'

    @published_ds_site.setter
    def published_ds_site(self, new_site_content_url):
        """
        :type new_site_content_url: unicode
        :return:
        """
        self.start_log_block()
        # If it was originally the default site, you need to add the site name in front
        if self.repository_location.get(u"site") is None:
            self.repository_location.attrib[u"path"] = u'/t/{}{}'.format(new_site_content_url, self.repository_location.get(u"path"))
        # Replace the original site_content_url with the new one
        elif self.repository_location.get(u"site") is not None:
            self.repository_location.attrib[u"path"] = self.repository_location.get(u"path").replace(self.repository_location.get(u"site"), new_site_content_url)
        self.repository_location.attrib[u'site'] = new_site_content_url
        self.end_log_block()

    @property
    def published_ds_content_url(self):
        """
        :rtype: unicode
        """
        if self.repository_location.get(u"id"):
            return self.repository_location.get(u"id")
        else:
            return None

    @published_ds_content_url.setter
    def published_ds_content_url(self, new_content_url):
        """
        :type new_content_url: unicode
        :return:
        """
        if self.published is False:
            return
        else:
            self.repository_location.attrib[u'id'] = new_content_url
            self.connections[0].dbname = new_content_url

    @staticmethod
    def create_new_datasource_xml():
        # nsmap = {u"user": u'http://www.tableausoftware.com/xml/user'}
        ds_xml = etree.Element(u"datasource")
        return ds_xml

    @staticmethod
    def create_new_connection_xml(ds_version, ds_type, server, db_name, authentication=None, initial_sql=None):
        connection = etree.Element(u"connection")
        if ds_version == u'9':
            c = connection
        elif ds_version == u'10':
            nc = etree.Element(u'named-connection')
            nc.set(u'caption', u'Connection')
            nc.set(u'name', u'connection.{}'.format(u'1912381971719892841')) # add in real random generated num
            nc.append(connection)
            c = nc
        else:
            raise InvalidOptionException(u'ds_version must be either "9" or "10"')
        connection.set(u'class', ds_type)
        connection.set(u'dbname', db_name)
        connection.set(u'odbc-native-protocol', u'yes') # is this always necessary, or just PostgreSQL?
        connection.set(u'server', server)
        if authentication is not None:
            connection.set(u'authentication', authentication)
        if initial_sql is not None:
            connection.set(u'one-time-sql', initial_sql)
        return c

    def add_new_connection(self, ds_type, server, db_or_schema_name, authentication=None, initial_sql=None):
        self.start_log_block()
        conn = self.create_new_connection_xml(self.ds_version, ds_type, server, db_or_schema_name, authentication, initial_sql)
        if self.ds_version == u'9':
            self.xml.append(conn)
        elif self.ds_version == u'10':
            c = etree.Element(u'connection')
            c.set(u'class', u'federated')
            ncs = etree.Element(u'named-connections')
            ncs.append(conn)
            c.append(ncs)
            self.xml.append(c)
        else:
            raise InvalidOptionException(u'ds_version of TableauDatasource must be u"9" or u"10" ')
        self.connections.append(TableauConnection(conn))

        self.end_log_block()

    def get_datasource_xml(self):
        self.start_log_block()
        self.log(u'Generating datasource xml')

        # Run through and generate any new sections to be added from the datasource_generator

        # Column Mappings

        # Column Aliases
        if self.ds_generator is not None:
            cas = self.ds_generator.generate_aliases_column_section()
            # If there is no existing aliases tag, gotta add one. Unlikely but safety first
            if len(cas) > 0 and self.xml.find('aliases') is False:
                self.xml.append(self.ds_generator.generate_aliases_tag())
            for c in cas:
                self.log(u'Appending the column alias XML')
                self.xml.append(c)
            # Column Instances
            cis = self.ds_generator.generate_column_instances_section()
            for ci in cis:
                self.log(u'Appending the column-instances XML')
                self.xml.append(ci)
            # Datasource Filters
            dsf = self.ds_generator.generate_datasource_filters_section()
            self.log(u'Appending the ds filters to existing XML')
            for f in dsf:
                self.xml.append(f)
        # Extracts
        if self.tde_filename is not None:
            # Extract has to be in a sort of order it appears, before the layout and semantic-values nodes
            # Try and remove them if possible
            l = self.xml.find(u"layout")
            new_l = copy.deepcopy(l)
            self.xml.remove(l)

            sv = self.xml.find(u'semantic-values')
            new_sv = copy.deepcopy(sv)
            self.xml.remove(sv)
            # chop, then readd

            self.log(u'Generating the extract and XML object related to it')
            extract_xml = self.generate_extract_section()
            self.log(u'Appending the new extract XML to the existing XML')
            self.xml.append(extract_xml)

            self.xml.append(new_l)
            self.xml.append(new_sv)

        xmlstring = etree.tostring(self.xml, encoding="unicode")
        self.end_log_block()
        return xmlstring

    def save_file(self, filename_no_extension, save_to_directory=None):
        """
        :param filename_no_extension: Filename to save the XML to. Will append .tds if not found
        :type filename_no_extension: unicode
        :type save_to_directory: unicode
        :rtype: bool
        """
        self.start_log_block()
        file_extension = u'.tds'
        #if self.tde_filename is not None:
        #    file_extension = u'.tdsx'
        try:
            # In case the .tds gets passed in from earlier
            filename_no_extension = filename_no_extension.split(u'.tds')[0]
            tds_filename = filename_no_extension + u'.tds'
            if save_to_directory is not None:
                lh = codecs.open(save_to_directory + tds_filename, 'w', encoding='utf-8')
            else:
                lh = codecs.open(tds_filename, 'w', encoding='utf-8')
            lh.write(self.get_datasource_xml())
            lh.close()

            # Handle all of this in the TableauFile object now

            #if file_extension == u'.tdsx':
            #    zf = zipfile.ZipFile(save_to_directory + filename_no_extension + u'.tdsx', 'w')
            #    if save_to_directory is not None:
            #        zf.write(save_to_directory + tds_filename, u'/{}'.format(tds_filename))
            #    else:
            #        zf.write(tds_filename, u'/{}'.format(tds_filename))
            #    # Delete temporary TDS at some point
            #    zf.write(self.tde_filename, u'/Data/Datasources/{}'.format(self.tde_filename))
            #    zf.close()
            #    # Remove the temp tde_file that is created
            #    os.remove(self.tde_filename)
            #    return True
        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

    def translate_columns(self, translation_dict):
        self.start_log_block()
        self.columns.set_translation_dict(translation_dict)
        self.columns.translate_captions()
        self.end_log_block()

    def add_extract(self, new_extract_filename):
        """
        :param new_extract_filename: Name of the new stub TDE file to be created. .tde will be added if not specified
        :type new_extract_filename: unicode
        :return:
        """
        self.log(u'add_extract called, checking if extract exists already')
        # Test to see if extract exists already
        e = self.xml.find(u'extract')
        if e is not None:
            self.log(u"Existing extract found, no need to add")
            raise AlreadyExistsException(u"An extract already exists, can't add a new one", u"")
        else:
            self.log(u'Extract doesnt exist')
            if new_extract_filename.find(u'.tde') == -1:
                new_extract_filename += u'.tde'
            self._tde_filename = new_extract_filename
            self.log(u'Adding extract to the  data source')

    def generate_extract_section(self):
        # Short circuit if no extract had been set
        if self._tde_filename is None:
            self.log(u'No tde_filename, no extract being added')
            return False
        self.log(u'Importing the Tableau SDK to build the extract')

        # Import only if necessary
        self.log(u'Building the extract Element object')
        try:
            from tde_file_generator import TDEFileGenerator
        except Exception as ex:
            print u"Must install the Tableau Extract SDK to add extracts"
            raise
        e = etree.Element(u'extract')
        e.set(u'count', u'-1')
        e.set(u'enabled', u'true')
        e.set(u'units', u'records')

        c = etree.Element(u'connection')
        c.set(u'class', u'dataengine')
        c.set(u'dbname', u'Data/Datasources/{}'.format(self._tde_filename))
        c.set(u'schema', u'Extract')
        c.set(u'tablename', u'Extract')
        right_now = datetime.datetime.now()
        pretty_date = right_now.strftime("%m/%d/%Y %H:%M:%S %p")
        c.set(u'update-time', pretty_date)

        r = etree.Element(u'relation')
        r.set(u"name", u"Extract")
        r.set(u"table", u"[Extract].[Extract]")
        r.set(u"type", u"table")
        c.append(r)

        calcs = etree.Element(u"calculations")
        calc = etree.Element(u"calculation")
        calc.set(u"column", u"[Number of Records]")
        calc.set(u"formula", u"1")
        calcs.append(calc)
        c.append(calcs)

        ref = etree.Element(u'refresh')
        if self.incremental_refresh_field is not None:
            ref.set(u"increment-key", self.incremental_refresh_field)
            ref.set(u"incremental-updates", u'true')
        elif self.incremental_refresh_field is None:
            ref.set(u"increment-key", u"")
            ref.set(u"incremental-updates", u'false')

        c.append(ref)

        e.append(c)

        tde_columns = {}
        self.log(u'Creating the extract filters')
        if len(self.extract_filters) > 0:
            filters = self.generate_filters(self.extract_filters)
            for f in filters:
                e.append(f)
            # Any column in the extract filters needs to exist in the TDE file itself
            if len(self.extract_filters) > 0:
                for f in self.extract_filters:
                    # Check to see if column_name is actually an instance
                    field_name = f[u'column_name']
                    for col_instance in self.column_instances:
                        if field_name == col_instance[u'name']:
                            field_name = col_instance[u'column']
                    # Simple heuristic for determining type from the first value in the values array
                    if f[u'type'] == u'categorical':
                        first_value = f[u'values'][0]
                        if isinstance(first_value, basestring):
                            filter_column_tableau_type = 'str'
                        else:
                            filter_column_tableau_type = 'int'
                    elif f[u'type'] == u'relative-date':
                        filter_column_tableau_type = 'datetime'
                    elif f[u'type'] == u'quantitative':
                        # Time values passed in with strings
                        if isinstance(f[u'max'], basestring) or isinstance(f[u'min'], basestring):
                            filter_column_tableau_type = 'datetime'
                        else:
                            filter_column_tableau_type = 'int'
                    else:
                        raise InvalidOptionException('{} is not a valid type'.format(f[u'type']))
                    tde_columns[field_name[1:-1]] = filter_column_tableau_type
        else:
            self.log(u'Creating TDE with only one field, "Generic Field", of string type')
            tde_columns[u'Generic Field'] = 'str'

        self.log(u'Using the Extract SDK to build an empty extract file with the right definition')
        tde_file_generator = TDEFileGenerator(self.logger)
        tde_file_generator.set_table_definition(tde_columns)
        tde_file_generator.create_tde(self.tde_filename)
        return e

    #
    # Reading existing table relations
    #
    def read_existing_relations(self):
        # Test for single relation
        if len(self.relation_xml_obj) == 0:
                if self.relation_xml_obj.get(u'type') == u'text':
                    self.set_first_custom_sql(self.relation_xml_obj.get(u'table'),
                                              self.relation_xml_obj.get(u'name'),
                                              self.relation_xml_obj.get(u'connection'))
                else:
                    self.set_first_table(self.relation_xml_obj.get(u'table'),
                                         self.relation_xml_obj.get(u'name'),
                                         self.relation_xml_obj.get(u'connection'))

    #
    # For creating new table relations
    #
    def set_first_table(self, db_table_name, table_alias, connection=None):
        self.main_table_relation = self.create_table_relation(db_table_name, table_alias, connection=connection)

    def set_first_custom_sql(self, custom_sql, table_alias, connection=None):
        self.main_table_relation = self.create_custom_sql_relation(custom_sql, table_alias, connection=connection)

    @staticmethod
    def create_random_calculation_name():
        n = 19
        range_start = 10 ** (n - 1)
        range_end = (10 ** n) - 1
        random_digits = random.randint(range_start, range_end)
        return u'Calculation_{}'.format(unicode(random_digits))

    @staticmethod
    def create_table_relation(db_table_name, table_alias, connection=None):
        r = etree.Element(u"relation")
        r.set(u'name', table_alias)
        r.set(u"table", u"[{}]".format(db_table_name))
        r.set(u"type", u"table")
        if connection is not None:
            r.set(u'connection', connection)
        return r

    @staticmethod
    def create_custom_sql_relation(custom_sql, table_alias, connection=None):
        r = etree.Element(u"relation")
        r.set(u'name', table_alias)
        r.text = custom_sql
        r.set(u"type", u"text")
        if connection is not None:
            r.set(u'connection', connection)
        return r

    # on_clauses = [ { left_table_alias : , left_field : , operator : right_table_alias : , right_field : ,  },]
    @staticmethod
    def define_join_on_clause(left_table_alias, left_field, operator, right_table_alias, right_field):
        return {u"left_table_alias": left_table_alias,
                u"left_field": left_field,
                u"operator": operator,
                u"right_table_alias": right_table_alias,
                u"right_field": right_field
                }

    def join_table(self, join_type, db_table_name, table_alias, join_on_clauses, custom_sql=None):
        full_join_desc = {u"join_type": join_type.lower(),
                          u"db_table_name": db_table_name,
                          u"table_alias": table_alias,
                          u"on_clauses": join_on_clauses,
                          u"custom_sql": custom_sql}
        self.join_relations.append(full_join_desc)

    def generate_relation_section(self):
        # Because of the strange way that the interior definition is the last on, you need to work inside out
        # "Middle-out" as Silicon Valley suggests.
        # Generate the actual JOINs

        self.relation_xml_obj.clear()
        # There's only a single main relation with only one table

        if len(self.join_relations) == 0:
            for item in self.main_table_relation.items():
                self.relation_xml_obj.set(item[0], item[1])
            if self.main_table_relation.text is not None:
                self.relation_xml_obj.text = self.main_table_relation.text
        else:
            prev_relation = self.relation_xml_obj

            # We go through each relation, build the whole thing, then append it to the previous relation, then make
            # that the new prev_relationship. Something like recurssion
            for join_desc in self.join_relations:
                r = etree.Element(u"relation")
                r.set(u"join", join_desc[u"join_type"])
                r.set(u"type", u"join")
                if len(join_desc[u"on_clauses"]) == 0:
                    raise InvalidOptionException("Join clause must have at least one ON clause describing relation")
                else:
                    and_expression = None
                    if len(join_desc[u"on_clauses"]) > 1:
                        and_expression = etree.Element(u"expression")
                        and_expression.set(u"op", u'AND')
                    for on_clause in join_desc[u"on_clauses"]:
                        c = etree.Element(u"clause")
                        c.set(u"type", u"join")
                        e = etree.Element(u"expression")
                        e.set(u"op", on_clause[u"operator"])

                        e_field1 = etree.Element(u"expression")
                        e_field1_name = u'[{}].[{}]'.format(on_clause[u"left_table_alias"],
                                                            on_clause[u"left_field"])
                        e_field1.set(u"op", e_field1_name)
                        e.append(e_field1)

                        e_field2 = etree.Element(u"expression")
                        e_field2_name = u'[{}].[{}]'.format(on_clause[u"right_table_alias"],
                                                            on_clause[u"right_field"])
                        e_field2.set(u"op", e_field2_name)
                        e.append(e_field2)
                        if and_expression is not None:
                            and_expression.append(e)
                        else:
                            and_expression = e

                c.append(and_expression)
                r.append(c)
                r.append(prev_relation)

                if join_desc[u"custom_sql"] is None:
                    new_table_rel = self.create_table_relation(join_desc[u"db_table_name"],
                                                               join_desc[u"table_alias"])
                elif join_desc[u"custom_sql"] is not None:
                    new_table_rel = self.create_custom_sql_relation(join_desc[u'custom_sql'],
                                                                    join_desc[u'table_alias'])
                r.append(new_table_rel)
                prev_relation = r

                self.relation_xml_obj.append(prev_relation)

    def add_table_column(self, table_alias, table_field_name, tableau_field_alias):
        # Check to make sure the alias has been added

        # Check to make sure the tableau_field_alias hasn't been used already

        self.column_mapping[tableau_field_alias] = u"[{}].[{}]".format(table_alias, table_field_name)

    def add_column_alias(self, tableau_field_alias, caption=None, dimension_or_measure=None,
                         discrete_or_continuous=None, datatype=None, calculation=None):
        if dimension_or_measure.lower() in [u'dimension', u'measure']:
            role = dimension_or_measure.lower()
        else:
            raise InvalidOptionException("{} should be either measure or dimension".format(dimension_or_measure))

        if discrete_or_continuous.lower() in [u'discrete', u'continuous']:
            if discrete_or_continuous.lower() == u'discrete':
                if datatype.lower() in [u'string']:
                    t_type = u'nominal'
                else:
                    t_type = u'ordinal'
            elif discrete_or_continuous.lower() == u'continuous':
                t_type = u'quantitative'
        else:
            raise InvalidOptionException("{} should be either discrete or continuous".format(discrete_or_continuous))

        if datatype.lower() not in [u'string', u'integer', u'datetime', u'date', u'real', u'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))

        self.column_aliases[tableau_field_alias] = {u"caption": caption,
                                                    u"type": t_type,
                                                    u"datatype": datatype.lower(),
                                                    u"role": role,
                                                    u"calculation": calculation}

    def add_calculation(self, calculation, calculation_name, dimension_or_measure, discrete_or_continuous, datatype):
        internal_calc_name = self.create_random_calculation_name()
        self.add_column_alias(internal_calc_name, calculation_name, dimension_or_measure, discrete_or_continuous,
                              datatype, calculation)
        # internal_calc_name allows you to create a filter on this
        return internal_calc_name

    @staticmethod
    def create_dimension_filter(column_name, values, include_or_exclude=u'include', custom_value_list=False):
        # Check if column_name is actually the alias of a calc, if so, replace with the random internal calc name

        if include_or_exclude.lower() in [u'include', u'exclude']:
            if include_or_exclude.lower() == u'include':
                ui_enumeration = u'inclusive'
            elif include_or_exclude.lower() == u'exclude':
                ui_enumeration = u'exclusive'
            else:
                ui_enumeration = u'inclusive'
        else:
            raise InvalidOptionException('{} is not valid, must be include or exclude'.format(include_or_exclude))
        ds_filter = {
                     u"type": u'categorical',
                     u'column_name': u'[{}]'.format(column_name),
                     u"values": values,
                     u'ui-enumeration': ui_enumeration,
                     u'ui-manual-selection': custom_value_list
                    }
        return ds_filter

    def add_dimension_datasource_filter(self, column_name, values, include_or_exclude=u'include',
                                        custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.datasource_filters.append(ds_filter)

    def add_dimension_extract_filter(self, column_name, values, include_or_exclude=u'include', custom_value_list=False):
        ds_filter = self.create_dimension_filter(column_name, values, include_or_exclude, custom_value_list)
        self.extract_filters.append(ds_filter)

    def add_continuous_datasource_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value, date=date)
        self.datasource_filters.append(ds_filter)

    def add_continuous_extract_filter(self, column_name, min_value=None, max_value=None, date=False):
        ds_filter = self.create_continuous_filter(column_name, min_value=min_value, max_value=max_value ,date=date)
        self.extract_filters.append(ds_filter)

    def add_relative_date_datasource_filter(self, column_name, period_type, number_of_periods=None,
                                            previous_next_current=u'previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current
                                                     , to_date)
        self.datasource_filters.append(ds_filter)

    def add_relative_date_extract_filter(self, column_name, period_type, number_of_periods=None,
                                         previous_next_current=u'previous', to_date=False):
        ds_filter = self.create_relative_date_filter(column_name, period_type, number_of_periods, previous_next_current,
                                                     to_date)
        self.extract_filters.append(ds_filter)

    def create_continuous_filter(self, column_name, min_value=None, max_value=None, date=False):
        # Dates need to be wrapped in # #
        if date is True:
            if min_value is not None:
                min_value = u'#{}#'.format(unicode(min_value))
            if max_value is not None:
                max_value = u'#{}#'.format(unicode(max_value))
            final_column_name = u'[none:{}:qk]'.format(column_name)
            # Need to create a column-instance tag in the columns section
            self.column_instances.append(
                {
                    u'column': u'[{}]'.format(column_name),
                    u'name': final_column_name,
                    u'type': u'quantitative'
                }
            )
        else:
            final_column_name = u'[{}]'.format(column_name)
        ds_filter = {
            u'type': u'quantitative',
            u'min': min_value,
            u'max': max_value,
            u'column_name': final_column_name,
        }
        return ds_filter

    def create_relative_date_filter(self, column_name, period_type, number_of_periods,
                                    previous_next_current=u'previous', to_date=False):
        if period_type.lower() not in [u'quarter', u'year', u'month', u'day', u'hour', u'minute']:
            raise InvalidOptionException('period_type must be one of : quarter, year, month, day, hour, minute')
        # Need to create a column-instance tag in the columns section
        final_column_name = u'[none:{}:qk]'.format(column_name)
        # Need to create a column-instance tag in the columns section
        self.column_instances.append(
            {
                u'column': u'[{}]'.format(column_name),
                u'name': final_column_name,
                u'type': u'quantitative'
            }
        )
        if previous_next_current.lower() == u'previous':
            first_period = u'-{}'.format(unicode(number_of_periods))
            last_period = u'0'
        elif previous_next_current.lower() == u'next':
            first_period = u'1'
            last_period = u'{}'.format(unicode(number_of_periods))
        elif previous_next_current.lower() == u'current':
            first_period = u'0'
            last_period = u'0'
        else:
            raise InvalidOptionException('You must use "previous", "next" or "current" for the period selections')

        if to_date is False:
            include_future = u'true'
        elif to_date is True:
            include_future = u'false'

        ds_filter = {
            u'type': u'relative-date',
            u'column_name': final_column_name,
            u'first-period': first_period,
            u'last-period': last_period,
            u'period-type': period_type.lower(),
            u'include-future': include_future
        }
        return ds_filter

    def generate_filters(self, filter_array):
        return_array = []
        for filter_def in filter_array:
            f = etree.Element(u'filter')
            f.set(u'class', filter_def[u'type'])
            f.set(u'column', filter_def[u'column_name'])
            f.set(u'filter-group', u'2')
            if filter_def[u'type'] == u'quantitative':
                f.set(u'include-values', u'in-range')
                if filter_def[u'min'] is not None:
                    m = etree.Element(u'min')
                    m.text = unicode(filter_def[u'min'])
                    f.append(m)
                if filter_def[u'max'] is not None:
                    m = etree.Element(u'max')
                    m.text = unicode(filter_def[u'max'])
                    f.append(m)
            elif filter_def[u'type'] == u'relative-date':
                f.set(u'first-period', filter_def[u'first-period'])
                f.set(u'include-future', filter_def[u'include-future'])
                f.set(u'last-period', filter_def[u'last-period'])
                f.set(u'include-null', u'false')
                f.set(u'period-type', filter_def[u'period-type'])

            elif filter_def[u'type'] == u'categorical':
                gf = etree.Element(u'groupfilter')
                # This attribute has a user namespace
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-domain', u'database')
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-enumeration', filter_def[u'ui-enumeration'])
                gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-marker', u'enumerate')
                if filter_def[u'ui-manual-selection'] is True:
                    gf.set(u'{' + u'{}'.format(self.nsmap[u'user']) + u'}ui-manual-selection', u'true')
                if len(filter_def[u'values']) == 1:
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        gf.set(u'function', u'except')
                        gf1 = etree.Element(u'groupfilter')
                        gf1.set(u'function', u'member')
                        gf1.set(u'level', filter_def[u'column_name'])
                    else:
                        gf.set(u'function', u'member')
                        gf.set(u'level', filter_def[u'column_name'])
                        gf1 = gf
                    # strings need the &quot;, ints do not
                    if isinstance(filter_def[u'values'][0], basestring):
                        gf1.set(u'member', quoteattr(filter_def[u'values'][0]))
                    else:
                        gf1.set(u'member', unicode(filter_def[u'values'][0]))
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        # Single exclude filters include an extra groupfilter set with level-members function
                        lm = etree.Element(u'groupfilter')
                        lm.set(u'function', u'level-members')
                        lm.set(u'level', filter_def[u'column_name'])
                        gf.append(lm)
                        gf.append(gf1)
                    f.append(gf)
                else:
                    if filter_def[u'ui-enumeration'] == u'exclusive':
                        gf.set(u'function', u'except')
                    else:
                        gf.set(u'function', u'union')
                    for val in filter_def[u'values']:
                        gf1 = etree.Element(u'groupfilter')
                        gf1.set(u'function', u'member')
                        gf1.set(u'level', filter_def[u'column_name'])
                        # String types need &quot; , ints do not
                        if isinstance(val, basestring):
                            gf1.set(u'member', quoteattr(val))
                        else:
                            gf1.set(u'member', unicode(val))
                        gf.append(gf1)
                    f.append(gf)
            return_array.append(f)
        return return_array

    def generate_datasource_filters_section(self):
        filters = self.generate_filters(self.datasource_filters)
        filters_array = []
        for f in filters:
            filters_array.append(f)
        return filters_array

    def generate_cols_map_section(self):
        if len(self.column_mapping) == 0:
            return False
        c = etree.Element(u"cols")
        for key in self.column_mapping:
            m = etree.Element(u"map")
            m.set(u"key", u"[{}]".format(key))
            m.set(u"value", self.column_mapping[key])
            c.append(m)
        self.ds_xml.append(c)

    @staticmethod
    def generate_aliases_tag():
        # For whatever reason, the aliases tag does not contain the columns, but it always precedes it
        a = etree.Element(u"aliases")
        a.set(u"enabled", u"yes")
        return a

    def generate_aliases_column_section(self):
        column_aliases_array = []

        # Now to put in each column tag
        for column_alias in self.column_aliases:
            c = etree.Element(u"column")
            # Name is the Tableau Field Alias, always surrounded by brackets SQL Server style
            c.set(u"name", u"[{}]".format(column_alias))
            if self.column_aliases[column_alias][u"datatype"] is not None:
                c.set(u"datatype", self.column_aliases[column_alias][u"datatype"])
            if self.column_aliases[column_alias][u"caption"] is not None:
                c.set(u"caption", self.column_aliases[column_alias][u"caption"])
            if self.column_aliases[column_alias][u"role"] is not None:
                c.set(u"role", self.column_aliases[column_alias][u"role"])
            if self.column_aliases[column_alias][u"type"] is not None:
                c.set(u"type", self.column_aliases[column_alias][u"type"])
            if self.column_aliases[column_alias][u'calculation'] is not None:
                calc = etree.Element(u'calculation')
                calc.set(u'class', u'tableau')
                # quoteattr adds an extra real set of quotes around the string, which needs to be sliced out
                calc.set(u'formula', quoteattr(self.column_aliases[column_alias][u'calculation'])[1:-1])
                c.append(calc)
            column_aliases_array.append(c)
        return column_aliases_array

    def generate_column_instances_section(self):
        column_instances_array = []
        for column_instance in self.column_instances:
            ci = etree.Element(u'column-instance')
            ci.set(u'column', column_instance[u'column'])
            ci.set(u'derivation', u'None')
            ci.set(u'name', column_instance[u'name'])
            ci.set(u'pivot', u'key')
            ci.set(u'type', column_instance[u'type'])
            column_instances_array.append(ci)
        return column_instances_array


class TableauParameters(TableauDocument):
    def __init__(self, datasource_xml, logger_obj=None):
        """
        :type datasource_xml: etree.Element
        :type logger_obj: Logger
        """
        TableauDocument.__init__(self)
        self.logger = logger_obj

    # Parameters manipulation methods
    def get_parameter_by_name(self, parameter_name):
        param_column = self.xml.xpath(u'//t:column[@alias="{}"]'.format(parameter_name), namespaces=self.ns_map)
        return param_column