from ..tableau_base import TableauBase
from tableau_connection import TableauConnection
from tableau_document import TableauColumns, TableauDocument
from tableau_datasource_generator import TableauDatasourceGenerator, TableauParametersGenerator

import xml.etree.cElementTree as etree
from ..tableau_exceptions import *
import zipfile
import os


# Meant to represent a TDS file, does not handle the file opening
class TableauDatasource(TableauDocument):
    def __init__(self, datasource_xml=None, logger_obj=None, ds_version=None):
        TableauDocument.__init__(self)
        """
        :type datasource_xml: etree.Element
        :type logger_obj: Logger
        """
        self._document_type = u'datasource'
        etree.register_namespace(u't', self.ns_map['t'])
        self.logger = logger_obj
        self.connections = []
        self.ds_name = None
        self.ds_version = None
        self._published = False

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
                self.connections.append(TableauConnection(connection_xml_obj))
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

        self.repository_location = None

        if self.xml.find(u'repository-location') is not None:
            if len(self.xml.find(u'repository-location')) == 0:
                self._published = True
                repository_location_xml = self.xml.find(u'repository-location')
                self.repository_location = repository_location_xml

        # To make work as tableau_document from TableauFile
        self._datasources.append(self)

        self._columns = None
        # Possible, though unlikely, that there would be no columns
        if self.xml.find(u'column') is not None:
            columns_list = self.xml.findall(u'column')
            self._columns = TableauColumns(columns_list, self.logger)

        self.tde_filename = None
        self.ds_generator = None

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

    def add_extract(self, new_extract_filename):

        self.ds_generator = TableauDatasourceGenerator(self.connection_type,
                                                       self.xml_obj.get('formatted-name'),
                                                       self.server,
                                                       self.dbname,
                                                       self.logger,
                                                       authentication=u'username-password', initial_sql=None)
        self.log(u'add_extract called, checking if extract exists already')
        # Test to see if extract exists already
        e = self.xml.find(u'extract')
        self.log(u'Found the extract portion of the ')
        if e is not None:
            self.log(u"Existing extract found, no need to add")
            raise AlreadyExistsException(u"An extract already exists, can't add a new one")
        else:
            self.log(u'Extract doesnt exist')
            # Initial test case -- create a TDG object, then use to build the extract connection
            self.tde_filename = new_extract_filename
            self.log(u'Adding extract to the generated data source')
            self.ds_generator.add_extract(self.tde_filename)

    def get_datasource_xml(self):
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
                self.log(u'Generating the extract and XML object related to it')
                extract_xml = self.ds_generator.generate_extract_section()
                self.log(u'Appending the new extract XML to the existing XML')
                self.xml.append(extract_xml)

        xmlstring = etree.tostring(self.xml, encoding='utf-8')
        self.log(xmlstring)
        return xmlstring

    def save_file(self, filename_no_extension, save_to_directory):
        self.start_log_block()
        file_extension = u'.tds'
        if self.tde_filename is not None:
            file_extension = u'.tdsx'
        try:
            tds_filename = filename_no_extension + u'.tds'
            lh = open(save_to_directory + tds_filename, 'wb')
            lh.write(self.get_datasource_xml())
            lh.close()

            if file_extension == u'.tdsx':
                zf = zipfile.ZipFile(save_to_directory + filename_no_extension + u'.tdsx', 'w')
                zf.write(save_to_directory + tds_filename, u'/{}'.format(tds_filename))
                # Delete temporary TDS at some point
                zf.write(self.tde_filename, u'/Data/Datasources/{}'.format(self.tde_filename))
                zf.close()
                # Remove the temp tde_file that is created
                os.remove(self.tde_filename)
        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename_no_extension + file_extension))
            self.end_log_block()
            raise

    def translate_columns(self, translation_dict):
        self.start_log_block()
        self.columns.set_translation_dict(translation_dict)
        self.columns.translate_captions()
        self.end_log_block()


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