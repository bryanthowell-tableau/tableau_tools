from ..tableau_base import TableauBase
from tableau_connection import TableauConnection, TableauRepositoryLocation
from tableau_document import TableauColumns
from tableau_datasource_generator import TableauDatasourceGenerator, TableauParametersGenerator
import xml.etree.cElementTree as etree
from ..tableau_exceptions import *
import zipfile
import os


# Meant to represent a TDS file, does not handle the file opening
class TableauDatasource(TableauBase):
    def __init__(self, datasource_xml, logger_obj=None):
        """
        :type datasource_xml: etree.Element
        :type logger_obj: Logger
        """
        self.logger = logger_obj
        self.xml = datasource_xml
        self.parameters = False
        self.ds_name = None
        if self.xml.get("caption"):
            self.ds_name = self.xml.attrib["caption"]
        elif self.xml.get("name"):
            self.ds_name = self.xml.attrib['name']

        self.columns = None
        # Possible, though unlikely, that there would be no columns
        if self.xml.find(u'column') is not None:
            columns_list = self.xml.findall(u'column')
            self.columns = TableauColumns(columns_list, self.logger)

        # Internal "Parameters" datasource of a TWB acts differently
        if self.ds_name == u'Parameters':
            self.parameters = True
            self.ds_generator = TableauParametersGenerator()
        else:
            connection_xml_obj = self.xml.find(u'connection')
            self.log(u'connection tags found, building a TableauConnection object')
            self.connection = TableauConnection(connection_xml_obj)

            self.repository_location = None
            if len(self.xml.find(u'repository-location')) == 0:
                repository_location_xml = self.xml.find(u'repository-location')
                self.repository_location = TableauRepositoryLocation(repository_location_xml, self.logger)

            self.tde_filename = None
            if self.connection.get_connection_type() == u'sqlproxy':
                self.ds_generator = None
            else:
                self.ds_generator = TableauDatasourceGenerator(self.connection.get_connection_type(),
                                                               self.xml.get('formatted-name'),
                                                               self.connection.get_server(),
                                                               self.connection.get_dbname(),
                                                               self.logger,
                                                               authentication=u'username-password', initial_sql=None)

    def get_connection(self):
        """
        :rtype: TableauConnection
        """
        return self.connection

    def get_datasource_name(self):
        self.start_log_block()
        name = self.ds_name
        self.end_log_block()
        return name

    def add_extract(self, new_extract_filename):
        self.log(u'add_extract called, chicking if extract exists already')
        # Test to see if extract exists already
        e = self.xml.find(u'extract')
        self.log(u'Found the extract portion of the ')
        if e is not None:
            self.log("Existing extract found, no need to add")
            raise AlreadyExistsException("An extract already exists, can't add a new one")
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

    def is_published_ds(self):
        if self.repository_location is not None:
            return True
        else:
            return False

    def set_published_datasource_site(self, new_site_content_url):
        self.start_log_block()
        self.repository_location.set_site(new_site_content_url)
        self.end_log_block()

    def get_columns_obj(self):
        self.start_log_block()
        cols = self.columns
        self.end_log_block()
        return cols

    def translate_columns(self, translation_dict):
        self.start_log_block()
        self.columns.set_translation_dict(translation_dict)
        self.columns.translate_captions()
        self.end_log_block()

    # Parameters manipulation methods
    def get_parameter_by_name(self, parameter_name):
        param_column = self.xml.xpath(u'//t:column[@alias="{}"]'.format(parameter_name), namespaces=self.ns_map)
        return param_column
