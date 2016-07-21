from ..tableau_base import TableauBase
from tableau_connection import TableauConnection, TableauConnection2, TableauRepositoryLocation
from tableau_document import TableauColumns
from tableau_datasource_generator import TableauDatasourceGenerator
from StringIO import StringIO
from lxml import etree
from ..tableau_exceptions import *
import zipfile
import os


# Meant to represent a TDS file, does not handle the file opening
class TableauDatasource(TableauBase):
    def __init__(self, datasource_string, logger_obj=None, translation_on=False):
        self.logger = logger_obj
        self.log(u'Initializing a TableauDatasource object')
        self.ds = StringIO(datasource_string)
        self.start_xml = ""
        self.end_xml = ""
        self.middle_xml = ""
        self.columns_xml = ""
        self.ds_name = None
        self.connection = None
        self.columns_obj = None
        self.translate_flag = False
        self.repository_location = None

        # Find connection line and build TableauConnection object
        start_flag = True
        columns_flag = False
        aliases_flag = False
        for line in self.ds:
            # Grab the caption if coming from
            if line.find('<datasource ') != -1:
                # Complete the tag so XML can be parsed
                ds_tag = line + '</datasource>'
                utf8_parser = etree.XMLParser(encoding='utf-8')
                xml = etree.parse(StringIO(ds_tag), parser=utf8_parser)
                xml_obj = xml.getroot()
                if xml_obj.get("caption"):
                    self.ds_name = xml_obj.attrib["caption"]
                elif xml_obj.get("name"):
                    self.ds_name = xml_obj.attrib['name']

                if start_flag is True:
                    self.start_xml += line
                elif start_flag is False:
                    self.end_xml += line
            elif line.find('<repository-location ') != -1 and start_flag is True:
                self.log(u'Creating a TableauRepositoryLocation object')
                self.repository_location = TableauRepositoryLocation(line, self.logger)
                self.log(u"This is the repository location line:")
                self.log(line)
                continue

            elif line.find('<connection ') != -1 and start_flag is True:
                self.log(u'Creating a TableauConnection object')
                self.connection = TableauConnection(line)
                self.log(u"This is the connection line:")
                self.log(line)
                start_flag = False
                continue
            else:
                # For columns object creation, the start at the first <column> and end after last </column>
                if line.find(u"<aliases enabled='yes' />") != -1:
                    aliases_flag = True
                    self.middle_xml += line
                    continue
                if aliases_flag is True:
                    if columns_flag is False and line.find('<column') != -1:
                        columns_flag = True
                    # columns can have calculation tags inside that defind a calc
                    if columns_flag is True and line.find('column-instance') != -1:
                        columns_flag = False
                    elif columns_flag is True and line.find('group') != -1:
                        columns_flag = False
                    elif columns_flag is True and line.find('layout') != -1:
                        columns_flag = False
                    if columns_flag is True:
                        self.columns_xml += line
                    elif start_flag is False and columns_flag is False:
                        self.end_xml += line
                elif start_flag is True:
                    self.start_xml += line
                elif start_flag is False and aliases_flag is False:
                    self.middle_xml += line
                elif start_flag is False and aliases_flag is True:
                    self.end_xml += line

        if self.columns_xml != "":
            self.log(u'Creating a TableauColumns object')
            self.log(self.columns_xml)
            self.columns_obj = TableauColumns(self.columns_xml, self.logger)

    def get_datasource_name(self):
        self.start_log_block()
        name = self.ds_name
        self.end_log_block()
        return name

    def get_datasource_xml(self):
        self.start_log_block()
        xml = self.start_xml
        # Parameters datasource section does not have a connection tag
        if self.repository_location is not None:
            xml += self.repository_location.get_xml_string() + "\n"
        if self.connection is not None:
            xml += self.connection.get_xml_string()
        xml += self.middle_xml
        if self.translate_flag is True:
            xml += self.columns_obj.get_xml_string()
        else:
            xml += self.columns_xml
        xml += self.end_xml
        self.end_log_block()
        return xml

    def save_datasource_xml(self, filename):
        self.start_log_block()
        try:
            lh = open(filename, 'wb')
            lh.write(self.get_datasource_xml())
            lh.close()
            self.end_log_block()
        except IOError:
            self.log(u"Error: File '{}' cannot be opened to write to".format(filename))
            self.end_log_block()
            raise

    def get_columns_obj(self):
        self.start_log_block()
        cols = self.columns_obj
        self.end_log_block()
        return cols

    def translate_columns(self, translation_dict):
        self.start_log_block()
        self.columns_obj.set_translation_dict(translation_dict)
        self.columns_obj.translate_captions()
        self.translate_flag = True
        xml = self.columns_obj.get_xml_string()
        self.end_log_block()
        return xml

    def is_published_ds(self):
        if self.repository_location is not None:
            return True
        else:
            return False

    def set_published_datasource_site(self, new_site_content_url):
        self.start_log_block()
        self.repository_location.set_site(new_site_content_url)
        self.end_log_block()


class TableauDatasource2(TableauBase):
    def __init__(self, datasource_string, logger_obj=None):
        self.logger = logger_obj
        self.log(u'Itsa me, a TableauDatasource2 object')
        self.original_xml_string = datasource_string
        utf8_parser = etree.XMLParser(encoding='utf-8')
        self.xml = etree.parse(StringIO(datasource_string), parser=utf8_parser)
        connection_xml_obj = self.xml.getroot().find(u'connection')
        self.log(u'connection tags found, building a TableauConnection2 object')
        self.connection = TableauConnection2(connection_xml_obj)
        self.tde_filename = None
        self.generated_ds = TableauDatasourceGenerator(self.connection.get_connection_type(),
                                                       self.xml.getroot().get('formatted-name'),
                                                       self.connection.get_server(),
                                                       self.connection.get_dbname(),
                                                       self.logger,
                                                       authentication=u'username-password', initial_sql=None)

    def add_extract(self, new_extract_filename):
        self.log(u'add_extract called, chicking if extract exists already')
        # Test to see if extract exists already
        e = self.xml.getroot().find(u'extract')
        self.log(u'Found the extract portion of the ')
        if e is not None:
            self.log("Existing extract found, no need to add")
            raise AlreadyExistsException("An extract already exists, can't add a new one")
        else:
            self.log(u'Extract doesnt exist')
            # Initial test case -- create a TDG object, then use to build the extract connection
            self.tde_filename = new_extract_filename
            self.log(u'Adding extract to the generated data source')
            self.generated_ds.add_extract(self.tde_filename)
            self.log(u'Generating the extract and XML object related to it')
            extract_xml = self.generated_ds.generate_extract_section()
            self.log(u'Appending the new extract XML to the existing XML')
            self.xml.getroot().append(extract_xml)

    def get_xml_string(self):
        xmlstring = etree.tostring(self.xml, pretty_print=True, xml_declaration=True, encoding='utf-8')
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
            lh.write(self.get_xml_string())
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



