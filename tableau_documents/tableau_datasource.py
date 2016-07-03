from ..tableau_base import TableauBase
from tableau_connection import TableauConnection, TableauRepositoryLocation
from tableau_document import TableauColumns
from StringIO import StringIO
from lxml import etree


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