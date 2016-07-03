from ..tableau_base import TableauBase
from ..tableau_exceptions import *
from lxml import etree
from StringIO import StringIO


# Represents the actual Connection tag of a given datasource
class TableauConnection(TableauBase):
    def __init__(self, connection_line, logger_obj=None):
        self.logger = logger_obj
        # Building from a <connection> tag
        self.xml_obj = None

        if connection_line.find(u"<connection ") != -1:
            self.log(u'Looking at: {}'.format(connection_line))
            # Add ending tag for XML parsing
            connection_line += u"</connection>"
            utf8_parser = etree.XMLParser(encoding='utf-8')
            xml = etree.parse(StringIO(connection_line), parser=utf8_parser)
            self.xml_obj = xml.getroot()
            # xml = etree.fromstring(connection_line)
        else:
            raise InvalidOptionException(u"Must create a TableauConnection from a Connection line")

    def set_dbname(self, new_db_name):
        if self.xml_obj.get("dbname") is not None:
            self.xml_obj.attrib["dbname"] = new_db_name

    def get_dbname(self):
        return self.xml_obj.get("dbname")

    def set_server(self, new_server):
        if self.xml_obj.get("server") is not None:
            self.xml_obj.attrib["server"] = new_server

    def get_server(self):
        return self.xml_obj.get("server")

    def set_username(self, new_username):
        if self.xml_obj.get("username") is not None:
            self.xml_obj.attrib["username"] = new_username

    def set_port(self, new_port):
        if self.xml_obj.get("port") is not None:
            self.xml_obj.attrib["port"] = new_port

    def get_port(self):
        return self.xml_obj.get("port")

    def get_connection_type(self):
        return self.xml_obj.get('class')

    def get_xml_string(self):
        xml_with_ending_tag = etree.tostring(self.xml_obj)
        # Slice off the extra connection ending tag
        return xml_with_ending_tag[0:xml_with_ending_tag.find('</connection>')]

    def is_published_datasource(self):
        if self.xml_obj.get("class") == 'sqlproxy':
            return True
        else:
            return False

    def is_windows_auth(self):
        if self.xml_obj.get("authentication") is not None:
            if self.xml_obj.get("authentication") == 'sspi':
                return True
            else:
                return False

    def set_sslmode(self, value='require'):
        self.xml_obj.attrib["sslmode"] = value


# Represents the repository-location tag present in a published datasource in a workbook
class TableauRepositoryLocation(TableauBase):
    def __init__(self, tag_line, logger_obj=None):
        self.logger = logger_obj
        # Building from a <connection> tag
        self.xml_obj = None
        if tag_line.find(u"<repository-location ") != -1:
            self.log(u'Looking at: {}'.format(tag_line))

            utf8_parser = etree.XMLParser(encoding='utf-8')
            xml = etree.parse(StringIO(tag_line), parser=utf8_parser)
            self.xml_obj = xml.getroot()

        else:
            raise InvalidOptionException(u"Must create a TableauRepositoryLocation from a repository-location line")

    def get_site(self):
        return self.xml_obj.get("site")

    def set_site(self, new_site_content_url):
        self.start_log_block()
        # If it was originally the default site, you need to add the site name in front
        if self.xml_obj.get("site") is None:
            self.xml_obj.attrib["path"] = '/t/{}'.format(new_site_content_url) + self.xml_obj.get("path")
        # Replace the original site_content_url with the new one
        elif self.xml_obj.get("site") is not None:
            self.xml_obj.attrib["path"] = self.xml_obj.get("path").replace(self.xml_obj.get("site"), new_site_content_url)
        self.xml_obj.attrib['site'] = new_site_content_url
        self.end_log_block()

    def get_xml_string(self):
        self.start_log_block()
        xml_with_ending_tag = etree.tostring(self.xml_obj)
        self.log('XML from TableauRepositoryLocation')
        self.log(xml_with_ending_tag)
        self.end_log_block()
        return xml_with_ending_tag