from ..tableau_base import *
from ..tableau_exceptions import *
import xml.etree.cElementTree as etree


# Represents the actual Connection tag of a given datasource
class TableauConnection(TableauBase):
    def __init__(self, connection_xml_obj, logger_obj=None):
        TableauBase.__init__(self)
        self.logger = logger_obj
        self.connection_name = None
        # Differentiate between named-connection and connection itself
        if connection_xml_obj.tag == 'named-connection':
            self.connection_name = connection_xml_obj.get('name')
            self.xml_obj = connection_xml_obj.find('connection')
        else:
            self.xml_obj = connection_xml_obj

    @property
    def cols(self):
        """
        :rtype: etree.Element
        """
        return self.xml_obj.find('cols')

    @property
    def dbname(self):
        # Looks for schema tag as well in case it's an Oracle system
        if self.xml_obj.get('dbname'):
            return self.xml_obj.get('dbname')
        elif self.xml_obj.get('schema'):
            return self.xml_obj.get('schema')
        else:
            return None

    @dbname.setter
    def dbname(self, new_db_name):
        """
        :type new_db_name: unicode
        :return:
        """
        if self.xml_obj.get("dbname") is not None:
            self.xml_obj.attrib["dbname"] = new_db_name
        elif self.xml_obj.get('schema') is not None:
            self.xml_obj.attrib['schema'] = new_db_name
        else:
            if self.connection_type == 'oracle':
                self.xml_obj.set('schema', new_db_name)
            else:
                self.xml_obj.set('dbname', new_db_name)

    @property
    def schema(self):
        # dbname already handles this for Oracle, just here for the heck of it
        return self.dbname

    @schema.setter
    def schema(self, new_schema):
        """
        :type new_schema: unicode
        :return:
        """
        self.dbname = new_schema

    @property
    def server(self):
        return self.xml_obj.get("server")

    @server.setter
    def server(self, new_server):
        """
        :type new_server: unicode
        :return:
        """
        if self.xml_obj.get("server") is not None:
            self.xml_obj.attrib["server"] = new_server
        else:
            self.xml_obj.set('server', new_server)

    @property
    def port(self):
        return self.xml_obj.get("port")

    @port.setter
    def port(self, new_port):
        """
        :type port: unicode
        :return:
        """
        if self.xml_obj.get("port") is not None:
            self.xml_obj.attrib["port"] = str(new_port)
        else:
            self.xml_obj.set('port', str(new_port))

    @property
    def connection_type(self):
        return self.xml_obj.get('class')

    @connection_type.setter
    def connection_type(self, new_type):
        if self.xml_obj.get("class") is not None:
            self.xml_obj.attrib["class"] = new_type
        else:
            self.xml_obj.set('class', new_type)

    def is_windows_auth(self):
        if self.xml_obj.get("authentication") is not None:
            if self.xml_obj.get("authentication") == 'sspi':
                return True
            else:
                return False

    @property
    def filename(self):
        if self.xml_obj.get('filename') is None:
            raise NoResultsException('Connection type {} does not have filename attribute'.format(
                self.connection_type))
        else:
            return self.xml_obj.get('filename')

    @filename.setter
    def filename(self, filename):
        if self.xml_obj.get('filename') is not None:
            self.xml_obj.attrib['filename'] = filename
        else:
            self.xml_obj.set('filename', filename)

    @property
    def sslmode(self):
        return self.xml_obj.get('sslmode')

    @sslmode.setter
    def sslmode(self, value='require'):
        if self.xml_obj.get('sslmode') is not None:
            self.xml_obj.attrib["sslmode"] = value
        else:
            self.xml_obj.set('sslmode', value)

    @property
    def authentication(self):
        return self.xml_obj.get('authentication')

    @authentication.setter
    def authentication(self, auth_type):
        """
        :type auth_type: unicode
        :return:
        """
        if self.xml_obj.get("authentication") is not None:
            self.xml_obj.attrib["authentication"] = auth_type
        else:
            self.xml_obj.set("authentication", auth_type)

    @property
    def service(self):
        return self.xml_obj.get('service')

    @service.setter
    def service(self, service):
        """
        :type service: unicode
        :return:
        """
        if self.xml_obj.get("service") is not None:
            self.xml_obj.attrib["service"] = service
        else:
            self.xml_obj.set("service", service)

    @property
    def username(self):
        return self.xml_obj.get('username')

    @username.setter
    def username(self, username):
        """
        :type username: unicode
        :return:
        """
        if self.xml_obj.get('username') is not None:
            self.xml_obj.attrib['username'] = username
        else:
            self.xml_obj.set('username', username)

