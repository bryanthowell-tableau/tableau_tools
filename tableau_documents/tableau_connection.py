from ..tableau_base import *
from ..tableau_exceptions import *
import xml.etree.cElementTree as etree


# Represents the actual Connection tag of a given datasource
class TableauConnection(TableauBase):
    def __init__(self, connection_xml_obj, logger_obj=None):
        TableauBase.__init__(self)
        self.logger = logger_obj
        self.connection_name = None
        # Differentiate between named-connetion and connection itself
        if connection_xml_obj.tag == u'named-connection':
            self.connection_name = connection_xml_obj.get(u'name')
            self.xml_obj = connection_xml_obj.find(u'connection')
        else:
            self.xml_obj = connection_xml_obj

    @property
    def dbname(self):
        # Looks for schema tag as well in case it's an Oracle system
        if self.xml_obj.get(u'dbname'):
            return self.xml_obj.get(u'dbname')
        elif self.xml_obj.get(u'schema'):
            return self.xml_obj.get(u'schema')
        else:
            return None

    @dbname.setter
    def dbname(self, new_db_name):
        """
        :type new_db_name: unicode
        :return:
        """
        if self.xml_obj.get(u"dbname") is not None:
            self.xml_obj.attrib[u"dbname"] = new_db_name
        elif self.xml_obj.get(u'schema') is not None:
            self.xml_obj.attrib[u'schema'] = new_db_name
        else:
            if self.connection_type == u'oracle':
                self.xml_obj.set(u'schema', new_db_name)
            else:
                self.xml_obj.set(u'dbname', new_db_name)

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
        return self.xml_obj.get(u"server")

    @server.setter
    def server(self, new_server):
        if self.xml_obj.get(u"server") is not None:
            self.xml_obj.attrib[u"server"] = new_server
        else:
            self.xml_obj.set(u'server', new_server)

    @property
    def port(self):
        return self.xml_obj.get(u"port")

    @port.setter
    def port(self, new_port):
        if self.xml_obj.get(u"port") is not None:
            self.xml_obj.attrib[u"port"] = new_port
        else:
            self.xml_obj.set(u'port', new_port)

    @property
    def connection_type(self):
        return self.xml_obj.get(u'class')

    @connection_type.setter
    def connection_type(self, new_type):
        if self.xml_obj.get(u"class") is not None:
            self.xml_obj.attrib[u"class"] = new_type
        else:
            self.xml_obj.set(u'class', new_type)

    def is_windows_auth(self):
        if self.xml_obj.get(u"authentication") is not None:
            if self.xml_obj.get(u"authentication") == u'sspi':
                return True
            else:
                return False

    @property
    def sslmode(self):
        return self.xml_obj.get(u'sslmode')

    @sslmode.setter
    def sslmode(self, value='require'):
        self.xml_obj.attrib["sslmode"] = value

    @property
    def authentication(self):
        return self.xml_obj.get(u'authentication')

    @authentication.setter
    def authentication(self, auth_type):
        if self.xml_obj.get(u"authentication") is not None:
            self.xml_obj.attrib[u"authentication"] = auth_type
        else:
            self.xml_obj.set(u"authentication")

    @property
    def service(self):
        return self.xml_obj.get(u'service')

    @service.setter
    def service(self, service):
        if self.xml_obj.get(u"service") is not None:
            self.xml_obj.attrib[u"service"] = service
        else:
            self.xml_obj.set(u"service")

