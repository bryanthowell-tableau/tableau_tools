import xml.etree.ElementTree as ET
from typing import Union, Any, Optional, List, Dict, Tuple

# from tableau_tools.tableau_base import *
from tableau_tools.tableau_exceptions import *
from tableau_tools.logger import Logger
from tableau_tools.logging_methods import LoggingMethods


# Represents the actual Connection tag of a given datasource
class TableauConnection(LoggingMethods):
    def __init__(self, connection_xml_obj: ET.Element, logger_obj: Optional[Logger] = None):
        #TableauBase.__init__(self)
        self.logger = logger_obj
        self.connection_name = None
        # Differentiate between named-connection and connection itself
        if connection_xml_obj.tag == 'named-connection':
            self.connection_name = connection_xml_obj.get('name')
            self.xml_obj = connection_xml_obj.find('connection')
        else:
            self.xml_obj = connection_xml_obj

    @property
    def cols(self) -> ET.Element:
        return self.xml_obj.find('cols')

    @property
    def dbname(self) -> Optional[str]:
        # Looks for schema tag as well in case it's an Oracle system (potentially others)
        if self.connection_type in ['oracle', ]:
            return self.xml_obj.get('schema')
        elif self.xml_obj.get('dbname'):
            return self.xml_obj.get('dbname')
        else:
            return None

    @dbname.setter
    def dbname(self, new_db_name: str):
        # Potentially could be others with the oracle issue, for later
        if self.connection_type in ['oracle', ]:
            self.xml_obj.set('schema', new_db_name)
        else:
            self.xml_obj.set('dbname', new_db_name)

    @property
    def schema(self) -> Optional[str]:

        if self.xml_obj.get("schema") is not None:
            return self.xml_obj.get('schema')
        # This is just in case you are trying schema with dbname only type database
        else:
            return self.xml_obj.get('dbname')

    @schema.setter
    def schema(self, new_schema: str):
        if self.xml_obj.get("schema") is not None:
            self.xml_obj.set('schema', new_schema)
        # This is just in case you are trying schema with dbname only type database
        else:
            self.dbname = new_schema


    @property
    def server(self) -> str:
        return self.xml_obj.get("server")

    @server.setter
    def server(self, new_server: str):
        if self.xml_obj.get("server") is not None:
            self.xml_obj.attrib["server"] = new_server
        else:
            self.xml_obj.set('server', new_server)

    @property
    def port(self) -> str:
        return self.xml_obj.get("port")

    @port.setter
    def port(self, new_port: str):
        if self.xml_obj.get("port") is not None:
            self.xml_obj.attrib["port"] = str(new_port)
        else:
            self.xml_obj.set('port', str(new_port))

    @property
    def connection_type(self) -> str:
        return self.xml_obj.get('class')

    @connection_type.setter
    def connection_type(self, new_type: str):
        if self.xml_obj.get("class") is not None:
            self.xml_obj.attrib["class"] = new_type
        else:
            self.xml_obj.set('class', new_type)

    def is_windows_auth(self) -> bool:
        if self.xml_obj.get("authentication") is not None:
            if self.xml_obj.get("authentication") == 'sspi':
                return True
            else:
                return False

    @property
    def filename(self) -> str:
        if self.xml_obj.get('filename') is None:
            raise NoResultsException('Connection type {} does not have filename attribute'.format(
                self.connection_type))
        else:
            return self.xml_obj.get('filename')

    @filename.setter
    def filename(self, filename: str):
        if self.xml_obj.get('filename') is not None:
            self.xml_obj.attrib['filename'] = filename
        else:
            self.xml_obj.set('filename', filename)

    @property
    def sslmode(self) -> str:
        return self.xml_obj.get('sslmode')

    @sslmode.setter
    def sslmode(self, value: str = 'require'):
        if self.xml_obj.get('sslmode') is not None:
            self.xml_obj.attrib["sslmode"] = value
        else:
            self.xml_obj.set('sslmode', value)

    @property
    def authentication(self) -> str:
        return self.xml_obj.get('authentication')

    @authentication.setter
    def authentication(self, auth_type: str):
        if self.xml_obj.get("authentication") is not None:
            self.xml_obj.attrib["authentication"] = auth_type
        else:
            self.xml_obj.set("authentication", auth_type)

    @property
    def service(self) -> str:
        return self.xml_obj.get('service')

    @service.setter
    def service(self, service: str):
        if self.xml_obj.get("service") is not None:
            self.xml_obj.attrib["service"] = service
        else:
            self.xml_obj.set("service", service)

    @property
    def username(self) -> str:
        return self.xml_obj.get('username')

    @username.setter
    def username(self, username: str):
        if self.xml_obj.get('username') is not None:
            self.xml_obj.attrib['username'] = username
        else:
            self.xml_obj.set('username', username)

