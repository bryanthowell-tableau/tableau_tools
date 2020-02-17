# -*- coding: utf-8 -*-

import os
from typing import Union, Optional, List, Dict, Tuple
from urllib.parse import urlencode
import copy
import xml.etree.ElementTree as ET
import random
import re

from tableau_tools.logger import Logger
from tableau_tools.logging_methods import LoggingMethods
from ._lookups import LookupMethods
# from tableau_tools.tableau_documents.tableau_file import TableauFile
from tableau_tools.tableau_exceptions import *
from tableau_tools.tableau_rest_api.rest_xml_request import RestXmlRequest
from tableau_tools.tableau_rest_api.rest_json_request import RestJsonRequest
from tableau_tools.tableau_rest_api.published_content import Project, Project28, Project33, Workbook, Datasource, Flow33
from tableau_tools.tableau_rest_api.url_filter import *
from tableau_tools.tableau_rest_api.sort import *
from ...tableau_rest_xml import TableauRestXml

class TableauRestApiBase(LookupMethods, LoggingMethods, TableauRestXml):
    # Defines a class that represents a RESTful connection to Tableau Server. Use full URL (http:// or https://)
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        if server.find('http') == -1:
            raise InvalidOptionException('Server URL must include http:// or https://')

        self.tableau_namespace = 'http://tableau.com/api'
        self.ns_map = {'t': 'http://tableau.com/api'}
        self.ns_prefix = '{' + self.ns_map['t'] + '}'
        ET.register_namespace('t', self.ns_map['t'])

        self.server: str = server
        self.site_content_url: str = site_content_url
        self.username: str = username
        self._password: str = password
        self._token: str = ""  # Holds the login token from the Sign In call
        self.site_luid: str = ""
        self.user_luid: str = ""
        self._login_as_user_id: str = ""
        self._last_error = None
        self.logger: Optional[Logger] = None
        self._last_response_content_type = None

        self._request_obj: Optional[RestXmlRequest] = None
        self._request_json_obj: Optional[RestJsonRequest] = None

        # UrlFilter object for factory methods
        self.url_filters = UrlFilter
        self.sorts = Sort

        # Lookup caches to minimize calls
        self.username_luid_cache = {}
        self.group_name_luid_cache = {}

        # For working around SSL issues
        self.verify_ssl_cert = True

        self.version: Optional[str] = None
        self.api_version: Optional[str]  = None
        # Starting in version 5 of tableau_tools, 10.3 is the lowest supported version
        self.set_tableau_server_version("10.3")

        # Try to see if this gets the composition right
        self.rest_api_base = self

        self.site_roles = (
            u'Interactor',
            u'Publisher',
            u'SiteAdministrator',
            u'Unlicensed',
            u'UnlicensedWithPublish',  # This was sunset at some point
            u'Viewer',
            u'ViewerWithPublish',
            u'ServerAdministrator',
            u'ReadOnly',
            u'Explorer',
            u'ExplorerCanPublish',
            u'SiteAdministratorExplorer',
            u'Creator',
            u'SiteAdministratorCreator'
        )



    def set_tableau_server_version(self, tableau_server_version: str) -> str:
        if str(tableau_server_version)in ["10.3", "10.4", "10.5", '2018.1', '2018.2', '2018.3', '2019.1',
                                          '2019.2', '2019.3', '2019.4']:
            if str(tableau_server_version) == '10.3':
                self.api_version = '2.6'
            elif str(tableau_server_version) == '10.4':
                self.api_version = '2.7'
            elif str(tableau_server_version) == '10.5':
                self.api_version = '2.8'
            elif str(tableau_server_version) == '2018.1':
                self.api_version = '3.0'
            elif str(tableau_server_version) == '2018.2':
                self.api_version = '3.1'
            elif str(tableau_server_version) == '2018.3':
                self.api_version = '3.2'
            elif str(tableau_server_version) == '2019.1':
                self.api_version = '3.3'
            elif str(tableau_server_version) == '2019.2':
                self.api_version = '3.4'
            elif str(tableau_server_version) == '2019.3':
                self.api_version = '3.5'
            elif str(tableau_server_version) == '2019.4':
                self.api_version = '3.6'
            self.tableau_namespace = 'http://tableau.com/api'
            self.ns_map = {'t': 'http://tableau.com/api'}
            self.version = tableau_server_version
            self.ns_prefix = '{' + self.ns_map['t'] + '}'
            #print("Current API Version set to: {}".format(self.api_version))
            return self.api_version

        else:
            raise InvalidOptionException("Please specify tableau_server_version as a string. '10.5' or '2019.3' etc...")


    # Method to handle single str or list and return a list
    @staticmethod
    def to_list(x: Union[str, List[str]]):
        if isinstance(x, str):
            l = [x]  # Make single into a collection
        else:
            l = x
        return l

    # Method to read file in x MB chunks for upload, 10 MB by default (1024 bytes = KB, * 1024 = MB, * 10)
    @staticmethod
    def read_file_in_chunks(file_object, chunk_size=(1024 * 1024 * 10)):
        while True:
            data = file_object.read(chunk_size)
            if not data:
                break
            yield data



    @property
    def token(self) -> str:
        return self._token

    @token.setter
    def token(self, new_token: str):
        self._token = new_token
        if self._request_obj is not None:
            self._request_obj.token = self._token
        if self._request_json_obj is not None:
            self._request_json_obj.token = self._token

    def enable_logging(self, logger_obj: Logger):
        self.logger = logger_obj
        if self._request_obj is not None:
            self._request_obj.enable_logging(logger_obj)

    #
    # Object helpers and setter/getters
    #

    def get_last_error(self):
        self.log(self._last_error)
        return self._last_error

    def set_last_error(self, error):
        self._last_error = error

    #
    # REST API Helper Methods
    #

    def build_api_url(self, call: str, server_level: bool = False,
                      url_parameters: Optional[str] = None ) -> str:
        if server_level is True:
            final_string = "{}/api/{}/{}".format(self.server, self.api_version, call)
        else:
            final_string = "{}/api/{}/sites/{}/{}".format(self.server, self.api_version, self.site_luid, call)
        if url_parameters is not None:
            if url_parameters[0] == "?":
                final_string += url_parameters
            else:
                final_string += "?{}".format(url_parameters)
        return final_string

    @staticmethod
    def build_url_parameter_string(map_dict: Optional[Dict] = None, name_value_tuple_list: Optional[List[Tuple]] = None,
                                   hand_built_portion: Optional[str] = None):
        encoded_list = None
        if name_value_tuple_list is not None:
            if len(name_value_tuple_list) > 0:
                for v in name_value_tuple_list:
                    if len(v) != 2:
                        raise InvalidOptionException('Each element should have a two-element Tuples (Name, Value)')
                    encoded_list = urlencode(name_value_tuple_list)

        if hand_built_portion is not None and encoded_list is None:
            final_string = hand_built_portion
        elif hand_built_portion is None and encoded_list is not None:
            final_string = encoded_list
        else:
            final_string = "{}&{}".format(hand_built_portion, encoded_list)
        return final_string

    # Check method for filter objects
    @staticmethod
    def _check_filter_objects(filter_checks):
        filters = []
        for f in filter_checks:
            if filter_checks[f] is not None:
                if filter_checks[f].field != f:
                    raise InvalidOptionException('A {} filter must be UrlFilter object set to {} field').format(f)
                else:
                    filters.append(filter_checks[f])
        return filters



    # This is specifically for replication from one site to another
    def build_request_from_response(self, request: ET.Element) -> ET.Element:
        tsr = ET.Element('tsRequest')
        request_copy = copy.deepcopy(request)
        # If the object happens to include the tsResponse root tag, strip it out
        if request_copy.tag.find("tsResponse") != -1:
            for r in request_copy:
                request_copy = copy.deepcopy(r)

        # Try to remove any namespaces
        for e in request_copy.iter():
            e.tag = e.tag.replace("{{{}}}".format(self.ns_map['t']), "")
        if request_copy.get('id') is not None:
            del(request_copy.attrib['id'])

        tsr.append(request_copy)
        return tsr

    @staticmethod
    def __build_connection_update_xml(new_server_address: Optional[str] = None,
                                      new_server_port: Optional[str] = None,
                                      new_connection_username: Optional[str] = None,
                                      new_connection_password: Optional[str] = None) -> ET.Element:
        tsr = ET.Element('tsRequest')
        c = ET.Element("connection")
        if new_server_address is not None:
            c.set('serverAddress', new_server_address)
        if new_server_port is not None:
            c.set('serverPort', new_server_port)
        if new_connection_username is not None:
            c.set('userName', new_connection_username)
        if new_connection_username is not None:
            c.set('password', new_connection_password)
        tsr.append(c)
        return tsr

    #
    # Sign-in and Sign-out
    #

    def signin(self, user_luid_to_impersonate: Optional[str] = None):
        self.start_log_block()
        tsr = ET.Element("tsRequest")
        c = ET.Element("credentials")
        c.set("name", self.username)
        c.set("password", self._password)
        s = ET.Element("site")
        if self.site_content_url.lower() not in ['default', '']:
            s.set("contentUrl", self.site_content_url)

        c.append(s)

        if user_luid_to_impersonate is not None:
            u = ET.Element('user')
            u.set('id', user_luid_to_impersonate)
            c.append(u)

        tsr.append(c)

        url = self.build_api_url("auth/signin", server_level=True)

        self.log('Logging in via: {}'.format(url))

        # Create the RestXmlRequest to be used throughout

        self._request_obj = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                                           verify_ssl_cert=self.verify_ssl_cert)
        self._request_obj.xml_request = tsr
        self._request_obj.http_verb = 'post'
        self.log('Login payload is\n {}'.format(ET.tostring(tsr)))
        try:
            self._request_obj.request_from_api(0)
            # self.log(api.get_raw_response())
            xml = self._request_obj.get_response()

            credentials_element = xml.findall('.//t:credentials', self.ns_map)
            self.token = credentials_element[0].get("token")
            self.log("Token is " + self.token)
            self._request_obj.token = self.token
            self.site_luid = credentials_element[0].findall(".//t:site", self.ns_map)[0].get("id")
            self.user_luid = credentials_element[0].findall(".//t:user", self.ns_map)[0].get("id")
            self.log("Site ID is " + self.site_luid)
            self._request_obj.url = None
            self._request_obj.xml_request = None
        except RecoverableHTTPException as e:
            if e.tableau_error_code == '401001':
                self.end_log_block()
                raise NotSignedInException('Sign-in failed due to wrong credentials')
        self.end_log_block()

    def swap_token(self, site_luid: str, user_luid: str, token: str):
        self.start_log_block()
        self.token = token
        # Reset caches if you are changing site
        if self.site_luid != site_luid:
            self.group_name_luid_cache = {}
            self.username_luid_cache = {}
        self.site_luid = site_luid
        self.user_luid = user_luid
        if self._request_obj is None:
            self._request_obj = RestXmlRequest(None, self.token, self.logger, ns_map_url=self.ns_map['t'],
                                               verify_ssl_cert=self.verify_ssl_cert)
            self._request_obj.token = self.token
        else:
            self._request_obj.token = self.token
        self.end_log_block()

    def signout(self, session_token: Optional[str] = None):
        self.start_log_block()
        url = self.build_api_url("auth/signout", server_level=True)
        self.log('Logging out via: {}'.format(url))
        self._request_obj.url = url
        # This allows for signout when using the older session token style
        if session_token is not None:
            self._request_obj.token = session_token

        self._request_obj.http_verb = 'post'
        self._request_obj.request_from_api()
        # Reset the main object to the original token for other requests
        self._request_obj.token = self.token
        self._request_obj.url = None
        self.log('Signed out successfully')
        self.end_log_block()

    def switch_site(self, site_content_url):
        self.start_log_block()
        url = self.build_api_url("auth/signout", server_level=True)
        self.log('Switching site via'.format(url))
        tsr = ET.Element('tsRequest')
        s = ET.Element('site')
        s.set('contentUrl', site_content_url)
        tsr.append(s)

        self._request_obj.http_verb = 'post'
        self.log('Switch site request XML is\n {}'.format(ET.tostring(tsr)))

        self._request_obj.request_from_api(0)
        # self.log(api.get_raw_response())
        xml = self._request_obj.get_response()

        credentials_element = xml.findall('.//t:credentials', self.ns_map)
        self.token = credentials_element[0].get("token")
        self.log("Token is " + self.token)
        self._request_obj.token = self.token
        self.site_luid = credentials_element[0].findall(".//t:site", self.ns_map)[0].get("id")
        self.user_luid = credentials_element[0].findall(".//t:user", self.ns_map)[0].get("id")
        self.log("Site ID is " + self.site_luid)
        self._request_obj.url = None
        self._request_obj.xml_request = None

        self.end_log_block()

    #
    # HTTP "verb" methods. These actually communicate with the RestXmlRequest object to place the requests
    #

    # baseline method for any get request. appends to base url
    def query_resource(self, url_ending: str, server_level:bool = False, filters: Optional[List[UrlFilter]] = None,
                       sorts: Optional[List[Sort]] = None, additional_url_ending: Optional[str] = None,
                       fields: Optional[List[str]] = None) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        url_endings = []
        if filters is not None:
            if len(filters) > 0:
                filters_url = "filter="
                for f in filters:
                    filters_url += f.get_filter_string() + ","
                filters_url = filters_url[:-1]
                url_endings.append(filters_url)
        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = "sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + ","
                sorts_url = sorts_url[:-1]
                url_endings.append(sorts_url)
        if fields is not None:
            if len(fields) > 0:
                fields_url = "fields="
                for field in fields:
                    fields_url += "{},".format(field)
                fields_url = fields_url[:-1]
                url_endings.append(fields_url)
        if additional_url_ending is not None:
            url_endings.append(additional_url_ending)

        first = True
        if len(url_endings) > 0:
            for ending in url_endings:
                if first is True:
                    url_ending += "?{}".format(ending)
                    first = False
                else:
                    url_ending += "&{}".format(ending)

        api_call = self.build_api_url(url_ending, server_level)
        self._request_obj.set_response_type('xml')
        self._request_obj.url = api_call
        self._request_obj.http_verb = 'get'
        self._request_obj.request_from_api()
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        self._request_obj.url = None
        self.end_log_block()
        return xml

    def query_elements_from_endpoint_with_filter(self, element_name: str, name_or_luid: Optional[str] = None,
                                                 all_fields: bool = True) -> ET.Element:

        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = ['workbook', 'user', 'datasource', 'site']
        if element_name in singular_endpoints and self.is_luid(name_or_luid):
            if all_fields is True:
                element = self.query_resource("{}s/{}?fields=_all_".format(element_name, name_or_luid))
            else:
                element = self.query_resource("{}s/{}".format(element_name, name_or_luid))
            self.end_log_block()
            return element
        else:
            if self.is_luid(name_or_luid):
                if all_fields is True:
                    elements = self.query_resource("{}s?fields=_all_".format(element_name))
                else:
                    elements = self.query_resource("{}s".format(element_name))
                luid = name_or_luid
                elements = elements.findall('.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
            else:
                elements = self.query_resource("{}s?filter=name:eq:{}&fields=_all_".format(element_name, name_or_luid))
        self.end_log_block()
        return elements

    def query_single_element_from_endpoint_with_filter(self, element_name: str,
                                                       name_or_luid: Optional[str] = None,
                                                       all_fields: bool = True) -> ET.Element:
        self.start_log_block()
        elements = self.query_elements_from_endpoint_with_filter(element_name, name_or_luid, all_fields=all_fields)

        if len(elements) == 1:
            self.end_log_block()
            return elements[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_luid_from_name(self, content_type: str, name: str, content_url: bool = False) -> str:
        self.start_log_block()
        # If it turns out the name is already a luid, just return it back
        if self.is_luid(name):
            return name

        # Some endpoints have an API filter method:
        content_url_endpoints = ['workbook', 'datasource']
        if content_url is True:
            if content_type not in content_url_endpoints:
                raise InvalidOptionException('Only workbook and datasource can be used as content_type when searching via content_url')
        filterable_endpoints = ['project', 'workbook', 'datasource']  # complete this outy
        if content_type in filterable_endpoints and content_url is False:
            luid = self.query_single_element_luid_from_endpoint_with_filter(element_name=content_type, name=name)
        else:
            if content_url is False:
                luid = self.query_single_element_luid_by_name_from_endpoint(element_name=content_type, name=name)
            # Enable when that method is writtne
            #else:
            #    luid = self.query_luid_from_content_url(content_type=content_type, content_url=content_url)
        self.end_log_block()
        return luid

    # Build this out because sometimes it is the thing you need to search for with Workbooks or Data Sources
    def query_luid_from_content_url(self, content_type: str, content_url: str) -> str:
        pass

    def query_single_element_luid_from_endpoint_with_filter(self, element_name: str, name: str) -> str:
        # Double check which can be optimized with the Fields to only bring back id
        optimizable_fields = ['user', ]
        self.start_log_block()
        if element_name in optimizable_fields:
            elements = self.query_resource("{}s?filter=name:eq:{}&fields=id".format(element_name, name))
        else:
            elements = self.query_resource("{}s?filter=name:eq:{}".format(element_name, name))
        if len(elements) == 1:
            self.end_log_block()
            return elements[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name {}".format(element_name, name))

    def query_single_element_luid_by_name_from_endpoint(self, element_name: str, name: str,
                                                        server_level: bool = False) -> str:
        self.start_log_block()
        # Short circuit if this is already a luid
        if self.is_luid(name):
            return name

        elements = self.query_resource("{}s".format(element_name), server_level=server_level)
        # Groups have a cache within tableau_tools
        if element_name == 'group':
            for e in elements:
                self.group_name_luid_cache[e.get('name')] = e.get('id')
        element = elements.findall('.//t:{}[@name="{}"]'.format(element_name, name), self.ns_map)
        if len(element) == 1:
            self.end_log_block()
            return element[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name {}".format(element_name, name))

    # baseline method for any get request. appends to base url
    def query_resource_json(self, url_ending: str, server_level: bool = False,
                            filters: Optional[List[UrlFilter]] = None,
                            sorts: Optional[List[Sort]] = None, additional_url_ending: str = None,
                            fields: Optional[List[str]] = None, page_number: Optional[int] = None) -> Dict:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        url_endings = []
        if filters is not None:
            if len(filters) > 0:
                filters_url = "filter="
                for f in filters:
                    filters_url += f.get_filter_string() + ","
                filters_url = filters_url[:-1]
                url_endings.append(filters_url)
        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = "sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + ","
                sorts_url = sorts_url[:-1]
                url_endings.append(sorts_url)
        if fields is not None:
            if len(fields) > 0:
                fields_url = "fields="
                for field in fields:
                    fields_url += "{},".format(field)
                fields_url = fields_url[:-1]
                url_endings.append(fields_url)
        if additional_url_ending is not None:
            url_endings.append(additional_url_ending)

        first = True
        if len(url_endings) > 0:
            for ending in url_endings:
                if first is True:
                    url_ending += "?{}".format(ending)
                    first = False
                else:
                    url_ending += "&{}".format(ending)

        api_call = self.build_api_url(url_ending, server_level)
        if self._request_json_obj is None:
            self._request_json_obj = RestJsonRequest(token=self.token, logger=self.logger,
                                                     verify_ssl_cert=self.verify_ssl_cert)
        self._request_json_obj.http_verb = 'get'
        self._request_json_obj.url = api_call
        self._request_json_obj.request_from_api(page_number=page_number)
        json_response = self._request_json_obj.get_response()  # return JSON as string
        self._request_obj.url = None
        self.end_log_block()
        return json_response

    def query_single_element_from_endpoint(self, element_name: str, name_or_luid: str,
                                           server_level: bool = False) -> ET.Element:

        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = ['workbook', 'user', 'datasource', 'site', 'database', 'table']
        if element_name in singular_endpoints and self.is_luid(name_or_luid):
            element = self.query_resource("{}s/{}".format(element_name, name_or_luid))
            self.end_log_block()
            return element
        else:
            elements = self.query_resource("{}s".format(element_name), server_level=server_level)
            if self.is_luid(name_or_luid):
                luid = name_or_luid
            else:
                luid = self.query_single_element_luid_by_name_from_endpoint(element_name, name_or_luid)
            element = elements.findall('.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
        if len(element) == 1:
            self.end_log_block()
            return element[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name or luid {}".format(element_name, name_or_luid))

    def send_post_request(self, url: str) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.http_verb = 'post'
        self._request_obj.request_from_api(0)
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        self._request_obj.url = None
        self.end_log_block()
        return xml



    def send_add_request(self, url: str, request: ET.Element) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.xml_request = request
        self._request_obj.http_verb = 'post'
        self._request_obj.request_from_api(0)  # Zero disables paging, for all non queries
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        # Clean up after request made
        self._request_obj.url = None
        self._request_obj.xml_request = None
        self.end_log_block()
        return xml

    def send_add_request_json(self, url: str, request: Dict) -> Dict:
        self.start_log_block()
        if self._request_json_obj is None:
            self._request_json_obj = RestJsonRequest(token=self.token, logger=self.logger,
                                                     verify_ssl_cert=self.verify_ssl_cert)
        self._request_json_obj.http_verb = 'post'
        self._request_json_obj.url = url
        self._request_json_obj.json_request = request
        self._request_json_obj.request_from_api(0)
        json_response = self._request_json_obj.get_response()
        self._request_json_obj.url = None
        self._request_json_obj.json_request = None
        self.end_log_block()
        return json_response

    def send_update_request(self, url: str, request: ET.Element) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.xml_request = request
        self._request_obj.http_verb = 'put'
        self._request_obj.request_from_api(0)  # Zero disables paging, for all non queries
        self.end_log_block()
        self._request_obj.url = None
        self._request_obj.xml_request = None
        return self._request_obj.get_response()

    def send_delete_request(self, url: str) -> int:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.http_verb = 'delete'

        try:
            self._request_obj.request_from_api(0)  # Zero disables paging, for all non queries
            self._request_obj.url = None
            self.end_log_block()
            # Return for counter
            return 1
        except RecoverableHTTPException as e:
            self.log('Non fatal HTTP Exception Response {}, Tableau Code {}'.format(e.http_code, e.tableau_error_code))
            if e.tableau_error_code in [404003, 404002]:
                self.log('Delete action did not find the resource. Consider successful, keep going')
            self._request_obj.url = None
            self.end_log_block()
        except:
            raise

    def send_publish_request(self, url: str, xml_request: Optional[ET.Element], content: bytes,
                             boundary_string: str) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.set_publish_content(content, boundary_string)
        self._request_obj.xml_request = xml_request
        self._request_obj.http_verb = 'post'
        self._request_obj.request_from_api(0)
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        # Cleanup
        self._request_obj.set_publish_content(None, None)
        self._request_obj.xml_request = None
        self._request_obj.url = None
        self.end_log_block()
        return xml

    def send_append_request(self, url: str, content: bytes, boundary_string: str) -> ET.Element:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.set_publish_content(content, boundary_string)
        self._request_obj.http_verb = 'put'
        self._request_obj.request_from_api(0)
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        # Cleanup
        self._request_obj.set_publish_content(None, None)
        self._request_obj.url = None
        self.end_log_block()
        return xml

    # Used when the result is not going to be XML and you want to save the raw response as binary
    def send_binary_get_request(self, url: str) -> bytes:
        self.start_log_block()
        if self.token == "":
            raise NotSignedInException('Must use .signin() to create REST API session first')
        self._request_obj.url = url

        self._request_obj.http_verb = 'get'
        self._request_obj.set_response_type('binary')
        self._request_obj.request_from_api(0)
        # Set this content type so we can set the file extension
        self._last_response_content_type = self._request_obj.get_last_response_content_type()

        # Cleanup
        self._request_obj.url = None

        self.end_log_block()
        return self._request_obj.get_response()

    # Generic implementation of all content publishing
    def _publish_content(self, content_type: str, content_filename: str, content_name: str, project_luid: str,
                         url_params: Optional[Dict] = None,
                         connection_username: Optional[str] = None, connection_password: Optional[str] = None,
                         save_credentials: bool = True, show_tabs: bool = False,
                         check_published_ds: bool = True, oauth_flag: bool = False,
                         generate_thumbnails_as_username_or_luid: Optional[str] = None,
                         description: Optional[str] = None, views_to_hide_list: Optional[List[str]] = None):
        # Single upload limit in MB
        single_upload_limit = 20

        # If you need a temporary copy when fixing the published datasources
        temp_wb_filename = None

        # Must be 'workbook' or 'datasource' or 'flow'
        if content_type not in ['workbook', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be 'workbook',  'datasource', or 'flow' ")

        file_extension = None
        final_filename = None
        cleanup_temp_file = False

        for ending in ['.twb', '.twbx', '.tde', '.tdsx', '.tds', '.tde', '.hyper', '.tfl', '.tflx']:
            if content_filename.endswith(ending):
                file_extension = ending[1:]

                # If twb or twbx, open up and check for any published data sources
                # Legacy and think it is unnecessary in any current version. Keeping code in case that proves untrue
                #if file_extension.lower() in ['twb', 'twbx'] and check_published_ds is True:
                #    self.log("Adjusting any published datasources")
                #    t_file = TableauFile(content_filename, self.logger)
                #    dses = t_file.tableau_document.datasources
                #    for ds in dses:
                #        # Set to the correct site
                #        if ds.published is True:
                #            self.log("Published datasource found")
                #            self.log("Setting publish datasource repository to {}".format(self.site_content_url))
                #            ds.published_ds_site = self.site_content_url

                #    temp_wb_filename = t_file.save_new_file('temp_wb')
                #    content_filename = temp_wb_filename

                # Open the file to be uploaded
                try:
                    content_file = open(content_filename, 'rb')
                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(content_filename))
                    raise
                file_size = os.path.getsize(content_filename)
                file_size_mb = float(file_size) / float(1000000)
                self.log("File {} is size {} MBs".format(content_filename, file_size_mb))
                final_filename = content_filename

                # Request type is mixed and require a boundary
                boundary_string = self.generate_boundary_string()

                # Create the initial XML portion of the request
                publish_request = bytes("--{}\r\n".format(boundary_string).encode('utf-8'))
                publish_request += bytes('Content-Disposition: name="request_payload"\r\n'.encode('utf-8'))
                publish_request += bytes('Content-Type: text/xml\r\n\r\n'.encode('utf-8'))

                # Build publish request in ElementTree then convert at publish
                publish_request_xml = ET.Element('tsRequest')
                # could be either workbook, datasource, or flow
                t1 = ET.Element(content_type)
                t1.set('name', content_name)
                if show_tabs is not False:
                    t1.set('showTabs', str(show_tabs).lower())
                if generate_thumbnails_as_username_or_luid is not None:
                    if self.is_luid(generate_thumbnails_as_username_or_luid):
                        thumbnail_user_luid = generate_thumbnails_as_username_or_luid
                    else:
                        thumbnail_user_luid = self.query_user_luid(generate_thumbnails_as_username_or_luid)
                    t1.set('generateThumbnailsAsUser', thumbnail_user_luid)

                if connection_username is not None:
                    cc = ET.Element('connectionCredentials')
                    cc.set('name', connection_username)
                    if oauth_flag is True:
                        cc.set('oAuth', "True")
                    if connection_password is not None:
                        cc.set('password', connection_password)
                    cc.set('embed', str(save_credentials).lower())
                    t1.append(cc)

                # Views to Hide in Workbooks from 3.2
                if views_to_hide_list is not None:
                    if len(views_to_hide_list) > 0:
                        vs = ET.Element('views')
                        for view_name in views_to_hide_list:
                            v = ET.Element('view')
                            v.set('name', view_name)
                            v.set('hidden', 'true')
                        t1.append(vs)

                # Description only allowed for Flows as of 3.3
                if description is not None:
                     t1.set('description', description)
                p = ET.Element('project')
                p.set('id', project_luid)
                t1.append(p)
                publish_request_xml.append(t1)

                encoded_request = ET.tostring(publish_request_xml, encoding='utf-8')

                publish_request += bytes(encoded_request)
                publish_request += bytes("\r\n--{}".format(boundary_string).encode('utf-8'))

                # Upload as single if less than file_size_limit MB
                if file_size_mb <= single_upload_limit:
                    # If part of a single upload, this if the next portion
                    self.log("Less than {} MB, uploading as a single call".format(str(single_upload_limit)))
                    publish_request += bytes('\r\n'.encode('utf-8'))
                    publish_request += bytes('Content-Disposition: name="tableau_{}"; filename="{}"\r\n'.format(
                        content_type, final_filename).encode('utf-8'))
                    publish_request += bytes('Content-Type: application/octet-stream\r\n\r\n'.encode('utf-8'))

                    # Content needs to be read unencoded from the file
                    content = content_file.read()

                    # Add to string as regular binary, no encoding
                    publish_request += content

                    publish_request += bytes("\r\n--{}--".format(boundary_string).encode('utf-8'))

                    url = self.build_api_url("{}s").format(content_type)

                    # Allow additional parameters on the publish url
                    if len(url_params) > 0:
                        additional_params = '?'
                        i = 1
                        for param in url_params:
                            if i > 1:
                                additional_params += "&"
                            additional_params += "{}={}".format(param, str(url_params[param]).lower())
                            i += 1
                        url += additional_params

                    content_file.close()
                    if temp_wb_filename is not None:
                        os.remove(temp_wb_filename)
                    if cleanup_temp_file is True:
                        os.remove(final_filename)

                    results = self.send_publish_request(url=url, xml_request=None, content=publish_request,
                                                 boundary_string=boundary_string)
                    return results
                # Break up into chunks for upload
                else:
                    self.log("Greater than 10 MB, uploading in chunks")
                    upload_session_id = self.initiate_file_upload()

                    # Upload each chunk
                    for piece in self.read_file_in_chunks(content_file):
                        self.log("Appending chunk to upload session {}".format(upload_session_id))
                        self.append_to_file_upload(upload_session_id, piece, final_filename)

                    # Finalize the publish
                    url = self.build_api_url("{}s").format(content_type) + "?uploadSessionId={}".format(
                        upload_session_id) + "&{}Type={}".format(content_type, file_extension)

                    # Allow additional parameters on the publish url
                    if len(url_params) > 0:
                        additional_params = '&'
                        i = 1
                        for param in url_params:
                            if i > 1:
                                additional_params += "&"
                            additional_params += "{}={}".format(param, str(url_params[param]).lower())
                            i += 1
                        url += additional_params

                    publish_request += bytes("--".encode('utf-8'))  # Need to finish off the last boundary
                    self.log("Finishing the upload with a publish request")
                    content_file.close()
                    if temp_wb_filename is not None:
                        os.remove(temp_wb_filename)
                    if cleanup_temp_file is True:
                        os.remove(final_filename)
                    return self.send_publish_request(url=url, xml_request=None, content=publish_request,
                                                     boundary_string=boundary_string)

        if file_extension is None:
            raise InvalidOptionException(
                "File {} does not have an acceptable extension. Should be .twb,.twbx,.tde,.tdsx,.tds,.tde, .tfl, .tlfx, .hyper".format(
                    content_filename))

    def initiate_file_upload(self) -> str:
        url = self.build_api_url("fileUploads")
        xml = self.send_post_request(url)
        file_upload = xml.findall('.//t:fileUpload', self.ns_map)
        return file_upload[0].get("uploadSessionId")

    # Uploads a chunk to an already started session
    def append_to_file_upload(self, upload_session_id: str, content: bytes, filename: str):
        boundary_string = self.generate_boundary_string()
        publish_request = bytes("--{}\r\n".format(boundary_string))
        publish_request += bytes('Content-Disposition: name="request_payload"\r\n')
        publish_request += bytes('Content-Type: text/xml\r\n\r\n')
        publish_request += bytes('\r\n')
        publish_request += bytes("--{}\r\n".format(boundary_string))
        publish_request += bytes('Content-Disposition: name="tableau_file"; filename="{}"\r\n'.format(
            filename))
        publish_request += bytes('Content-Type: application/octet-stream\r\n\r\n')

        publish_request += content

        publish_request += "\r\n--{}--".format(boundary_string)
        url = self.build_api_url("fileUploads/{}".format(upload_session_id))
        self.send_append_request(url=url, content=publish_request, boundary_string=boundary_string)

    # Generic implementation of all the CSV/PDF/PNG requests
    def _query_data_file(self, download_type: str, view_name_or_luid: str, high_resolution: Optional[bool] = None,
                         view_filter_map: Optional[Dict] = None,
                         wb_name_or_luid: Optional[str] = None, proj_name_or_luid: Optional[str] = None) -> bytes:
        self.start_log_block()
        view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)
        url_param_map = {}
        if view_filter_map is not None:
            for key in view_filter_map:
                new_key = "vf_{}".format(key)
                # Check if this just a string
                if isinstance(view_filter_map[key], str):
                    value = view_filter_map[key]
                else:
                    value = ",".join(map(str, view_filter_map[key]))
                url_param_map[new_key] = value

        if high_resolution is True:
                url_param_map['resolution'] = "high"

        url_params_str = self.build_url_parameter_string(map_dict=url_param_map)
        try:

            url = self.build_api_url("views/{}/{}".format(view_luid, download_type), url_parameters=url_params_str)
            binary_result = self.send_binary_get_request(url)

            self.end_log_block()
            return binary_result
        except RecoverableHTTPException as e:
            self.log("Attempt to request results in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                           e.tableau_error_code))
            self.end_log_block()
            raise
    def query_server_info(self) -> ET.Element:
        self.start_log_block()
        server_info = self.query_resource("serverinfo", server_level=True)
        self.end_log_block()
        return server_info

    def query_server_version(self) -> str:
        self.start_log_block()
        server_info = self.query_server_info()
        # grab the server number
        for t in server_info:
            if t.tag.find('restApiVersion') != -1:
                self.end_log_block()
                return t.text

    def query_build_number(self) -> str:
        self.start_log_block()
        server_info = self.query_server_info()
        # grab the server number
        for t in server_info:
            if t.tag.find('restApiVersion') != -1:
                self.end_log_block()
                return t.get('build')

    def query_api_version(self):
        self.start_log_block()
        server_info = self.query_server_info()
        # grab api version number
        for t in server_info:
            if t.tag.find('productVersion') != -1:
                self.end_log_block()
                return t.text

    def update_online_site_logo(self, image_filename: str):
        # Request type is mixed and require a boundary
        boundary_string = self.generate_boundary_string()
        file_extension = None
        for ending in ['.png', ]:
            if image_filename.endswith(ending):
                file_extension = ending[1:]
                # Open the file to be uploaded
                try:
                    content_file = open(image_filename, 'rb')
                    # Create the initial XML portion of the request
                    publish_request = bytes("--{}\r\n".format(boundary_string))
                    publish_request += bytes(
                        'Content-Disposition: form-data; name="site_logo"; filename="new_site_logo.png"\r\n')
                    publish_request += bytes('Content-Type: application/octet-stream\r\n\r\n')

                    publish_request += bytes("--{}".format(boundary_string))

                    # Content needs to be read unencoded from the file
                    content = content_file.read()

                    # Add to string as regular binary, no encoding
                    publish_request += content

                    publish_request += bytes("\r\n--{}--".format(boundary_string))
                    url = self.build_api_url('')[:-1]
                    return self.send_publish_request(url=url, xml_request=None, content=publish_request,
                                                     boundary_string=boundary_string)
                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(image_filename))
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                "File {} is not PNG. Use PNG image.".format(image_filename))

    def restore_online_site_logo(self):
        boundary_string = self.generate_boundary_string()

        # Create the initial XML portion of the request
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: form-data; name="site_logo"; filename="empty.txt"\r\n'
        publish_request += 'Content-Type: text/plain\r\n\r\n'

        publish_request += "--{}".format(boundary_string)

        url = self.build_api_url('')[:-1]
        return self.send_publish_request(url, publish_request, None, boundary_string)

class TableauRestApiBase27(TableauRestApiBase):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('10.4')
        self.url_filters = UrlFilter27

class TableauRestApiBase28(TableauRestApiBase27):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('10.5')

    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project28:
        luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project28(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj)
        return proj_obj

class TableauRestApiBase30(TableauRestApiBase28):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2018.1')



class TableauRestApiBase31(TableauRestApiBase30):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2018.2')
        self.url_filters = UrlFilter31

class TableauRestApiBase32(TableauRestApiBase31):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2018.3')

class TableauRestApiBase33(TableauRestApiBase32):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2019.1')
        self.url_filters = UrlFilter33

    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[ET.Element] = None) -> Project33:
        luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project33(luid=luid, tableau_rest_api_obj=self, tableau_server_version=self.version,
                             logger_obj=self.logger, content_xml_obj=project_xml_obj)
        return proj_obj

    def get_published_flow_object(self, flow_name_or_luid: str,
                                     flow_xml_obj: Optional[ET.Element] = None) -> Flow33:
        luid = self.query_flow_luid(flow_name_or_luid)
        flow_obj = Flow33(luid=luid, tableau_rest_api_obj=self, tableau_server_version=self.version,
                          logger_obj=self.logger, content_xml_obj=flow_xml_obj)
        return flow_obj

class TableauRestApiBase34(TableauRestApiBase33):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2019.2')

    # Generic implementation of all the CSV/PDF/PNG requests
    def _query_data_file(self, download_type: str, view_name_or_luid: Optional[str] = None, high_resolution: bool = False,
                         view_filter_map=Dict, wb_name_or_luid: Optional[str] = None,
                         proj_name_or_luid: Optional[str] = None, max_age_minutes: Optional[int] = None,
                         page_orientation: Optional[str] = None, page_type: Optional[str] = None) -> bytes:

        self.start_log_block()
        url_param_map = {}
        if view_filter_map is not None:
            for key in view_filter_map:
                new_key = "vf_{}".format(key)
                # Check if this just a string
                if isinstance(view_filter_map[key], str):
                    value = view_filter_map[key]
                else:
                    value = ",".join(map(str,view_filter_map[key]))
                url_param_map[new_key] = value

        if high_resolution is True:
            url_param_map['resolution'] = "high"
        if page_type is not None:
            url_param_map['page-type'] = page_type
        if page_orientation is not None:
            url_param_map['page-orientation'] = page_orientation

        url_params_str = self.build_url_parameter_string(map_dict=url_param_map)
        try:
            # Workbook PDF request is only like this right now
            if view_name_or_luid is None:
                wb_luid = self.query_workbook_luid(wb_name=wb_name_or_luid, proj_name_or_luid=proj_name_or_luid)
                url = self.build_api_url("workbooks/{}/{}".format(wb_luid, download_type),
                                         url_parameters=url_params_str)
            else:
                view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                          proj_name_or_luid=proj_name_or_luid)
                url = self.build_api_url("views/{}/{}".format(view_luid, download_type),
                                         url_parameters=url_params_str)
            binary_result = self.send_binary_get_request(url)

            self.end_log_block()
            return binary_result
        except RecoverableHTTPException as e:
            self.log("Attempt to request results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise

# NEED TO IMPLEMENT
class TableauRestApiBase35(TableauRestApiBase34):
    def __init__(self, server: str, username: str, password: str, site_content_url: Optional[str] = ""):
        TableauRestApiBase.__init__(self, server=server, username=username, password=password,
                                    site_content_url=site_content_url)
        self.set_tableau_server_version('2019.3')




class TableauRestApiBase36(TableauRestApiBase35):
    def __init__(self, server: str, username: Optional[str] = None, password: Optional[str] = None,
                 pat_name: Optional[str] = None, pat_secret: Optional[str] = None,
                 site_content_url: Optional[str] = ""):
        #TableauRestApiBase.__init__()
        if server.find('http') == -1:
            raise InvalidOptionException('Server URL must include http:// or https://')

        ET.register_namespace('t', self.ns_map['t'])
        self.server: str = server
        self.site_content_url: str = site_content_url
        self.username: str = username
        self._password: str = password
        self._pat_name: str = pat_name
        self._pat_secret: str = pat_secret
        self._token: str = ""  # Holds the login token from the Sign In call
        self.site_luid: str = ""
        self.user_luid: str = ""
        self._login_as_user_id: str = ""
        self._last_error = None
        self.logger: Optional[Logger] = None
        self._last_response_content_type = None

        self._request_obj: Optional[RestXmlRequest] = None
        self._request_json_obj: Optional[RestJsonRequest] = None

        # Lookup caches to minimize calls
        self.username_luid_cache = {}
        self.group_name_luid_cache = {}

        # For working around SSL issues
        self.verify_ssl_cert = True

        self.version: Optional[str] = None
        self.api_version: Optional[str]  = None
        # Starting in version 5 of tableau_tools, 10.3 is the lowest supported version
        self.set_tableau_server_version("2019.4")

    def signin(self, user_luid_to_impersonate: Optional[str] = None):
        self.start_log_block()
        tsr = ET.Element("tsRequest")
        c = ET.Element("credentials")
        if self._pat_name is not None:
            if self._pat_secret is not None:
                c.set('personalAccessTokenName', self._pat_name)
                c.set('personalAccessTokenSecret', self._pat_secret)
            else:
                raise InvalidOptionException('Must include both pat_name and pat_secret to use PAT for login')
        else:
            if self.username is not None:
                if self._password is not None:
                    c.set("name", self.username)
                    c.set("password", self._password)
                else:
                    raise InvalidOptionException('Must include both username and password to login without PAT')
        s = ET.Element("site")
        if self.site_content_url.lower() not in ['default', '']:
            s.set("contentUrl", self.site_content_url)

        c.append(s)


        if user_luid_to_impersonate is not None:
            if self._pat_name is not None:
                raise InvalidOptionException('Impersonation is not available when using PAT login')
            else:
                u = ET.Element('user')
                u.set('id', user_luid_to_impersonate)
                c.append(u)

        tsr.append(c)

        url = self.build_api_url("auth/signin", server_level=True)

        self.log('Logging in via: {}'.format(url))

        # Create the RestXmlRequest to be used throughout

        self._request_obj = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                                           verify_ssl_cert=self.verify_ssl_cert)
        self._request_obj.xml_request = tsr
        self._request_obj.http_verb = 'post'
        self.log('Login payload is\n {}'.format(ET.tostring(tsr)))

        self._request_obj.request_from_api(0)
        # self.log(api.get_raw_response())
        xml = self._request_obj.get_response()

        credentials_element = xml.findall('.//t:credentials', self.ns_map)
        self.token = credentials_element[0].get("token")
        self.log("Token is " + self.token)
        self._request_obj.token = self.token
        self.site_luid = credentials_element[0].findall(".//t:site", self.ns_map)[0].get("id")
        self.user_luid = credentials_element[0].findall(".//t:user", self.ns_map)[0].get("id")
        self.log("Site ID is " + self.site_luid)
        self._request_obj.url = None
        self._request_obj.xml_request = None
        self.end_log_block()
