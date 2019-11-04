# -*- coding: utf-8 -*-

import os
from typing import Union, Any, Optional, List

from ..tableau_base import *
from ..tableau_documents.tableau_file import TableauFile
from ..tableau_documents.tableau_workbook import TableauWorkbook
from ..tableau_documents.tableau_datasource import TableauDatasource
from ..tableau_exceptions import *
from .rest_xml_request import RestXmlRequest
from .rest_json_request import RestJsonRequest
from .published_content import Project20, Project21, Project28, Workbook, Datasource
from .url_filter import *
from .sort import *
import copy


class TableauRestApiConnection(TableauBase):
    # Defines a class that represents a RESTful connection to Tableau Server. Use full URL (http:// or https://)
    def __init__(self, server: str, username: str, password: str, site_content_url: str =""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauBase.__init__(self)
        if server.find('http') == -1:
            raise InvalidOptionException('Server URL must include http:// or https://')

        etree.register_namespace('t', self.ns_map['t'])
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

        # All defined in TableauBase superclass
        self._site_roles = self.site_roles
        self._permissionable_objects = self.permissionable_objects
        self._server_to_rest_capability_map = self.server_to_rest_capability_map

        # Lookup caches to minimize calls
        self.username_luid_cache = {}
        self.group_name_luid_cache = {}

        # For working around SSL issues
        self.verify_ssl_cert = True

        # Starting in version 5 of tableau_tools, 10.3 is the lowest supported version
        self.set_tableau_server_version("10.3")

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

    def build_api_url(self, call: str, server_level: bool = False) -> str:
        if server_level is True:
            return "{}/api/{}/{}".format(self.server, self.api_version, call)
        else:
            return "{}/api/{}/sites/{}/{}".format(self.server, self.api_version, self.site_luid, call)

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

    #
    # Internal REST API Helpers (mostly XML definitions that are reused between methods)
    #
    @staticmethod
    def build_site_request_xml(site_name: Optional[str] = None, content_url: Optional[str] = None,
                               admin_mode: Optional[str] = None, user_quota: Optional[str] = None,
                               storage_quota: Optional[str] = None, disable_subscriptions: Optional[bool] = None,
                               state: Optional[str] = None,
                               revision_history_enabled: Optional[bool] = None, revision_limit: Optional[str] = None):
        tsr = etree.Element("tsRequest")
        s = etree.Element('site')

        if site_name is not None:
            s.set('name', site_name)
        if content_url is not None:
            s.set('contentUrl', content_url)
        if admin_mode is not None:
            s.set('adminMode', admin_mode)
        if user_quota is not None:
            s.set('userQuota', str(user_quota))
        if state is not None:
            s.set('state', state)
        if storage_quota is not None:
            s.set('storageQuota', str(storage_quota))
        if disable_subscriptions is not None:
            s.set('disableSubscriptions', str(disable_subscriptions).lower())
        if revision_history_enabled is not None:
            s.set('revisionHistoryEnabled', str(revision_history_enabled).lower())
        if revision_limit is not None:
            s.set('revisionLimit', str(revision_limit))

        tsr.append(s)
        return tsr

    # This is specifically for replication from one site to another
    def build_request_from_response(self, request: etree.Element) -> etree.Element:
        tsr = etree.Element('tsRequest')
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
                                      new_connection_password: Optional[str] = None) -> etree.Element:
        tsr = etree.Element('tsRequest')
        c = etree.Element("connection")
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
    # Factory methods for PublishedContent and Permissions objects
    #
    def get_published_project_object(self, project_name_or_luid: str,
                                     project_xml_obj: Optional[etree.Element] = None) -> Project21:
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project21(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj)
        return proj_obj

    def get_published_workbook_object(self, workbook_name_or_luid: str,
                                      project_name_or_luid: Optional[str] = None) -> Workbook:
        if self.is_luid(workbook_name_or_luid):
            luid = workbook_name_or_luid
        else:
            luid = self.query_workbook_luid(workbook_name_or_luid, project_name_or_luid)
        wb_obj = Workbook(luid, self, tableau_server_version=self.version, default=False, logger_obj=self.logger)
        return wb_obj

    def get_published_datasource_object(self, datasource_name_or_luid: str,
                                        project_name_or_luid: Optional[str] = None) -> Datasource:
        if self.is_luid(datasource_name_or_luid):
            luid = datasource_name_or_luid
        else:
            luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        ds_obj = Datasource(luid, self, tableau_server_version=self.version, default=False, logger_obj=self.logger)
        return ds_obj

    #
    # Sign-in and Sign-out
    #

    def signin(self, user_luid_to_impersonate: Optional[str] = None):
        self.start_log_block()
        tsr = etree.Element("tsRequest")
        c = etree.Element("credentials")
        c.set("name", self.username)
        c.set("password", self._password)
        s = etree.Element("site")
        if self.site_content_url.lower() not in ['default', '']:
            s.set("contentUrl", self.site_content_url)

        c.append(s)

        if user_luid_to_impersonate is not None:
            u = etree.Element('user')
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
        self.log('Login payload is\n {}'.format(etree.tostring(tsr)))

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

    def swap_token(self, site_luid: str, user_luid: str, token: str):
        self.start_log_block()
        self.token = token
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

    #
    # HTTP "verb" methods. These actually communicate with the RestXmlRequest object to place the requests
    #

    # baseline method for any get request. appends to base url
    def query_resource(self, url_ending: str, server_level:bool = False, filters: Optional[list[UrlFilter]] = None,
                       sorts: Optional[list[Sort]] = None, additional_url_ending: Optional[str] = None,
                       fields: Optional[list[str]] = None) -> etree.Element:
        self.start_log_block()
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
                                                 all_fields: bool = True) -> etree.Element:

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
                                                       all_fields: bool = True) -> etree.Element:
        self.start_log_block()
        elements = self.query_elements_from_endpoint_with_filter(element_name, name_or_luid, all_fields=all_fields)

        if len(elements) == 1:
            self.end_log_block()
            return elements[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_single_element_luid_from_endpoint_with_filter(self, element_name: str, name: str,
                                                            optimize_with_field: bool = False) -> str:
        self.start_log_block()
        if optimize_with_field is True:
            elements = self.query_resource("{}s?filter=name:eq:{}&fields=id".format(element_name, name))
        else:
            elements = self.query_resource("{}s?filter=name:eq:{}".format(element_name, name))
        if len(elements) == 1:
            self.end_log_block()
            return elements[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name {}".format(element_name, name))

    # baseline method for any get request. appends to base url
    def query_resource_json(self, url_ending: str, server_level: bool = False,
                            filters: Optional[list[UrlFilter]] = None,
                            sorts: Optional[list[Sort]] = None, additional_url_ending: str = None,
                            fields: Optional[list[str]] = None, page_number: Optional[int] = None) -> str:
        self.start_log_block()
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
                                           server_level: bool = False) -> etree.Element:

        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = ['workbook', 'user', 'datasource', 'site']
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

    def query_single_element_luid_by_name_from_endpoint(self, element_name: str, name: str,
                                                        server_level: bool = False) -> str:
        self.start_log_block()
        elements = self.query_resource("{}s".format(element_name), server_level=server_level)
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

    def send_post_request(self, url: str) -> etree.Element:
        self.start_log_block()
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.http_verb = 'post'
        self._request_obj.request_from_api(0)
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        self._request_obj.url = None
        self.end_log_block()
        return xml

    def send_add_request(self, url: str, request: etree.Element) -> etree.Element:

        self.start_log_block()

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

    def send_update_request(self, url: str, request: etree.Element) -> etree.Element:
        self.start_log_block()

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

    def send_publish_request(self, url: str, xml_request: etree.Element, content,
                             boundary_string: str) -> etree.Element:
        self.start_log_block()

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

    def send_append_request(self, url: str, request, boundary_string: str) -> etree.Element:
        self.start_log_block()
        self._request_obj.set_response_type('xml')
        self._request_obj.url = url
        self._request_obj.set_publish_content(request, boundary_string)
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

    #
    # Basic Querying / Get Methods
    #

    #
    # Begin Datasource Querying Methods
    #

    def query_datasources(self, project_name_or_luid=None, all_fields=True, updated_at_filter=None, created_at_filter=None,
                          tags_filter=None, datasource_type_filter=None, sorts=None, fields=None):
        """
        :type project_name_or_luid: unicode
        :type all_fields: bool
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type datasource_type_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource('datasources', filters=filters, sorts=sorts, fields=fields)

        # If there is a project filter
        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            dses_in_project = datasources.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            dses = etree.Element(self.ns_prefix + 'datasources')
            for ds in dses_in_project:
                dses.append(ds)
        else:
            dses = datasources

        self.end_log_block()
        return dses

    def query_datasources_json(self, project_name_or_luid=None, all_fields=True, updated_at_filter=None,
                               created_at_filter=None, tags_filter=None, datasource_type_filter=None, sorts=None,
                               fields=None, page_number=None):
        """
        :type project_name_or_luid: unicode
        :type all_fields: bool
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type datasource_type_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource_json('datasources', filters=filters, sorts=sorts, fields=fields,
                                               page_number=page_number)

        self.end_log_block()
        return datasources

    # Tries to guess name or LUID, hope there is only one
    def query_datasource(self, ds_name_or_luid, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()

        # LUID
        if self.is_luid(ds_name_or_luid):
            ds = self.query_resource("datasources/{}".format(ds_name_or_luid))
        # Name
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
            ds = self.query_resource("datasources/{}".format(ds_luid))
        self.end_log_block()
        return ds

    # Filtering implemented in 2.2
    # query_workbook and query_workbook_luid can't be improved because filtering doesn't take a Project Name/LUID

    # Datasources in different projects can have the same 'pretty name'.
    def query_datasource_luid(self, datasource_name, project_name_or_luid=None, content_url=None):
        """
        :type datasource_name: unicode
        :type project_name_or_luid: unicode
        :type content_url: unicode
        :rtype: unicode
        """
        self.start_log_block()
        # This quick filters down to just those with the name
        datasources_with_name = self.query_elements_from_endpoint_with_filter('datasource', datasource_name)

        # Throw exception if nothing found
        if len(datasources_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException("No datasource found with name {} in any project".format(datasource_name))

        # Search for ContentUrl which should be unique, return
        if content_url is not None:
            datasources_with_content_url = datasources_with_name.findall('.//t:datasource[@contentUrl="{}"]'.format(content_url), self.ns_map)
            self.end_log_block()
            if len(datasources_with_name == 1):
                return datasources_with_content_url[0].get("id")
            else:
                raise NoMatchFoundException("No datasource found with ContentUrl {}".format(content_url))
        # If no ContentUrl search, find any with the name
        else:
            # If no match, exception

            # If no Project Name is specified, but only one match, return, otherwise throw MultipleMatchesException
            if project_name_or_luid is None:
                if len(datasources_with_name) == 1:
                    self.end_log_block()
                    return datasources_with_name[0].get("id")
                # If no project is declared, and more than one match
                else:
                    raise MultipleMatchesFoundException('More than one datasource found by name {} without a project specified'.format(datasource_name))
            # If Project_name is specified was filtered above, so find the name
            else:
                if self.is_luid(project_name_or_luid):
                    ds_in_proj = datasources_with_name.findall('.//t:project[@id="{}"]/..'.format(project_name_or_luid),
                                                               self.ns_map)
                else:
                    ds_in_proj = datasources_with_name.findall('.//t:project[@name="{}"]/..'.format(project_name_or_luid),
                                                               self.ns_map)
                if len(ds_in_proj) == 1:
                    self.end_log_block()
                    return ds_in_proj[0].get("id")
                else:
                    self.end_log_block()
                    raise NoMatchFoundException("No datasource found with name {} in project {}".format(datasource_name, project_name_or_luid))

    def query_datasource_content_url(self, datasource_name_or_luid, project_name_or_luid=None):
        """
        :type datasource_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        ds = self.query_datasource(datasource_name_or_luid, project_name_or_luid)
        content_url = ds.get('contentUrl')
        self.end_log_block()
        return content_url


    #
    # End Datasource Query Methods
    #

    #
    # Start Group Query Methods
    #

    def query_groups(self) -> etree.Element:
        self.start_log_block()
        groups = self.query_resource("groups")
        for group in groups:
            # Add to group-name : luid cache
            group_luid = group.get("id")
            group_name = group.get('name')
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

    # # No basic verb for querying a single group, so run a query_groups

    def query_groups_json(self, page_number: Optional[int]=None) -> str:
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        groups = self.query_resource_json("groups", page_number=page_number)
        #for group in groups:
        #    # Add to group-name : luid cache
        #    group_luid = group.get(u"id")
        #    group_name = group.get(u'name')
        #    self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

    def query_group(self, group_name_or_luid: str) -> etree.Element:
        self.start_log_block()
        group = self.query_single_element_from_endpoint('group', group_name_or_luid)
        # Add to group_name : luid cache
        group_luid = group.get("id")
        group_name = group.get('name')
        self.group_name_luid_cache[group_name] = group_luid

        self.end_log_block()
        return group

    # Groups luckily cannot have the same 'pretty name' on one site
    def query_group_luid(self, group_name: str) -> str:
        self.start_log_block()
        if group_name in self.group_name_luid_cache:
            group_luid = self.group_name_luid_cache[group_name]
            self.log('Found group name {} in cache with luid {}'.format(group_name, group_luid))
        else:
            group_luid = self.query_single_element_luid_by_name_from_endpoint('group', group_name)
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_luid

    def query_group_name(self, group_luid: str) -> str:
        self.start_log_block()
        for name, luid in list(self.group_name_luid_cache.items()):
            if luid == group_luid:
                group_name = name
                self.log('Found group name {} in cache with luid {}'.format(group_name, group_luid))
                return group_name
        # If match is found
        group = self.query_single_element_from_endpoint('group', group_luid)
        group_luid = group.get("id")
        group_name = group.get('name')
        self.log('Loading the Group: LUID cache')
        self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_name


    #
    # End Group Querying methods
    #

    #
    # Start Project Querying methods
    #

    def query_projects(self) -> etree.Element:
        self.start_log_block()
        projects = self.query_resource("projects")
        self.end_log_block()
        return projects

    def query_projects_json(self, page_number: Optional[int] = None) -> str:
        self.start_log_block()
        projects = self.query_resource_json("projects", page_number=page_number)
        self.end_log_block()
        return projects

    def create_project(self, project_name: Optional[str] = None, project_desc: Optional[str] = None,
                       locked_permissions: bool = True, publish_samples: bool = False,
                       no_return: Optional[bool] = False,
                       direct_xml_request: Optional[etree.Element] = None) -> Project21:
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            p = etree.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")
            tsr.append(p)

        url = self.build_api_url("projects")
        if publish_samples is True:
            url += '?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(
                    project_name))
                self.end_log_block()
                if no_return is False:
                    return self.get_published_project_object(project_name_or_luid=project_name)

    def query_project_luid(self, project_name: str) -> str:
        self.start_log_block()
        project_luid = self.query_single_element_luid_by_name_from_endpoint('project', project_name)
        self.end_log_block()
        return project_luid

    def query_project_xml_object(self, project_name_or_luid):
        """
        :param project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_xml = self.query_single_element_from_endpoint('project', luid)
        self.end_log_block()
        return proj_xml

    #
    # End Project Querying Methods
    #

    #
    # Start Site Querying Methods
    #

    # Site queries don't have the site portion of the URL, so login option gets correct format
    def query_sites(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        sites = self.query_resource("sites", server_level=True)
        self.end_log_block()
        return sites

    def query_sites_json(self, page_number=None):
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        sites = self.query_resource_json("sites", server_level=True, page_number=page_number)
        self.end_log_block()
        return sites

    # Methods for getting info about the sites, since you can only query a site when you are signed into it

    # Return list of all site contentUrls
    def query_all_site_content_urls(self):
        """
        :rtype: list[unicode]
        """
        self.start_log_block()
        sites = self.query_sites()
        site_content_urls = []
        for site in sites:
            site_content_urls.append(site.get("contentUrl"))
        self.end_log_block()
        return site_content_urls

    # You can only query a site you have logged into this way. Better to use methods that run through query_sites
    def query_current_site(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        site = self.query_resource("sites/{}".format(self.site_luid), server_level=True)
        self.end_log_block()
        return site

    #
    # End Site Querying Methods
    #

    #
    # Start User Querying Methods
    #

    # The reference has this name, so for consistency adding an alias
    def get_users(self, all_fields=True, last_login_filter=None, site_role_filter=None, sorts=None, fields=None):
        """
        :type all_fields: bool
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        return self.query_users(all_fields=all_fields, last_login_filter=last_login_filter,
                                site_role_filter=site_role_filter, sorts=sorts, fields=fields)


    def query_users(self, all_fields=True, last_login_filter=None, site_role_filter=None, sorts=None, fields=None,
                    username_filter=None):
        """
        :type all_fields: bool
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type username_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource("users", filters=filters, sorts=sorts, fields=fields)
        self.log('Found {} users'.format(str(len(users))))
        self.end_log_block()
        return users

    # The reference has this name, so for consistency adding an alias
    def get_users_json(self, all_fields=True, last_login_filter=None, site_role_filter=None, sorts=None, fields=None,
                       page_number=None):
        """
        :type all_fields: bool
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
        """
        return self.query_users_json(all_fields=all_fields, last_login_filter=last_login_filter,
                                site_role_filter=site_role_filter, sorts=sorts, fields=fields, page_number=page_number)


    def query_users_json(self, all_fields=True, last_login_filter=None, site_role_filter=None, sorts=None, fields=None,
                         username_filter=None, page_number=None):
        """
        :type all_fields: bool
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type username_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource_json("users", filters=filters, sorts=sorts, fields=fields, page_number=page_number)

        self.log('Found {} users'.format(str(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid, all_fields=True):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        user = self.query_single_element_from_endpoint_with_filter("user", username_or_luid, all_fields=all_fields)
        user_luid = user.get("id")
        username = user.get('name')
        self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user

    def query_user_luid(self, username):
        """
        :type username: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if username in self.username_luid_cache:
            user_luid = self.username_luid_cache[username]
        else:
            user_luid = self.query_single_element_luid_from_endpoint_with_filter("user", username,
                                                                                 optimize_with_field=True)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid

    def query_username(self, user_luid):
        """
        :type user_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        try:
            luid_index = list(self.username_luid_cache.values()).index(user_luid)
            username = list(self.username_luid_cache.keys())[luid_index]
        except ValueError as e:
            user = self.query_user(user_luid)
            username = user.get('name')

        self.end_log_block()
        return username

    def query_users_in_group(self, group_name_or_luid):
        """
        :type group_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(group_name_or_luid):
            luid = group_name_or_luid
        else:
            luid = self.query_group_luid(group_name_or_luid)
        users = self.query_resource("groups/{}/users".format(luid))
        self.end_log_block()
        return users

    #
    # End User Querying Methods
    #

    def query_server_info(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        server_info = self.query_resource("serverinfo", server_level=True)
        self.end_log_block()
        return server_info

    def query_server_version(self):
        """
        :rtype:
        """
        self.start_log_block()
        server_info = self.query_server_info()
        # grab the server number

    def query_api_version(self):
        self.start_log_block()
        server_info = self.query_server_info()
        # grab api version number

    def query_views(self, usage=False, created_at_filter=None, updated_at_filter=None, tags_filter=None, sorts=None):
        """
        :type usage: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource("views", filters=filters, sorts=sorts,
                                  additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    def query_views_json(self, usage=False, created_at_filter=None, updated_at_filter=None, tags_filter=None,
                         sorts=None, page_number=None):
        """
        :type usage: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource_json("views", filters=filters, sorts=sorts,
                                       additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()),
                                       page_number=page_number)
        self.end_log_block()
        return vws

    def query_view(self, vw_name_or_luid):
        """
        :type vw_name_or_luid:
        :rtype: etree.Element
        """
        self.start_log_block()
        vw = self.query_single_element_from_endpoint_with_filter('view', vw_name_or_luid)
        self.end_log_block()
        return vw

    def query_datasources(self, project_name_or_luid=None, updated_at_filter=None, created_at_filter=None,
                          tags_filter=None, datasource_type_filter=None, sorts=None):
        """
        :type project_name_or_luid: unicode
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type datasource_type_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource('datasources', filters=filters, sorts=sorts)
        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            dses_in_project = datasources.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            dses = etree.Element(self.ns_prefix + 'datasources')
            for ds in dses_in_project:
                dses.append(ds)
        else:
            dses = datasources

        self.end_log_block()
        return dses

    def query_datasources_json(self, updated_at_filter=None, created_at_filter=None,
                               tags_filter=None, datasource_type_filter=None, sorts=None, page_number=None):
        """
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type datasource_type_filter: UrlFilter
        :type sorts: list[Sort]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource_json('datasources', filters=filters, sorts=sorts, page_number=page_number)

        self.end_log_block()
        return datasources

    # query_datasource and query_datasource_luid can't be improved because filtering doesn't take a Project Name/LUID

    # Begin scheduler querying methods
    #

    def query_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_resource("schedules", server_level=True)
        self.end_log_block()
        return schedules

    def query_schedules_json(self, page_number=None):
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        schedules = self.query_resource_json("schedules", server_level=True, page_number=page_number)
        self.end_log_block()
        return schedules

    def query_extract_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        extract_schedules = schedules.findall('.//t:schedule[@type="Extract"]', self.ns_map)
        self.end_log_block()
        return extract_schedules

    def query_subscription_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        subscription_schedules = schedules.findall('.//t:schedule[@type="Subscription"]', self.ns_map)
        self.end_log_block()
        return subscription_schedules

    def query_schedule_luid(self, schedule_name):
        """
        :type schedule_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_luid_by_name_from_endpoint('schedule', schedule_name, server_level=True)
        self.end_log_block()
        return luid

    def query_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_from_endpoint('schedule', schedule_name_or_luid, server_level=True)
        self.end_log_block()
        return luid

    def query_extract_refresh_tasks_by_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.query_resource("schedules/{}/extracts".format(luid))
        self.end_log_block()
        return tasks

    #
    # End Scheduler Querying Methods
    #

    def get_extract_refresh_tasks(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_tasks = self.query_resource('tasks/extractRefreshes')
        self.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_task = self.query_resource('tasks/extractRefreshes/{}'.format(task_luid))
        self.start_log_block()
        return extract_task

    def get_extract_refresh_tasks_on_schedule(self, schedule_name_or_luid):
        """
        :param schedule_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        tasks_on_sched = tasks.findall('.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.ns_map)
        if len(tasks_on_sched) == 0:
            self.end_log_block()
            raise NoMatchFoundException(
                "No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.end_log_block()

    def run_extract_refresh_task(self, task_luid):
        """
        :task task_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        tsr = etree.Element('tsRequest')

        url = self.build_api_url('tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.send_add_request(url, tsr)
        self.end_log_block()
        return response.findall('.//t:job', self.ns_map)[0].get("id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    def run_extract_refresh_for_workbook(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        tasks = self.get_extract_refresh_tasks()

        extracts_for_wb = tasks.findall('.//t:extract/workbook[@id="{}"]..'.format(wb_luid), self.ns_map)

        for extract in extracts_for_wb:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    # Check if this actually works
    def run_extract_refresh_for_datasource(self, ds_name_or_luid, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        print(tasks)
        extracts_for_ds = tasks.findall('.//t:extract/datasource[@id="{}"]..'.format(ds_luid), self.ns_map)
        # print extracts_for_wb
        for extract in extracts_for_ds:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    # Tags can be scalar string or list
    def add_tags_to_datasource(self, ds_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type tag_s: list[unicode]
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)
        url = self.build_api_url("datasources/{}/tags".format(ds_luid))

        tsr = etree.Element("tsRequest")
        ts = etree.Element("tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def delete_tags_from_datasource(self, ds_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type tag_s: list[unicode] or unicode
        :rtype: int
        """
        self.start_log_block()
        tags = self.to_list(tag_s)
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.build_api_url("datasources/{}/tags/{}".format(ds_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count

    # Tags can be scalar string or list
    def add_tags_to_view(self, view_name_or_luid, workbook_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :type tag_s: list[unicode]
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()

        if self.is_luid(view_name_or_luid):
            vw_luid = view_name_or_luid
        else:
            vw_luid = self.query_workbook_view_luid(workbook_name_or_luid, view_name_or_luid, proj_name_or_luid)
        url = self.build_api_url("views/{}/tags".format(vw_luid))

        tsr = etree.Element("tsRequest")
        ts = etree.Element("tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def delete_tags_from_view(self, view_name_or_luid, workbook_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :type tag_s: list[unicode] or unicode
        :type proj_name_or_luid: unicode
        :rtype: int
        """
        self.start_log_block()
        tags = self.to_list(tag_s)
        if self.is_luid(view_name_or_luid):
            vw_luid = view_name_or_luid
        else:
            vw_luid = self.query_workbook_view_luid(view_name_or_luid, workbook_name_or_luid, proj_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.build_api_url("views/{}/tags/{}".format(vw_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count


    # Generic implementation of all the CSV/PDF/PNG requests
    def _query_data_file(self, download_type, view_name_or_luid, high_resolution=None, view_filter_map=None,
                         wb_name_or_luid=None, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type high_resolution: bool
        :type view_filter_map: dict
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)

        if view_filter_map is not None:
            final_filter_map = {}
            for key in view_filter_map:
                new_key = "vf_{}".format(key)
                # Check if this just a string
                if isinstance(view_filter_map[key], str):
                    value = view_filter_map[key]
                else:
                    value = ",".join(map(str, view_filter_map[key]))
                final_filter_map[new_key] = value

            additional_url_params = "?" + urllib.parse.urlencode(final_filter_map)
            if high_resolution is True:
                additional_url_params += "&resolution=high"

        else:
            additional_url_params = ""
            if high_resolution is True:
                additional_url_params += "?resolution=high"
        try:

            url = self.build_api_url("views/{}/{}{}".format(view_luid, download_type, additional_url_params))
            binary_result = self.send_binary_get_request(url)

            self.end_log_block()
            return binary_result
        except RecoverableHTTPException as e:
            self.log("Attempt to request results in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                           e.tableau_error_code))
            self.end_log_block()
            raise

    def query_view_image(self, view_name_or_luid, high_resolution=False, view_filter_map=None,
                         wb_name_or_luid=None, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type high_resolution: bool
        :type view_filter_map: dict
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid
        :rtype:
        """
        self.start_log_block()
        image = self._query_data_file('image', view_name_or_luid=view_name_or_luid, high_resolution=high_resolution,
                                      view_filter_map=view_filter_map, wb_name_or_luid=wb_name_or_luid,
                                      proj_name_or_luid=proj_name_or_luid)
        self.end_log_block()
        return image

    def save_view_image(self, wb_name_or_luid=None, view_name_or_luid=None, filename_no_extension=None,
                        proj_name_or_luid=None, view_filter_map=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type view_filter_map: dict
        :rtype:
        """
        self.start_log_block()
        data = self.query_view_image(wb_name_or_luid=wb_name_or_luid, view_name_or_luid=view_name_or_luid,
                                     proj_name_or_luid=proj_name_or_luid, view_filter_map=view_filter_map)

        if filename_no_extension is not None:
            if filename_no_extension.find('.png') == -1:
                filename_no_extension += '.png'
            try:
                save_file = open(filename_no_extension, 'wb')
                save_file.write(data)
                save_file.close()
                self.end_log_block()
                return
            except IOError:
                self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
                self.end_log_block()
                raise
        else:
            raise InvalidOptionException(
                'This method is for saving response to file. Must include filename_no_extension parameter')

    #
    # Start Workbook Querying Methods
    #

    # Filtering implemented for workbooks in 2.2
    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid=None, project_name_or_luid=None, all_fields=True, created_at_filter=None, updated_at_filter=None,
                        owner_name_filter=None, tags_filter=None, sorts=None, fields=None):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource("users/{}/workbooks".format(user_luid))
        else:
            wbs = self.query_resource("workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields)

        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            wbs_in_project = wbs.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            wbs = etree.Element(self.ns_prefix + 'workbooks')
            for wb in wbs_in_project:
                wbs.append(wb)
        self.end_log_block()
        return wbs

    def query_workbooks_for_user(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        wbs = self.query_workbooks(username_or_luid)
        self.end_log_block()
        return wbs

    def query_workbooks_json(self, username_or_luid=None, project_name_or_luid=None, all_fields=True,
                             created_at_filter=None, updated_at_filter=None, owner_name_filter=None,
                             tags_filter=None, sorts=None, fields=None, page_number=None):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource_json("users/{}/workbooks".format(user_luid), sorts=sorts, filters=filters,
                                           fields=fields, page_number=page_number)
        else:
            wbs = self.query_resource_json("workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields,
                                           page_number=page_number)

        self.end_log_block()
        return wbs

    # Because a workbook can have the same pretty name in two projects, requires more logic
    def query_workbook(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        workbooks = self.query_workbooks(username_or_luid)
        if self.is_luid(wb_name_or_luid):
            workbooks_with_name = self.query_resource("workbooks/{}".format(wb_name_or_luid))
        else:
            workbooks_with_name = workbooks.findall('.//t:workbook[@name="{}"]'.format(wb_name_or_luid), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException("No workbook found for username '{}' named {}".format(username_or_luid, wb_name_or_luid))
        elif proj_name_or_luid is None:
            if len(workbooks_with_name) == 1:
                wb_luid = workbooks_with_name[0].get("id")
                wb = self.query_resource("workbooks/{}".format(wb_luid))
                self.end_log_block()
                return wb
            else:
                self.end_log_block()
                raise MultipleMatchesFoundException('More than one workbook found by name {} without a project specified').format(wb_name_or_luid)
        else:
            if self.is_luid(proj_name_or_luid):
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name_or_luid, proj_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException('No workbook found with name {} in project {}'.format(wb_name_or_luid, proj_name_or_luid))
            wb_luid = wb_in_proj[0].get("id")
            wb = self.query_resource("workbooks/{}".format(wb_luid))
            self.end_log_block()
            return wb

    def query_workbook_luid(self, wb_name, proj_name_or_luid=None, username_or_luid=None):
        """
        :type username_or_luid: unicode
        :type wb_name: unicode
        :type proj_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if username_or_luid is None:
            username_or_luid = self.user_luid
        workbooks = self.query_workbooks(username_or_luid)
        workbooks_with_name = workbooks.findall('.//t:workbook[@name="{}"]'.format(wb_name), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException("No workbook found for username '{}' named {}".format(username_or_luid, wb_name))
        elif len(workbooks_with_name) == 1:
            wb_luid = workbooks_with_name[0].get("id")
            self.end_log_block()
            return wb_luid
        elif len(workbooks_with_name) > 1 and proj_name_or_luid is not None:
            if self.is_luid(proj_name_or_luid):
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@id="{}"]/..'.format(wb_name, proj_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall('.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name, proj_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException('No workbook found with name {} in project {}').format(wb_name, proj_name_or_luid)
            wb_luid = wb_in_proj[0].get("id")
            self.end_log_block()
            return wb_luid
        else:
            self.end_log_block()
            raise MultipleMatchesFoundException('More than one workbook found by name {} without a project specified').format(wb_name)

    def query_workbooks_in_project(self, project_name_or_luid, username_or_luid=None):
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            project_luid = project_name_or_luid
        else:
            project_luid = self.query_project_luid(project_name_or_luid)
        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        workbooks = self.query_workbooks(user_luid)
        # This brings back the workbook itself
        wbs_in_project = workbooks.findall('.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
        wbs = etree.Element(self.ns_prefix + 'workbooks')
        for wb in wbs_in_project:
            wbs.append(wb)
        self.end_log_block()
        return wbs

    def query_workbook_views(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None, usage=False):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        vws = self.query_resource("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        self.end_log_block()
        return vws

    def query_workbook_views_json(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None, usage=False,
                                  page_number=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :type usage: bool
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        vws = self.query_resource_json("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid,
                                                                                              str(usage).lower()),
                                                                                              page_number=page_number)
        self.end_log_block()
        return vws

    def query_workbook_view(self, wb_name_or_luid, view_name_or_luid=None, view_content_url=None, proj_name_or_luid=None, username_or_luid=None,
                            usage=False):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :type view_name_or_luid: unicode
        :type view_content_url: unicode
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        vws = self.query_resource("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        if view_content_url is not None:
            views_with_name = vws.findall('.//t:view[@contentUrl="{}"]'.format(view_content_url), self.ns_map)
        elif self.is_luid(view_name_or_luid):
            views_with_name = vws.findall('.//t:view[@id="{}"]'.format(view_name_or_luid), self.ns_map)
        else:
            views_with_name = vws.findall('.//t:view[@name="{}"]'.format(view_name_or_luid), self.ns_map)
        if len(views_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException('No view found with name {} in workbook {}').format(view_name_or_luid, wb_name_or_luid)
        elif len(views_with_name) > 1:
            self.end_log_block()
            raise MultipleMatchesFoundException(
                'More than one view found by name {} in workbook {}. Use view_content_url parameter').format(view_name_or_luid, wb_name_or_luid)
        self.end_log_block()
        return views_with_name

    def query_workbook_view_luid(self, wb_name_or_luid, view_name=None, view_content_url=None, proj_name_or_luid=None,
                                 username_or_luid=None, usage=False):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :type view_name: unicode
        :type view_content_url: unicode
        :type usage: bool
        :rtype: unicode
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        vws = self.query_resource("workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        if view_content_url is not None:
            views_with_name = vws.findall('.//t:view[@contentUrl="{}"]'.format(view_content_url), self.ns_map)
        else:
            views_with_name = vws.findall('.//t:view[@name="{}"]'.format(view_name), self.ns_map)
        if len(views_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException('No view found with name {} or content_url {} in workbook {}').format(view_name, view_content_url, wb_name_or_luid)
        elif len(views_with_name) > 1:
            self.end_log_block()
            raise MultipleMatchesFoundException(
                'More than one view found by name {} in workbook {}. Use view_content_url parameter').format(view_name, view_content_url, wb_name_or_luid)
        view_luid = views_with_name[0].get('id')
        self.end_log_block()
        return view_luid

    # This should be the key to updating the connections in a workbook. Seems to return
    # LUIDs for connections and the datatypes, but no way to distinguish them
    def query_workbook_connections(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        conns = self.query_resource("workbooks/{}/connections".format(wb_luid))
        self.end_log_block()
        return conns

    def query_views(self, all_fields=True, usage=False, created_at_filter=None, updated_at_filter=None,
                    tags_filter=None, sorts=None, fields=None):
        """
        :type usage: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()

        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource("views", filters=filters, sorts=sorts, fields=fields,
                                  additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    def query_views_json(self, all_fields=True, usage=False, created_at_filter=None, updated_at_filter=None,
                    tags_filter=None, sorts=None, fields=None, page_number=None):
        """
        :type usage: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()

        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource_json("views", filters=filters, sorts=sorts, fields=fields,
                                  additional_url_ending="includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    # Checks status of AD sync process
    def query_job(self, job_luid):
        """
        :type job_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        job = self.query_resource("jobs/{}".format(job_luid))
        self.end_log_block()
        return job

    #
    # End Workbook Query Methods
    #

    #

    #
    # Start of download / save methods
    #

    # You must pass in the wb name because the endpoint needs it (although, you could potentially look up the
    # workbook LUID from the view LUID
    def query_view_preview_image(self, wb_name_or_luid, view_name_or_luid,
                                         proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :rtype: bytes
       """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)

        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)
        try:

            url = self.build_api_url("workbooks/{}/views/{}/previewImage".format(wb_luid, view_luid))
            image = self.send_binary_get_request(url)

            self.end_log_block()
            return image

        # You might be requesting something that doesn't exist
        except RecoverableHTTPException as e:
            self.log("Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                                          e.tableau_error_code))
            self.end_log_block()
            raise


    # Do not include file extension

    # Just an alias but it matches the naming of the current reference guide (2019.1)
    def save_view_preview_image(self, wb_name_or_luid, view_name_or_luid, filename_no_extension,
                                         proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :rtype:
        """
        self.save_workbook_view_preview_image(wb_name_or_luid, view_name_or_luid, filename_no_extension,
                                         proj_name_or_luid)

    def save_workbook_view_preview_image(self, wb_name_or_luid, view_name_or_luid, filename_no_extension,
                                         proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type view_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type filename_no_extension: unicode
        :rtype:
        """
        self.start_log_block()
        image = self.query_view_preview_image(wb_name_or_luid=wb_name_or_luid, view_name_or_luid=view_name_or_luid,
                                              proj_name_or_luid=proj_name_or_luid)
        if filename_no_extension.find('.png') == -1:
            filename_no_extension += '.png'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(image)
            save_file.close()
            self.end_log_block()

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.end_log_block()
            raise

    def query_workbook_preview_image(self, wb_name_or_luid, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :rtype: bytes
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:

            url = self.build_api_url("workbooks/{}/previewImage".format(wb_luid))
            image = self.send_binary_get_request(url)
            self.end_log_block()
            return image

        # You might be requesting something that doesn't exist, but unlikely
        except RecoverableHTTPException as e:
            self.log("Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise


    # Do not include file extension
    def save_workbook_preview_image(self, wb_name_or_luid, filename_no_extension, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :param filename_no_extension: Correct extension will be added automatically
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        image = self.query_workbook_preview_image(wb_name_or_luid=wb_name_or_luid, proj_name_or_luid=proj_name_or_luid)
        if filename_no_extension.find('.png') == -1:
            filename_no_extension += '.png'
        try:
            save_file = open(filename_no_extension, 'wb')
            save_file.write(image)
            save_file.close()
            self.end_log_block()

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.end_log_block()
            raise

    # Do not include file extension. Without filename, only returns the response
    # Do not include file extension. Without filename, only returns the response
    def download_datasource(self, ds_name_or_luid, filename_no_extension, proj_name_or_luid=None,
                            include_extract=True):
        """"
        :type ds_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :type include_extract: bool
        :return Filename of the saved file
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, project_name_or_luid=proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.build_api_url("datasources/{}/content?includeExtract=False".format(ds_luid))
            else:
                url = self.build_api_url("datasources/{}/content".format(ds_luid))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.tds'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tdsx'
            self.log('Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:

            save_filename = filename_no_extension + extension
            save_file = open(save_filename, 'wb')
            save_file.write(ds)
            save_file.close()

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_workbook(self, wb_name_or_luid, filename_no_extension, proj_name_or_luid=None, include_extract=True):
        """
        :type wb_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :type include_extract: bool
        :return Filename of the save workbook
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.build_api_url("workbooks/{}/content?includeExtract=False".format(wb_luid))
            else:
                url = self.build_api_url("workbooks/{}/content".format(wb_luid))
            wb = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.twb'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.twbx'
            if extension is None:
                raise IOError('File extension could not be determined')
            self.log(
                'Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
        except RecoverableHTTPException as e:
            self.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:

            save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(wb)
            save_file.close()

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    #
    # End download / save methods
    #

    #
    # Create / Add Methods
    #

    def add_user_by_username(self, username=None, site_role='Unlicensed', auth_setting=None, update_if_exists=False,
                             direct_xml_request=None):
        """
        :type username: unicode
        :type site_role: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        # Check to make sure role that is passed is a valid role in the API
        if site_role not in self.site_roles:
            raise InvalidOptionException("{} is not a valid site role in Tableau Server".format(site_role))

        if auth_setting is not None:
            if auth_setting not in ['SAML', 'ServerDefault']:
                raise InvalidOptionException('auth_setting must be either "SAML" or "ServerDefault"')
        self.log("Adding {}".format(username))
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            u = etree.Element("user")
            u.set("name", username)
            u.set("siteRole", site_role)
            if auth_setting is not None:
                u.set('authSetting', auth_setting)
            tsr.append(u)

        url = self.build_api_url('users')
        try:
            new_user = self.send_add_request(url, tsr)
            new_user_luid = new_user.findall('.//t:user', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_user_luid
        # If already exists, update site role unless overridden.
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log("Username '{}' already exists on the server".format(username))
                if update_if_exists is True:
                    self.log('Updating {} to site role {}'.format(username, site_role))
                    self.update_user(username, site_role=site_role)
                    self.end_log_block()
                    return self.query_user_luid(username)
                else:
                    self.end_log_block()
                    raise AlreadyExistsException('Username already exists ', self.query_user_luid(username))
        except:
            self.end_log_block()
            raise

    # This is "Add User to Site", since you must be logged into a site.
    # Set "update_if_exists" to True if you want the equivalent of an 'upsert', ignoring the exceptions
    def add_user(self, username=None, fullname=None, site_role='Unlicensed', password=None, email=None,
                 auth_setting=None,
                 update_if_exists=False, direct_xml_request=None):
        """
        :type username: unicode
        :type fullname: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        try:
            # Add username first, then update with full name
            if direct_xml_request is not None:
                # Parse to second level, should be
                new_user_tsr = etree.Element('tsRequest')
                new_user_u = etree.Element('user')
                for t in direct_xml_request:
                    if t.tag != 'user':
                        raise InvalidOptionException('Must submit a tsRequest with a user element')
                    for a in t.attrib:
                        if a in ['name', 'siteRole', 'authSetting']:
                            new_user_u.set(a, t.attrib[a])
                new_user_tsr.append(new_user_u)
                new_user_luid = self.add_user_by_username(direct_xml_request=new_user_tsr)

                update_tsr = etree.Element('tsRequest')
                update_u = etree.Element('user')
                for t in direct_xml_request:
                    for a in t.attrib:
                        if a in ['fullName', 'email', 'password', 'siteRole', 'authSetting']:
                            update_u.set(a, t.attrib[a])
                update_tsr.append(update_u)
                self.update_user(username_or_luid=new_user_luid, direct_xml_request=update_tsr)

            else:
                new_user_luid = self.add_user_by_username(username, site_role=site_role,
                                                          update_if_exists=update_if_exists, auth_setting=auth_setting)
                self.update_user(new_user_luid, fullname, site_role, password, email)
            self.end_log_block()
            return new_user_luid
        except AlreadyExistsException as e:
            self.log("Username '{}' already exists on the server; no updates performed".format(username))
            self.end_log_block()
            return e.existing_luid

    # Returns the LUID of an existing group if one already exists
    def create_group(self, group_name=None, direct_xml_request=None):
        """
        :type group_name: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            g = etree.Element("group")
            g.set("name", group_name)
            tsr.append(g)

        url = self.build_api_url("groups")
        try:
            new_group = self.send_add_request(url, tsr)
            self.end_log_block()
            return new_group.findall('.//t:group', self.ns_map)[0].get("id")
        # If the name already exists, a HTTP 409 throws, so just find and return the existing LUID
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Group named {} already exists, finding and returning the LUID'.format(group_name))
                self.end_log_block()
                return self.query_group_luid(group_name)

    # Creating a synced ad group is completely different, use this method
    # The luid is only available in the Response header if bg sync. Nothing else is passed this way -- how to expose?
    def create_group_from_ad_group(self, ad_group_name, ad_domain_name, default_site_role='Unlicensed',
                                   sync_as_background=True):
        """
        :type ad_group_name: unicode
        :type ad_domain_name: unicode
        :type default_site_role: bool
        :type sync_as_background:
        :rtype: unicode
        """
        self.start_log_block()
        if default_site_role not in self._site_roles:
            raise InvalidOptionException('"{}" is not an acceptable site role'.format(default_site_role))

        tsr = etree.Element("tsRequest")
        g = etree.Element("group")
        g.set("name", ad_group_name)
        i = etree.Element("import")
        i.set("source", "ActiveDirectory")
        i.set("domainName", ad_domain_name)
        i.set("siteRole", default_site_role)
        g.append(i)
        tsr.append(g)

        url = self.build_api_url("groups/?asJob={}".format(str(sync_as_background).lower()))
        self.log(url)
        response = self.send_add_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall('.//t:job', self.ns_map)
            self.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            self.end_log_block()
            group = response.findall('.//t:group', self.ns_map)
            return group[0].get('id')

    def create_project(self, project_name=None, project_desc=None, locked_permissions=True, no_return=False,
                       direct_xml_request=None):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type no_return: bool
        :type direct_xml_request: etree.Element
        :rtype: Project21
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            p = etree.Element("project")
            p.set("name", project_name)

            if project_desc is not None:
                p.set('description', project_desc)
            if locked_permissions is not False:
                p.set('contentPermissions', "LockedToProject")
            tsr.append(p)

        url = self.build_api_url("projects")
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall('.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.get_published_project_object(project_name_or_luid=project_name)

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name, new_content_url, admin_mode=None, user_quota=None, storage_quota=None,
                    disable_subscriptions=None, direct_xml_request=None):
        """
        :type new_site_name: unicode
        :type new_content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        if direct_xml_request is not None:
            add_request = direct_xml_request
        else:
            add_request = self.build_site_request_xml(new_site_name, new_content_url, admin_mode, user_quota,
                                                      storage_quota, disable_subscriptions)
        url = self.build_api_url("sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.send_add_request(url, add_request)
            return new_site.findall('.//t:site', self.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log("Site with content_url {} already exists".format(new_content_url))
                self.end_log_block()
                raise AlreadyExistsException("Site with content_url {} already exists".format(new_content_url),
                                             new_content_url)

    # Take a single user_luid string or a collection of luid_strings
    def add_users_to_group(self, username_or_luid_s, group_name_or_luid):
        """
        :type username_or_luid_s: list[unicode] or unicode
        :type group_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        group_name = ""
        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_name = group_name_or_luid
            group_luid = self.query_group_luid(group_name_or_luid)

        users = self.to_list(username_or_luid_s)
        for user in users:
            username = ""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)

            tsr = etree.Element("tsRequest")
            u = etree.Element("user")
            u.set("id", user_luid)
            tsr.append(u)

            url = self.build_api_url("groups/{}/users/".format(group_luid))
            try:
                self.log("Adding username {}, ID {} to group {}, ID {}".format(username, user_luid, group_name, group_luid))
                self.send_add_request(url, tsr)
            except RecoverableHTTPException as e:
                self.log("Recoverable HTTP exception {} with Tableau Error Code {}, skipping".format(str(e.http_code), e.tableau_error_code))
        self.end_log_block()

    # Tags can be scalar string or list
    def add_tags_to_workbook(self, wb_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type tag_s: list[unicode]
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        url = self.build_api_url("workbooks/{}/tags".format(wb_luid))

        tsr = etree.Element("tsRequest")
        ts = etree.Element("tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element("tag")
            t.set("label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def add_workbook_to_user_favorites(self, favorite_name, wb_name_or_luid, username_or_luid, proj_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type wb_name_or_luid: unicode
        :type username_or_luid: unicode
        :type proj_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)

        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        tsr = etree.Element('tsRequest')
        f = etree.Element('favorite')
        f.set('label', favorite_name)
        w = etree.Element('workbook')
        w.set('id', wb_luid)
        f.append(w)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def add_view_to_user_favorites(self, favorite_name, username_or_luid, view_name_or_luid=None, view_content_url=None,
                                   wb_name_or_luid=None, proj_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type username_or_luid: unicode
        :type view_name_or_luid: unicode
        :type view_content_url: unicode
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            if wb_name_or_luid is None:
                raise InvalidOptionException('When passing a View Name instead of LUID, must also specify workbook name or luid')
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name_or_luid, view_content_url,
                                                      proj_name_or_luid, username_or_luid)
            self.log('View luid found {}'.format(view_luid))

        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        tsr = etree.Element('tsRequest')
        f = etree.Element('favorite')
        f.set('label', favorite_name)
        v = etree.Element('view')
        v.set('id', view_luid)
        f.append(v)
        tsr.append(f)

        url = self.build_api_url("favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    #
    # End Add methods
    #

    def query_user_favorites(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource("favorites/{}/".format(user_luid))

        self.end_log_block()
        return favorites

    def query_user_favorites_json(self, username_or_luid, page_number=None):
        """
        :type username_or_luid: unicode
        :rtype: json
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        favorites = self.query_resource_json("favorites/{}/".format(user_luid), page_number=page_number)

        self.end_log_block()
        return favorites

    #
    # Start Update Methods
    #

    def update_user(self, username_or_luid, full_name=None, site_role=None, password=None,
                    email=None, direct_xml_request=None):
        """
        :type username_or_luid: unicode
        :type full_name: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :type direct_xml_request: etree.Element
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            u = etree.Element("user")
            if full_name is not None:
                u.set('fullName', full_name)
            if site_role is not None:
                u.set('siteRole', site_role)
            if email is not None:
                u.set('email', email)
            if password is not None:
                u.set('password', password)
            tsr.append(u)

        url = self.build_api_url("users/{}".format(user_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def update_datasource(self, datasource_name_or_luid, datasource_project_name_or_luid=None,
                          new_datasource_name=None, new_project_luid=None, new_owner_luid=None):
        """
        :type datasource_name_or_luid: unicode
        :type datasource_project_name_or_luid: unicode
        :type new_datasource_name: unicode
        :type new_project_luid: unicode
        :type new_owner_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(datasource_name_or_luid):
            datasource_luid = datasource_name_or_luid
        else:
            datasource_luid = self.query_datasource_luid(datasource_name_or_luid, datasource_project_name_or_luid)

        tsr = etree.Element("tsRequest")
        d = etree.Element("datasource")
        if new_datasource_name is not None:
            d.set('name', new_datasource_name)
        if new_project_luid is not None:
            p = etree.Element('project')
            p.set('id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = etree.Element('owner')
            o.set('id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url("datasources/{}".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def update_datasource_connection_by_luid(self, datasource_luid, new_server_address=None, new_server_port=None,
                                             new_connection_username=None, new_connection_password=None):
        """
        :type datasource_luid: unicode
        :type new_server_address: unicode
        :type new_server_port: unicode
        :type new_connection_username: unicode
        :type new_connection_password: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.__build_connection_update_xml(new_server_address, new_server_port,
                                                            new_connection_username,
                                                            new_connection_password)
        url = self.build_api_url("datasources/{}/connection".format(datasource_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # Local Authentication update group
    def update_group(self, name_or_luid, new_group_name):
        """
        :type name_or_luid: unicode
        :type new_group_name: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            group_luid = name_or_luid
        else:
            group_luid = self.query_group_luid(name_or_luid)

        tsr = etree.Element("tsRequest")
        g = etree.Element("group")
        g.set("name", new_group_name)
        tsr.append(g)

        url = self.build_api_url("groups/{}".format(group_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # AD group sync. Must specify the domain and the default site role for imported users
    def sync_ad_group(self, group_name_or_luid, ad_group_name, ad_domain, default_site_role, sync_as_background=True):
        """
        :type group_name_or_luid: unicode
        :type ad_group_name: unicode
        :type ad_domain: unicode
        :type default_site_role: unicode
        :type sync_as_background: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if sync_as_background not in [True, False]:
            error = "'{}' passed for sync_as_background. Use True or False".format(str(sync_as_background).lower())
            raise InvalidOptionException(error)

        if default_site_role not in self._site_roles:
            raise InvalidOptionException("'{}' is not a valid site role in Tableau".format(default_site_role))
        # Check that the group exists
        self.query_group(group_name_or_luid)
        tsr = etree.Element('tsRequest')
        g = etree.Element('group')
        g.set('name', ad_group_name)
        i = etree.Element('import')
        i.set('source', 'ActiveDirectory')
        i.set('domainName', ad_domain)
        i.set('siteRole', default_site_role)
        g.append(i)
        tsr.append(g)

        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_luid = self.query_group_luid(group_name_or_luid)
        url = self.build_api_url(
            "groups/{}".format(group_luid) + "?asJob={}".format(str(sync_as_background)).lower())
        response = self.send_update_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall('.//t:job', self.ns_map)
            self.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            group = response.findall('.//t:group', self.ns_map)
            self.end_log_block()
            return group[0].get('id')

    # Simplest method
    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None,
                       locked_permissions=None, publish_samples=False):
        """
        :type name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :rtype: Project21
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            project_luid = name_or_luid
        else:
            project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element("tsRequest")
        p = etree.Element("project")
        if new_project_name is not None:
            p.set('name', new_project_name)
        if new_project_description is not None:
            p.set('description', new_project_description)
        if locked_permissions is True:
            p.set('contentPermissions', "LockedToProject")
        elif locked_permissions is False:
            p.set('contentPermissions', "ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url("projects/{}".format(project_luid))
        if publish_samples is True:
            url += '?publishSamples=true'

        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    # Can only update the site you are signed into, so take site_luid from the object
    def update_site(self, site_name=None, content_url=None, admin_mode=None, user_quota=None,
                    storage_quota=None, disable_subscriptions=None, state=None, revision_history_enabled=None,
                    revision_limit=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :type revision_history_enabled: bool
        :type revision_limit: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.build_site_request_xml(site_name, content_url, admin_mode, user_quota, storage_quota,
                                          disable_subscriptions, state)
        url = self.build_api_url("")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def update_workbook(self, workbook_name_or_luid, workbook_project_name_or_luid, new_project_luid=None,
                        new_owner_luid=None, show_tabs=True):
        """
        :type workbook_name_or_luid: unicode
        :type workbook_project_name_or_luid: unicode
        :type new_project_luid: unicode
        :type new_owner_luid: unicode
        :type show_tabs: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(workbook_name_or_luid):
            workbook_luid = workbook_name_or_luid
        else:
            workbook_luid = self.query_workbook_luid(workbook_name_or_luid, workbook_project_name_or_luid,
                                                     self.username)
        tsr = etree.Element("tsRequest")
        w = etree.Element("workbook")
        w.set('showTabs', str(show_tabs).lower())
        if new_project_luid is not None:
            p = etree.Element('project')
            p.set('id', new_project_luid)
            w.append(p)

        if new_owner_luid is not None:
            o = etree.Element('owner')
            o.set('id', new_owner_luid)
            w.append(o)
        tsr.append(w)

        url = self.build_api_url("workbooks/{}".format(workbook_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    # To do this, you need the workbook's connection_luid. Seems to only come from "Query Workbook Connections",
    # which does not return any names, just types and LUIDs
    def update_workbook_connection_by_luid(self, wb_luid, connection_luid, new_server_address=None,
                                           new_server_port=None,
                                           new_connection_username=None, new_connection_password=None):
        """
        :type wb_luid: unicode
        :type connection_luid: unicode
        :type new_server_address: unicode
        :type new_server_port: unicode
        :type new_connection_username: unicode
        :type new_connection_password: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.__build_connection_update_xml(new_server_address, new_server_port, new_connection_username,
                                                 new_connection_password)
        url = self.build_api_url("workbooks/{}/connections/{}".format(wb_luid, connection_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    #
    # Start Delete methods
    #

    # Can take collection or luid_string
    def delete_datasources(self, datasource_name_or_luid_s):
        """
        :type datasource_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        datasources = self.to_list(datasource_name_or_luid_s)
        for datasource_name_or_luid in datasources:
            if self.is_luid(datasource_name_or_luid):
                datasource_luid = datasource_name_or_luid
            else:
                datasource_luid = self.query_datasource_luid(datasource_name_or_luid, None)

            url = self.build_api_url("datasources/{}".format(datasource_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_projects(self, project_name_or_luid_s):
        """
        :type project_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        projects = self.to_list(project_name_or_luid_s)
        for project_name_or_luid in projects:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            url = self.build_api_url("projects/{}".format(project_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_groups(self, group_name_or_luid_s):
        """
        :type group_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        groups = self.to_list(group_name_or_luid_s)
        for group_name_or_luid in groups:
            if group_name_or_luid == 'All Users':
                self.log('Cannot delete All Users group, skipping')
                continue
            if self.is_luid(group_name_or_luid):
                group_luid = group_name_or_luid
            else:
                group_luid = self.query_group_luid(group_name_or_luid)
            url = self.build_api_url("groups/{}".format(group_luid))
            self.send_delete_request(url)
        self.end_log_block()

    # Can only delete a site that you have signed into
    def delete_current_site(self):
        """
        :rtype:
        """
        self.start_log_block()
        url = self.build_api_url("sites/{}".format(self.site_luid), server_level=True)
        self.send_delete_request(url)
        self.end_log_block()

    # Can take collection or luid_string
    def delete_workbooks(self, wb_name_or_luid_s):
        """
        :type wb_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        wbs = self.to_list(wb_name_or_luid_s)
        for wb in wbs:
            # Check if workbook_luid exists
            if self.is_luid(wb):
                wb_luid = wb
            else:
                wb_luid = self.query_workbook_luid(wb)
            url = self.build_api_url("workbooks/{}".format(wb_luid))
            self.send_delete_request(url)
        self.end_log_block()

    # Can take collection or luid_string
    def delete_workbooks_from_user_favorites(self, wb_name_or_luid_s, username_or_luid):
        """
        :type wb_name_or_luid_s: list[unicode] or unicode
        :type username_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        wbs = self.to_list(wb_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for wb in wbs:
            if self.is_luid(wb):
                wb_luid = wb
            else:
                wb_luid = self.query_workbook_luid(wb)
            url = self.build_api_url("favorites/{}/workbooks/{}".format(user_luid, wb_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_views_from_user_favorites(self, view_name_or_luid_s, username_or_luid, wb_name_or_luid=None):
        """
        :type view_name_or_luid_s: list[unicode] or unicode
        :type username_or_luid: unicode
        :type wb_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        views = self.to_list(view_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for view in views:
            if self.is_luid(view):
                view_luid = view
            else:
                view_luid = self.query_workbook_view_luid(wb_name_or_luid, view)
            url = self.build_api_url("favorites/{}/views/{}".format(user_luid, view_luid))
            self.send_delete_request(url)
        self.end_log_block()

    # Can take collection or string user_luid string
    def remove_users_from_group(self, username_or_luid_s, group_name_or_luid):
        """
        :type username_or_luid_s: list[unicode] or unicode
        :type group_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        group_name = ""
        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_name = group_name_or_luid
            group_luid = self.query_group_name(group_name_or_luid)
        users = self.to_list(username_or_luid_s)
        for user in users:
            username = ""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)
            url = self.build_api_url("groups/{}/users/{}".format(group_luid, user_luid))
            self.log('Removing user {}, id {} from group {}, id {}'.format(username, user_luid, group_name, group_luid))
            self.send_delete_request(url)
        self.end_log_block()

    # Can take collection or single user_luid string
    def remove_users_from_site(self, username_or_luid_s):
        """
        :type username_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        users = self.to_list(username_or_luid_s)
        for user in users:
            username = ""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)
            url = self.build_api_url("users/{}".format(user_luid))
            self.log('Removing user {}, id {} from site'.format(username, user_luid))
            self.send_delete_request(url)
        self.end_log_block()

    def delete_tags_from_workbook(self, wb_name_or_luid, tag_s):
        """
        :type wb_name_or_luid: unicode
        :type tag_s: list[unicode] or unicode
        :rtype: int
        """
        self.start_log_block()
        tags = self.to_list(tag_s)
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.build_api_url("workbooks/{}/tags/{}".format(wb_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count

    #
    # End Delete Methods
    #

    #
    # Begin Subscription Methods
    #

    def query_subscriptions(self, username_or_luid=None, schedule_name_or_luid=None, subscription_subject=None,
                            view_or_workbook=None, content_name_or_luid=None, project_name_or_luid=None, wb_name_or_luid=None):
        """
        :type username_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type subscription_subject: unicode
        :type view_or_workbook: unicode
        :type content_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :type wb_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        subscriptions = self.query_resource('subscriptions')
        filters_dict = {}
        if subscription_subject is not None:
            filters_dict['subject'] = '[@subject="{}"]'.format(subscription_subject)
        if schedule_name_or_luid is not None:
            if self.is_luid(schedule_name_or_luid):
                filters_dict['sched'] = 'schedule[@id="{}"'.format(schedule_name_or_luid)
            else:
                filters_dict['sched'] = 'schedule[@user="{}"'.format(schedule_name_or_luid)
        if username_or_luid is not None:
            if self.is_luid(username_or_luid):
                filters_dict['user'] = 'user[@id="{}"]'.format(username_or_luid)
            else:
                filters_dict['user'] = 'user[@name="{}"]'.format(username_or_luid)
        if view_or_workbook is not None:
            if view_or_workbook not in ['View', 'Workbook']:
                raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")
            # Does this search make sense my itself?

        if content_name_or_luid is not None:
            if self.is_luid(content_name_or_luid):
                filters_dict['content_luid'] = 'content[@id="{}"'.format(content_name_or_luid)
            else:
                if view_or_workbook is None:
                    raise InvalidOptionException('view_or_workbook must be specified for content: "Workook" or "View"')
                if view_or_workbook == 'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException('Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 proj_name_or_luid=project_name_or_luid)
                elif view_or_workbook == 'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid)
                filters_dict['content_luid'] = 'content[@id="{}"'.format(content_luid)

        if 'subject' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription{}'.format(filters_dict['subject']))
        if 'user' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['user']))
        if 'sched' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['sched']))
        if 'content_luid' in filters_dict:
            subscriptions = subscriptions.findall('.//t:subscription/{}/..'.format(filters_dict['content_luid']))
        self.end_log_block()
        return subscriptions

    def create_subscription(self, subscription_subject=None, view_or_workbook=None, content_name_or_luid=None,
                            schedule_name_or_luid=None, username_or_luid=None, project_name_or_luid=None,
                            wb_name_or_luid=None, direct_xml_request=None):
        """
        :type subscription_subject: unicode
        :type view_or_workbook: unicode
        :type content_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type username_or_luid: unicode
        :type project_name_or_luid: unicode
        :type wb_name_or_luid: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            if view_or_workbook not in ['View', 'Workbook']:
                raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            if self.is_luid(username_or_luid):
                user_luid = username_or_luid
            else:
                user_luid = self.query_user_luid(username_or_luid)

            if self.is_luid(schedule_name_or_luid):
                schedule_luid = schedule_name_or_luid
            else:
                schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

            if self.is_luid(content_name_or_luid):
                content_luid = content_name_or_luid
            else:
                if view_or_workbook == 'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException('Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 proj_name_or_luid=project_name_or_luid, username_or_luid=user_luid)
                elif view_or_workbook == 'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid, user_luid)
                else:
                    raise InvalidOptionException("view_or_workbook must be 'Workbook' or 'View'")

            tsr = etree.Element('tsRequest')
            s = etree.Element('subscription')
            s.set('subject', subscription_subject)
            c = etree.Element('content')
            c.set('type', view_or_workbook)
            c.set('id', content_luid)
            sch = etree.Element('schedule')
            sch.set('id', schedule_luid)
            u = etree.Element('user')
            u.set('id', user_luid)
            s.append(c)
            s.append(sch)
            s.append(u)
            tsr.append(s)

        url = self.build_api_url('subscriptions')
        try:
            new_subscription = self.send_add_request(url, tsr)
            new_subscription_luid = new_subscription.findall('.//t:subscription', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_subscription_luid
        except RecoverableHTTPException as e:
            self.end_log_block()
            raise e

    def create_subscription_to_workbook(self, subscription_subject, wb_name_or_luid, schedule_name_or_luid,
                                        username_or_luid, project_name_or_luid=None):
        """
        :type subscription_subject: unicode
        :type wb_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type username_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.create_subscription(subscription_subject, 'Workbook', wb_name_or_luid, schedule_name_or_luid,
                                        username_or_luid, project_name_or_luid=project_name_or_luid)
        self.end_log_block()
        return luid

    def create_subscription_to_view(self, subscription_subject, view_name_or_luid, schedule_name_or_luid,
                                    username_or_luid, wb_name_or_luid=None, project_name_or_luid=None):
        """
        :type subscription_subject: unicode
        :type view_name_or_luid: unicode
        :type schedule_name_or_luid:
        :type username_or_luid: unicode
        :type wb_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.create_subscription(subscription_subject, 'View', view_name_or_luid, schedule_name_or_luid,
                                        username_or_luid, wb_name_or_luid=wb_name_or_luid, project_name_or_luid=project_name_or_luid)
        self.end_log_block()
        return luid

    def update_subscription(self, subscription_luid, subject=None, schedule_luid=None):
        if subject is None and schedule_luid is None:
            raise InvalidOptionException("You must pass one of subject or schedule_luid, or both")
        request = '<tsRequest>'
        request += '<subscripotion '
        if subject is not None:
            request += 'subject="{}" '.format(subject)
        request += '>'
        if schedule_luid is not None:
            request += '<schedule id="{}" />'.format(schedule_luid)
        request += '</tsRequest>'

        url = self.build_api_url("subscriptions/{}".format(subscription_luid))
        response = self.send_update_request(url, request)
        self.end_log_block()
        return response

    def delete_subscriptions(self, subscription_luid_s):
        """
        :param subscription_luid_s:
        :rtype:
        """
        self.start_log_block()
        subscription_luids = self.to_list(subscription_luid_s)
        for subscription_luid in subscription_luids:
            url = self.build_api_url("subscriptions/{}".format(subscription_luid))
            self.send_delete_request(url)
        self.end_log_block()

    #
    # End Subscription Methods
    #

    #
    # Begin Schedule Methods
    #

    def create_schedule(self, name=None, extract_or_subscription=None, frequency=None, parallel_or_serial=None,
                        priority=None, start_time=None,end_time=None, interval_value_s=None,
                        interval_hours_minutes=None, direct_xml_request=None):
        """
        :type name: unicode
        :type extract_or_subscription: unicode
        :type frequency: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :type start_time: unicode
        :type end_time: unicode
        :type interval_value_s: unicode or list[unicode]
        :type interval_hours_minutes: unicode
        :type direct_xml_request: etree.Element
        :rtype:
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            if extract_or_subscription not in ['Extract', 'Subscription']:
                raise InvalidOptionException("extract_or_subscription can only be 'Extract' or 'Subscription'")
            if priority < 1 or priority > 100:
                raise InvalidOptionException("priority must be an integer between 1 and 100")
            if parallel_or_serial not in ['Parallel', 'Serial']:
                raise InvalidOptionException("parallel_or_serial must be 'Parallel' or 'Serial'")
            if frequency not in ['Hourly', 'Daily', 'Weekly', 'Monthly']:
                raise InvalidOptionException("frequency must be 'Hourly', 'Daily', 'Weekly' or 'Monthly'")
            tsr = etree.Element('tsRequest')
            s = etree.Element('schedule')
            s.set('name', name)
            s.set('priority', str(priority))
            s.set('type', extract_or_subscription)
            s.set('frequency', frequency)
            s.set('executionOrder', parallel_or_serial)
            fd = etree.Element('frequencyDetails')
            fd.set('start', start_time)
            if end_time is not None:
                fd.set('end', end_time)
            intervals = etree.Element('intervals')

            # Daily does not need an interval value

            if interval_value_s is not None:
                ivs = self.to_list(interval_value_s)
                for i in ivs:
                    interval = etree.Element('interval')
                    if frequency == 'Hourly':
                        if interval_hours_minutes is None:
                            raise InvalidOptionException('Hourly must set interval_hours_minutes to "hours" or "minutes"')
                        interval.set(interval_hours_minutes, i)
                    if frequency == 'Weekly':
                        interval.set('weekDay', i)
                    if frequency == 'Monthly':
                        interval.set('monthDay', i)
                    intervals.append(interval)

            fd.append(intervals)
            s.append(fd)
            tsr.append(s)

        # Schedule requests happen at the server rather than site level, like a login
        url = self.build_api_url("schedules", server_level=True)
        try:
            new_schedule = self.send_add_request(url, tsr)
            new_schedule_luid = new_schedule.findall('.//t:schedule', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_schedule_luid
        except RecoverableHTTPException as e:
            if e.tableau_error_code == '409021':
                raise AlreadyExistsException('Schedule Already exists on the server', None)

    def update_schedule(self, schedule_name_or_luid, new_name=None, frequency=None, parallel_or_serial=None,
                        priority=None, start_time=None, end_time=None, interval_value_s=None,
                        interval_hours_minutes=None, direct_xml_request=None):
        """
        :type schedule_name_or_luid: unicode
        :type new_name: unicode
        :type frequency: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :type start_time: unicode
        :type end_time: unicode
        :type interval_value_s: unicode or list[unicode]
        :type interval_hours_minutes: unicode
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element('tsRequest')
            s = etree.Element('schedule')
            if new_name is not None:
                s.set('name', new_name)
            if priority is not None:
                if priority < 1 or priority > 100:
                    raise InvalidOptionException("priority must be an integer between 1 and 100")
                s.set('priority', str(priority))
            if frequency is not None:
                s.set('frequency', frequency)
            if parallel_or_serial is not None:
                if parallel_or_serial not in ['Parallel', 'Serial']:
                    raise InvalidOptionException("parallel_or_serial must be 'Parallel' or 'Serial'")
                s.set('executionOrder', parallel_or_serial)
            if frequency is not None:
                if frequency not in ['Hourly', 'Daily', 'Weekly', 'Monthly']:
                    raise InvalidOptionException("frequency must be 'Hourly', 'Daily', 'Weekly' or 'Monthly'")
                fd = etree.Element('frequencyDetails')
                fd.set('start', start_time)
                if end_time is not None:
                    fd.set('end', end_time)
                intervals = etree.Element('intervals')

                # Daily does not need an interval value

                if interval_value_s is not None:
                    ivs = self.to_list(interval_value_s)
                    for i in ivs:
                        interval = etree.Element('interval')
                        if frequency == 'Hourly':
                            if interval_hours_minutes is None:
                                raise InvalidOptionException('Hourly must set interval_hours_minutes to "hours" or "minutes"')
                            interval.set(interval_hours_minutes, i)
                        if frequency == 'Weekly':
                            interval.set('weekDay', i)
                        if frequency == 'Monthly':
                            interval.set('monthDay', i)
                        intervals.append(interval)

                fd.append(intervals)
                s.append(fd)
            tsr.append(s)

        # Schedule requests happen at the server rather than site level, like a login
        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def disable_schedule(self, schedule_name_or_luid):
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        s = etree.Element('schedule')
        s.set('state', 'Suspended')
        tsr.append(s)

        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def enable_schedule(self, schedule_name_or_luid):
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        s = etree.Element('schedule')
        s.set('state', 'Active')
        tsr.append(s)

        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def create_daily_extract_schedule(self, name, start_time, priority=1, parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: In format HH:MM:SS , like 18:30:00
        :type start_time: unicode
        :rtype: unicode
        """
        self.start_log_block()
        # Check the time format at some point

        luid = self.create_schedule(name, 'Extract', 'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_daily_subscription_schedule(self, name, start_time, priority=1, parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: In format HH:MM:SS , like 18:30:00
        :type start_time: unicode
        :rtype: unicode
        """
        self.start_log_block()
        # Check the time format at some point

        luid = self.create_schedule(name, 'Subscription', 'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_weekly_extract_schedule(self, name, weekday_s, start_time, priority=1, parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param weekday_s: Use 'Monday', 'Tuesday' etc.
        :type weekday_s: list[unicode] or unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Extract', 'Weekly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_weekly_subscription_schedule(self, name, weekday_s, start_time, priority=1,
                                            parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param weekday_s: Use 'Monday', 'Tuesday' etc.
        :type weekday_s: list[unicode] or unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Subscription', 'Weekly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_monthly_extract_schedule(self, name, day_of_month, start_time, priority=1,
                                        parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param day_of_month: Use '1', '2' or 'LastDay'
        :type day_of_month: unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Extract', 'Monthly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_monthly_subscription_schedule(self, name, day_of_month, start_time, priority=1,
                                             parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param day_of_month: Use '1', '2' or 'LastDay'
        :type day_of_month: unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Subscription', 'Monthly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_hourly_extract_schedule(self, name, interval_hours_or_minutes, interval, start_time, end_time,
                                       priority=1, parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: In format HH:MM:SS , like 18:30:00
        :type start_time: unicode
        :param end_time: In format HH:MM:SS , like 18:30:00
        :type end_time: unicode
        :param interval_hours_or_minutes: Either 'hours' or 'minutes'
        :type interval_hours_or_minutes: unicode
        :parame interval: This can be '1','2', '4', '6', '8', or '12' for hours or '15' or '30' for minutes
        :type interval: unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Extract', 'Hourly', parallel_or_serial, priority, start_time, end_time,
                                    interval, interval_hours_or_minutes)
        self.end_log_block()
        return luid

    def create_hourly_subscription_schedule(self, name, interval_hours_or_minutes, interval, start_time, end_time,
                                            priority=1, parallel_or_serial='Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: In format HH:MM:SS , like 18:30:00
        :type start_time: unicode
        :param end_time: In format HH:MM:SS , like 18:30:00
        :type end_time: unicode
        :param interval_hours_or_minutes: Either 'hours' or 'minutes'
        :type interval_hours_or_minutes: unicode
        :parame interval: This can be '1','2', '4', '6', '8', or '12' for hours or '15' or '30' for minutes
        :type interval: unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, 'Subscription', 'Hourly', parallel_or_serial, priority, start_time, end_time,
                                    interval, interval_hours_or_minutes)
        self.end_log_block()
        return luid

    def delete_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)
        url = self.build_api_url("schedules/{}".format(schedule_luid), server_level=True)
        self.send_delete_request(url)

        self.end_log_block()

    #
    # End Schedule Methodws
    #

    def add_datasource_to_user_favorites(self, favorite_name, ds_name_or_luid_s, username_or_luid, p_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type ds_name_or_luid_s: unicode
        :type username_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        dses = self.to_list(ds_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        for ds in dses:
            if self.is_luid(ds_name_or_luid_s):
                datasource_luid = ds
            else:
                datasource_luid = self.query_datasource_luid(ds, p_name_or_luid)

            tsr = etree.Element('tsRequest')
            f = etree.Element('favorite')
            f.set('label', favorite_name)
            d = etree.Element('datasource')
            d.set('id', datasource_luid)
            f.append(d)
            tsr.append(f)

            url = self.build_api_url("favorites/{}".format(user_luid))
            self.send_update_request(url, tsr)

        self.end_log_block()

    def delete_datasources_from_user_favorites(self, ds_name_or_luid_s, username_or_luid, p_name_or_luid=None):
        """
        :type ds_name_or_luid_s: list[unicode] or unicode
        :type username_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        dses = self.to_list(ds_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for ds in dses:
            if self.is_luid(ds):
                ds_luid = ds
            else:
                ds_luid = self.query_datasource_luid(ds, p_name_or_luid)
            url = self.build_api_url("favorites/{}/datasources/{}".format(user_luid, ds_luid))
            self.send_delete_request(url)
        self.end_log_block()

        #
        # Online Logo Updates
        #

    def update_online_site_logo(self, image_filename):
        """
        :type image_filename: unicode
        :rtype:
        """
        # Request type is mixed and require a boundary
        boundary_string = self.generate_boundary_string()
        for ending in ['.png', ]:
            if image_filename.endswith(ending):
                file_extension = ending[1:]

                # Open the file to be uploaded
                try:
                    content_file = open(image_filename, 'rb')

                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(image_filename))
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                "File {} is not PNG. Use PNG image.".format(image_filename))

        # Create the initial XML portion of the request
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: form-data; name="site_logo"; filename="new_site_logo.png"\r\n'
        publish_request += 'Content-Type: application/octet-stream\r\n\r\n'

        publish_request += "--{}".format(boundary_string)

        # Content needs to be read unencoded from the file
        content = content_file.read()

        # Add to string as regular binary, no encoding
        publish_request += content

        publish_request += "\r\n--{}--".format(boundary_string)
        url = self.build_api_url('')[:-1]
        return self.send_publish_request(url, publish_request, None, boundary_string)

    def restore_online_site_logo(self):
        """
        :rtype:
        """
        # Request type is mixed and require a boundary
        boundary_string = self.generate_boundary_string()

        # Create the initial XML portion of the request
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: form-data; name="site_logo"; filename="empty.txt"\r\n'
        publish_request += 'Content-Type: text/plain\r\n\r\n'

        publish_request += "--{}".format(boundary_string)

        url = self.build_api_url('')[:-1]
        return self.send_publish_request(url, publish_request, None, boundary_string)

        #
        # Begin Revision Methods
        #

    def get_workbook_revisions(self, workbook_name_or_luid, username_or_luid=None, project_name_or_luid=None):
        """
        :type workbook_name_or_luid: unicode
        :type username_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(workbook_name_or_luid):
            wb_luid = workbook_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(workbook_name_or_luid, project_name_or_luid, username_or_luid)
        wb_revisions = self.query_resource('workbooks/{}/revisions'.format(wb_luid))
        self.end_log_block()
        return wb_revisions

    def get_datasource_revisions(self, datasource_name_or_luid, project_name_or_luid=None):
        """
        :type datasource_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(datasource_name_or_luid):
            ds_luid = datasource_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        wb_revisions = self.query_resource('workbooks/{}/revisions'.format(ds_luid))
        self.end_log_block()
        return wb_revisions

    def remove_datasource_revision(self, datasource_name_or_luid, revision_number, project_name_or_luid=None):
        """
        :type datasource_name_or_luid: unicode
        :type revision_number: int
        :type project_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(datasource_name_or_luid):
            ds_luid = datasource_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        url = self.build_api_url("datasources/{}/revisions/{}".format(ds_luid, str(revision_number)))
        self.send_delete_request(url)
        self.end_log_block()

    def remove_workbook_revision(self, wb_name_or_luid, revision_number,
                                 project_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type revision_number: int
        :type project_name_or_luid: unicode
        :type username_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, project_name_or_luid, username_or_luid)
        url = self.build_api_url("workbooks/{}/revisions/{}".format(wb_luid, str(revision_number)))
        self.send_delete_request(url)
        self.end_log_block()

        # Do not include file extension. Without filename, only returns the response

    def download_datasource_revision(self, ds_name_or_luid, revision_number, filename_no_extension,
                                     proj_name_or_luid=None, include_extract=True):
        """
        :type ds_name_or_luid: unicode
        :type revision_number: int
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :type include_extract: bool
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        try:

            if include_extract is False:
                url = self.build_api_url("datasources/{}/revisions/{}/content?includeExtract=False".format(ds_luid,
                                                                                                            str(revision_number)))
            else:
                url = self.build_api_url(
                    "datasources/{}/revisions/{}/content".format(ds_luid, str(revision_number)))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.tds'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tdsx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                              e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_ds' + extension
            else:
                save_filename = filename_no_extension + extension
            save_file = open(save_filename, 'wb')
            save_file.write(ds)
            save_file.close()
            self.end_log_block()
            return save_filename
        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.end_log_block()
            raise

        # Do not include file extension, added automatically. Without filename, only returns the response
        # Use no_obj_return for save without opening and processing

    def download_workbook_revision(self, wb_name_or_luid, revision_number, filename_no_extension,
                                   proj_name_or_luid=None, include_extract=True):
        """
        :type wb_name_or_luid: unicode
        :type revision_number: int
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :type include_extract: bool
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if include_extract is False:
                url = self.build_api_url("workbooks/{}/revisions/{}/content?includeExtract=False".format(wb_luid,
                                                                                                          str(revision_number)))
            else:
                url = self.build_api_url("workbooks/{}/revisions/{}/content".format(wb_luid, str(revision_number)))
            wb = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.twb'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.twbx'
            if extension is None:
                raise IOError('File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
                                                                                            e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:
            if filename_no_extension is None:
                save_filename = 'temp_wb' + extension
            else:
                save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(wb)
            save_file.close()
            self.end_log_block()
            return save_filename

        except IOError:
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.end_log_block()
            raise

    #
    # End Revision Methods
    #

    #
    # Start Publish methods -- workbook, datasources, file upload
    #

    ''' Publish process can go two way:
        (1) Initiate File Upload (2) Publish workbook/datasource (less than 64MB)
        (1) Initiate File Upload (2) Append to File Upload (3) Publish workbook to commit (over 64 MB)
    '''

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False):
        """
        :type workbook_filename: unicode
        :type workbook_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type show_tabs: bool
        :param check_published_ds: Set to False to improve publish speed if you KNOW there are no published data sources
        :type check_published_ds: bool
        :type oauth_flag: bool
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content('workbook', workbook_filename, workbook_name, project_luid,
                                   {"overwrite": overwrite}, connection_username, connection_password,
                                   save_credentials, show_tabs=show_tabs, check_published_ds=check_published_ds,
                                   oauth_flag=oauth_flag)
        workbook = xml.findall('.//t:workbook', self.ns_map)
        return workbook[0].get('id')

    def publish_datasource(self, ds_filename, ds_name, project_obj, overwrite=False, connection_username=None,
                           connection_password=None, save_credentials=True, oauth_flag=False):
        """
        :type ds_filename: unicode
        :type ds_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type oauth_flag: bool
        :rtype: unicode
        """
        project_luid = project_obj.luid
        xml = self.publish_content('datasource', ds_filename, ds_name, project_luid, {"overwrite": overwrite},
                                   connection_username, connection_password, save_credentials, oauth_flag=oauth_flag)
        datasource = xml.findall('.//t:datasource', self.ns_map)
        return datasource[0].get('id')

    # Main method for publishing a workbook. Should intelligently decide to chunk up if necessary
    # If a TableauDatasource or TableauWorkbook is passed, will upload from its content
    def publish_content(self, content_type, content_filename, content_name, project_luid, url_params=None,
                        connection_username=None, connection_password=None, save_credentials=True, show_tabs=False,
                        check_published_ds=True, oauth_flag=False, generate_thumbnails_as_username_or_luid=None,
                        description=None, views_to_hide_list=None):
        # Single upload limit in MB
        single_upload_limit = 20

        # If you need a temporary copy when fixing the published datasources
        temp_wb_filename = None

        # Must be 'workbook' or 'datasource'
        if content_type not in ['workbook', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be 'workbook',  'datasource', or 'flow' ")

        file_extension = None
        final_filename = None
        cleanup_temp_file = False

        for ending in ['.twb', '.twbx', '.tde', '.tdsx', '.tds', '.tde', '.hyper', '.tfl', '.tflx']:
            if content_filename.endswith(ending):
                file_extension = ending[1:]

                # If twb or twbx, open up and check for any published data sources
                if file_extension.lower() in ['twb', 'twbx'] and check_published_ds is True:
                    self.log("Adjusting any published datasources")
                    t_file = TableauFile(content_filename, self.logger)
                    dses = t_file.tableau_document.datasources
                    for ds in dses:
                        # Set to the correct site
                        if ds.published is True:
                            self.log("Published datasource found")
                            self.log("Setting publish datasource repository to {}".format(self.site_content_url))
                            ds.published_ds_site = self.site_content_url

                    temp_wb_filename = t_file.save_new_file('temp_wb')
                    content_filename = temp_wb_filename
                    # Open the file to be uploaded
                try:
                    content_file = open(content_filename, 'rb')
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
                    publish_request_xml = etree.Element('tsRequest')
                    # could be either workbook, datasource, or flow
                    t1 = etree.Element(content_type)
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
                        cc = etree.Element('connectionCredentials')
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
                            vs = etree.Element('views')
                            for view_name in views_to_hide_list:
                                v = etree.Element('view')
                                v.set('name', view_name)
                                v.set('hidden', 'true')
                            t1.append(vs)

                    # Description only allowed for Flows as of 3.3
                    if description is not None:
                         t1.set('description', description)
                    p = etree.Element('project')
                    p.set('id', project_luid)
                    t1.append(p)
                    publish_request_xml.append(t1)

                    encoded_request = etree.tostring(publish_request_xml, encoding='utf-8')

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
                        return self.send_publish_request(url, None, publish_request, boundary_string)
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
                        return self.send_publish_request(url, None, publish_request, boundary_string)

                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(content_filename))
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                "File {} does not have an acceptable extension. Should be .twb,.twbx,.tde,.tdsx,.tds,.tde".format(
                    content_filename))

    def initiate_file_upload(self):
        url = self.build_api_url("fileUploads")
        xml = self.send_post_request(url)
        file_upload = xml.findall('.//t:fileUpload', self.ns_map)
        return file_upload[0].get("uploadSessionId")

    # Uploads a chunk to an already started session
    def append_to_file_upload(self, upload_session_id, content, filename):
        boundary_string = self.generate_boundary_string()
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: name="request_payload"\r\n'
        publish_request += 'Content-Type: text/xml\r\n\r\n'
        publish_request += '\r\n'
        publish_request += "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: name="tableau_file"; filename="{}"\r\n'.format(
            filename)
        publish_request += 'Content-Type: application/octet-stream\r\n\r\n'

        publish_request += content

        publish_request += "\r\n--{}--".format(boundary_string)
        url = self.build_api_url("fileUploads/{}".format(upload_session_id))
        self.send_append_request(url, publish_request, boundary_string)
