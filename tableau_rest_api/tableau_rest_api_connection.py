# -*- coding: utf-8 -*-

import os

from ..tableau_base import *
from ..tableau_documents.tableau_file import TableauFile
from ..tableau_documents.tableau_workbook import TableauWorkbook
from ..tableau_documents.tableau_datasource import TableauDatasource
from ..tableau_exceptions import *
from rest_xml_request import RestXmlRequest
from published_content import Project20, Project21, Project28, Workbook, Datasource
import urllib


class TableauRestApiConnection(TableauBase):
    # Defines a class that represents a RESTful connection to Tableau Server. Use full URL (http:// or https://)
    def __init__(self, server, username, password, site_content_url=""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauBase.__init__(self)
        if server.find(u'http') == -1:
            raise InvalidOptionException(u'Server URL must include http:// or https://')

        etree.register_namespace(u't', self.ns_map[u't'])
        self.server = server
        self.site_content_url = site_content_url
        self.username = username
        self.__password = password
        self.token = None  # Holds the login token from the Sign In call
        self.site_luid = ""
        self.user_luid = ""
        self.__login_as_user_id = None
        self.__last_error = None
        self.logger = None
        self.__last_response_content_type = None

        # All defined in TableauBase superclass
        self.__site_roles = self.site_roles
        self.__permissionable_objects = self.permissionable_objects
        self.__server_to_rest_capability_map = self.server_to_rest_capability_map

        # Lookup caches to minimize calls
        self.username_luid_cache = {}
        self.group_name_luid_cache = {}

        # For working around SSL issues
        self.verify_ssl_cert = True
    #
    # Object helpers and setter/getters
    #

    def get_last_error(self):
        self.log(self.__last_error)
        return self.__last_error

    def set_last_error(self, error):
        self.__last_error = error

    #
    # REST API Helper Methods
    #

    def build_api_url(self, call, server_level=False):
        if server_level is True:
            return u"{}/api/{}/{}".format(self.server, self.api_version, call)
        else:
            return u"{}/api/{}/sites/{}/{}".format(self.server, self.api_version, self.site_luid, call)

    #
    # Internal REST API Helpers (mostly XML definitions that are reused between methods)
    #
    @staticmethod
    def build_site_request_xml(site_name=None, content_url=None, admin_mode=None, user_quota=None,
                               storage_quota=None, disable_subscriptions=None, state=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :rtype: unicode
        """
        tsr = etree.Element(u"tsRequest")
        s = etree.Element(u'site')

        if site_name is not None:
            s.set(u'name', site_name)
        if content_url is not None:
            s.set(u'contentUrl', content_url)
        if admin_mode is not None:
            s.set(u'adminMode', admin_mode)
        if user_quota is not None:
            s.set(u'userQuota', unicode(user_quota))
        if state is not None:
            s.set(u'state', state)
        if storage_quota is not None:
            s.set(u'storageQuota', unicode(storage_quota))
        if disable_subscriptions is not None:
            s.set(u'disableSubscriptions', unicode(disable_subscriptions).lower())

        tsr.append(s)
        return tsr

    @staticmethod
    def __build_connection_update_xml(new_server_address=None, new_server_port=None,
                                      new_connection_username=None, new_connection_password=None):
        """
        :type new_server_address: unicode
        :type new_server_port: unicode
        :type new_connection_username: unicode
        :type new_connection_password: unicode
        :rtype:
        """

        tsr = etree.Element(u'tsRequest')
        c = etree.Element(u"connection")
        if new_server_address is not None:
            c.set(u'serverAddress', new_server_address)
        if new_server_port is not None:
            c.set(u'serverPort', new_server_port)
        if new_connection_username is not None:
            c.set(u'userName', new_connection_username)
        if new_connection_username is not None:
            c.set(u'password', new_connection_password)
        tsr.append(c)
        return tsr

    #
    # Factory methods for PublishedContent and GranteeCapabilities objects
    #
    def get_published_project_object(self, project_name_or_luid, project_xml_obj=None):
        """
        :type project_name_or_luid: unicode
        :type project_xml_obj: etree.Element
        :rtype: proj_obj:Project
        """
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project20(luid, self, self.version, self.logger, project_xml_obj)
        return proj_obj

    def get_published_workbook_object(self, workbook_name_or_luid, project_name_or_luid=None):
        """
        :type project_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :rtype: wb_obj:Workbook
        """
        if self.is_luid(workbook_name_or_luid):
            luid = workbook_name_or_luid
        else:
            luid = self.query_datasource_luid(workbook_name_or_luid, project_name_or_luid)
        wb_obj = Workbook(luid, self, tableau_server_version=self.version, default=False, logger_obj=self.logger)
        return wb_obj

    def get_published_datasource_object(self, datasource_name_or_luid, project_name_or_luid=None):
        """
        :param project_name_or_luid:
        :param datasource_name_or_luid:
        :type datasource_name_or_luid: unicode
        :rtype: ds_obj:Datasource
        """
        if self.is_luid(datasource_name_or_luid):
            luid = datasource_name_or_luid
        else:
            luid = self.query_datasource_luid(datasource_name_or_luid, project_name_or_luid)
        ds_obj = Datasource(luid, self, tableau_server_version=self.version, default=False, logger_obj=self.logger)
        return ds_obj


    #
    # Sign-in and Sign-out
    #

    def signin(self):
        """
        :rtype:
        """
        self.start_log_block()
        tsr = etree.Element(u"tsRequest")
        c = etree.Element(u"credentials")
        c.set(u"name", self.username)
        c.set(u"password", self.__password)
        s = etree.Element(u"site")
        if self.site_content_url.lower() not in ['default', '']:
            s.set(u"contentUrl", self.site_content_url)

        c.append(s)
        tsr.append(c)

        url = self.build_api_url(u"auth/signin", server_level=True)

        self.log(u'Logging in via: {}'.format(url))
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.xml_request = tsr
        api.http_verb = 'post'
        self.log(u'Login payload is\n {}'.format(etree.tostring(tsr)))


        api.request_from_api(0)
        # self.log(api.get_raw_response())
        xml = api.get_response()


        credentials_element = xml.findall(u'.//t:credentials', self.ns_map)
        self.token = credentials_element[0].get("token")
        self.log(u"Token is " + self.token)
        self.site_luid = credentials_element[0].findall(u".//t:site", self.ns_map)[0].get("id")
        self.user_luid = credentials_element[0].findall(u".//t:user", self.ns_map)[0].get("id")
        self.log(u"Site ID is " + self.site_luid)

        self.end_log_block()

    def signout(self, session_token=None):
        """
        :type session_token: unicode
        :rtype:
        """
        self.start_log_block()
        url = self.build_api_url(u"auth/signout", server_level=True)
        self.log(u'Logging out via: {}'.format(url))
        if session_token is not None:
            api = RestXmlRequest(url, session_token, self.logger, ns_map_url=self.ns_map['t'],
                                 verify_ssl_cert=self.verify_ssl_cert)
        else:
            api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                                 verify_ssl_cert=self.verify_ssl_cert)
        api.http_verb = 'post'
        api.request_from_api()
        self.log(u'Signed out successfully')
        self.end_log_block()

    #
    # HTTP "verb" methods. These actually communicate with the RestXmlRequest object to place the requests
    #

    # baseline method for any get request. appends to base url
    def query_resource(self, url_ending, server_level=False):
        """
        :type url_ending: unicode
        :type server_level: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        api_call = self.build_api_url(url_ending, server_level)
        api = RestXmlRequest(api_call, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.request_from_api()
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def query_single_element_from_endpoint(self, element_name, name_or_luid, server_level=False):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
        :type server_level: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = [u'workbook', u'user', u'datasource', u'site']
        if element_name in singular_endpoints and self.is_luid(name_or_luid):
            element = self.query_resource(u"{}s/{}".format(element_name, name_or_luid))
            self.end_log_block()
            return element
        else:
            elements = self.query_resource(u"{}s".format(element_name), server_level=server_level)
            if self.is_luid(name_or_luid):
                luid = name_or_luid
            else:
                luid = self.query_single_element_luid_by_name_from_endpoint(element_name, name_or_luid)
            element = elements.findall(u'.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
        if len(element) == 1:
            self.end_log_block()
            return element[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_single_element_luid_by_name_from_endpoint(self, element_name, name, server_level=False):
        self.start_log_block()
        elements = self.query_resource("{}s".format(element_name), server_level=server_level)
        if element_name == u'group':
            for e in elements:
                self.group_name_luid_cache[e.get(u'name')] = e.get(u'id')
        element = elements.findall(u'.//t:{}[@name="{}"]'.format(element_name, name), self.ns_map)
        if len(element) == 1:
            self.end_log_block()
            return element[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name {}".format(element_name, name))

    def send_post_request(self, url):
        self.start_log_block()
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.http_verb = u'post'
        api.request_from_api(0)
        xml = api.get_response().getroot()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def send_add_request(self, url, request):
        """
        :type url: unicode
        :type request: etree.Element
        :rtype:
        """
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.xml_request = request
        api.http_verb = 'post'
        api.request_from_api(0)  # Zero disables paging, for all non queries
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def send_update_request(self, url, request):
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.xml_request = request
        api.http_verb = u'put'
        api.request_from_api(0)  # Zero disables paging, for all non queries
        self.end_log_block()
        return api.get_response()

    def send_delete_request(self, url):
        self.start_log_block()
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.http_verb = u'delete'

        try:
            api.request_from_api(0)  # Zero disables paging, for all non queries
            self.end_log_block()
            # Return for counter
            return 1
        except RecoverableHTTPException as e:
            self.log(u'Non fatal HTTP Exception Response {}, Tableau Code {}'.format(e.http_code, e.tableau_error_code))
            if e.tableau_error_code in [404003, 404002]:
                self.log(u'Delete action did not find the resouce. Consider successful, keep going')
            self.end_log_block()
        except:
            raise

    def send_publish_request(self, url, request, boundary_string):
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.set_publish_content(request, boundary_string)
        api.http_verb = u'post'
        api.request_from_api(0)
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def send_append_request(self, url, request, boundary_string):
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)
        api.set_publish_content(request, boundary_string)
        api.http_verb = u'put'
        api.request_from_api(0)
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    # Used when the result is not going to be XML and you want to save the raw response as binary
    def send_binary_get_request(self, url):
        self.start_log_block()
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'],
                             verify_ssl_cert=self.verify_ssl_cert)

        api.http_verb = u'get'
        api.set_response_type(u'binary')
        api.request_from_api(0)
        # Set this content type so we can set the file externsion
        self.__last_response_content_type = api.get_last_response_content_type()
        self.end_log_block()
        return api.get_response()

    #
    # Basic Querying / Get Methods
    #

    #
    # Begin Datasource Querying Methods
    #

    def query_datasources(self, project_name_or_luid=None):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        datasources = self.query_resource(u"datasources")
        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            ds = datasources.findall(u'.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
        else:
            ds = datasources
        self.end_log_block()
        return ds

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
            ds = self.query_resource(u"datasource/{}".format(ds_name_or_luid))
        # Name
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
            ds = self.query_resource(u"datasource/{}".format(ds_luid))
        self.end_log_block()
        return ds

    # Datasources in different projects can have the same 'pretty name'.
    def query_datasource_luid(self, datasource_name, project_name_or_luid=None):
        """
        :type datasource_name: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        datasources = self.query_datasources()
        datasources_with_name = datasources.findall(u'.//t:datasource[@name="{}"]'.format(datasource_name), self.ns_map)
        if len(datasources_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException(u"No datasource found with name {} in any project".format(datasource_name))
        elif project_name_or_luid is None:
            if len(datasources_with_name) == 1:
                self.end_log_block()
                return datasources_with_name[0].get("id")
            # If no project is declared, and
            else:
                raise MultipleMatchesFoundException(u'More than one datasource found by name {} without a project specified'.format(datasource_name))

        else:
            if self.is_luid(project_name_or_luid):
                ds_in_proj = datasources.findall(u'.//t:project[@id="{}"]/..'.format(project_name_or_luid), self.ns_map)
            else:
                ds_in_proj = datasources.findall(u'.//t:project[@name="{}"]/..'.format(project_name_or_luid), self.ns_map)
            if len(ds_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException(u"No datasource found with name {} in project {}".format(datasource_name, project_name_or_luid))
            return ds_in_proj[0].get("id")

    def query_datasource_content_url(self, datasource_name_or_luid, project_name_or_luid=None):
        """
        :type datasource_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        ds = self.query_datasource(datasource_name_or_luid, project_name_or_luid)
        content_url = ds.get(u'contentUrl')
        self.end_log_block()
        return content_url


    #
    # End Datasource Query Methods
    #

    #
    # Start Group Query Methods
    #

    def query_groups(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        groups = self.query_resource(u"groups")
        for group in groups:
            # Add to group-name : luid cache
            group_luid = group.get(u"id")
            group_name = group.get(u'name')
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

    # # No basic verb for querying a single group, so run a query_groups

    def query_group(self, group_name_or_luid):
        """
        :type group_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        group = self.query_single_element_from_endpoint(u'group', group_name_or_luid)
        # Add to group_name : luid cache
        group_luid = group.get(u"id")
        group_name = group.get(u'name')
        self.group_name_luid_cache[group_name] = group_luid

        self.end_log_block()
        return group

    # Groups luckily cannot have the same 'pretty name' on one site
    def query_group_luid(self, group_name):
        """
        :type group_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if group_name in self.group_name_luid_cache:
            group_luid = self.group_name_luid_cache[group_name]
            self.log(u'Found group name {} in cache with luid {}'.format(group_name, group_luid))
        else:
            group_luid = self.query_single_element_luid_by_name_from_endpoint(u'group', group_name)
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_luid

    def query_group_name(self, group_luid):
        """
        :type group_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        for name, luid in self.group_name_luid_cache.items():
            if luid == group_luid:
                group_name = name
                self.log(u'Found group name {} in cache with luid {}'.format(group_name, group_luid))
                return group_name
        # If match is found
        group = self.query_single_element_from_endpoint(u'group', group_luid)
        group_luid = group.get(u"id")
        group_name = group.get(u'name')
        self.log(u'Loading the Group: LUID cache')
        self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_name


    #
    # End Group Querying methods
    #

    #
    # Start Project Querying methods
    #

    def query_projects(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        projects = self.query_resource(u"projects")
        self.end_log_block()
        return projects

    def query_project(self, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :rtype: Project20
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint(u'project', project_name_or_luid))

        self.end_log_block()
        return proj

    def query_project_luid(self, project_name):
        """
        :type project_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        project_luid = self.query_single_element_luid_by_name_from_endpoint(u'project', project_name)
        self.end_log_block()
        return project_luid

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
        sites = self.query_resource(u"sites/", server_level=True)
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
            site_content_urls.append(site.get(u"contentUrl"))
        self.end_log_block()
        return site_content_urls

    # You can only query a site you have logged into this way. Better to use methods that run through query_sites
    def query_current_site(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        site = self.query_resource(u"sites/{}".format(self.site_luid), server_level=True)
        self.end_log_block()
        return site

    #
    # End Site Querying Methods
    #

    #
    # Start User Querying Methods
    #

    # The reference has this name, so for consistency adding an alias
    def get_users(self):
        """
        :rtype: etree.Element
        """
        return self.query_users()

    def query_users(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        users = self.query_resource(u"users")
        self.log(u'Found {} users'.format(unicode(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        user = self.query_single_element_from_endpoint(u"user", username_or_luid)

        # Add to username : luid cache
        user_luid = user.get(u"id")
        username = user.get(u'name')
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
            self.log(u'Found username {} in cache with luid {}'.format(username, user_luid))
        else:
            user_luid = self.query_single_element_luid_by_name_from_endpoint(u"user", username)
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
            luid_index = self.username_luid_cache.values().index(user_luid)
            username = self.username_luid_cache.keys()[luid_index]
        except ValueError as e:
            user = self.query_user(user_luid)
            username = user.get(u'name')

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
        users = self.query_resource(u"groups/{}/users".format(luid))
        self.end_log_block()
        return users

    #
    # End User Querying Methods
    #

    #
    # Start Workbook Querying Methods
    #

    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid=None):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        wbs = self.query_resource(u"users/{}/workbooks".format(user_luid))
        self.end_log_block()
        return wbs

    # Because a workbook can have the same pretty name in two projects, requires more logic
    # Maybe reduce down the xpath here into simpler ElementTree iteration, to remove lxml???
    def query_workbook(self, wb_name_or_luid, p_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type p_name_or_luid: unicode
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        workbooks = self.query_workbooks(username_or_luid)
        if self.is_luid(wb_name_or_luid):
            workbooks_with_name = self.query_resource(u"workbooks/{}".format(wb_name_or_luid))
        else:
            workbooks_with_name = workbooks.findall(u'.//t:workbook[@name="{}"]'.format(wb_name_or_luid), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException(u"No workbook found for username '{}' named {}".format(username_or_luid, wb_name_or_luid))
        elif p_name_or_luid is None:
            if len(workbooks_with_name) == 1:
                wb_luid = workbooks_with_name[0].get("id")
                wb = self.query_resource(u"workbooks/{}".format(wb_luid))
                self.end_log_block()
                return wb
            else:
                self.end_log_block()
                raise MultipleMatchesFoundException(u'More than one workbook found by name {} without a project specified').format(wb_name_or_luid)
        else:
            if self.is_luid(p_name_or_luid):
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(wbp_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(p_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException(u'No workbook found with name {} in project {}').format(wb_name_or_luid, p_name_or_luid)
            wb_luid = wb_in_proj[0].get("id")
            wb = self.query_resource(u"workbooks/{}".format(wb_luid))
            self.end_log_block()
            return wb

    def query_workbook_luid(self, wb_name, p_name_or_luid=None, username_or_luid=None):
        """
        :type username_or_luid: unicode
        :type wb_name: unicode
        :type p_name_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        if username_or_luid is None:
            username_or_luid = self.user_luid
        workbooks = self.query_workbooks(username_or_luid)
        workbooks_with_name = workbooks.findall(u'.//t:workbook[@name="{}"]'.format(wb_name), self.ns_map)
        if len(workbooks_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException(u"No workbook found for username '{}' named {}".format(username_or_luid, wb_name))
        elif len(workbooks_with_name) == 1:
            wb_luid = workbooks_with_name[0].get("id")
            self.end_log_block()
            return wb_luid
        elif len(workbooks_with_name) > 1 and p_name_or_luid is not False:
            if self.is_luid(p_name_or_luid):
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/t:project[@id="{}"]/..'.format(wb_name, p_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(wb_name, p_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException(u'No workbook found with name {} in project {}').format(wb_name, p_name_or_luid)
            wb_luid = wb_in_proj[0].get("id")
            self.end_log_block()
            return wb_luid
        else:
            self.end_log_block()
            raise MultipleMatchesFoundException(u'More than one workbook found by name {} without a project specified').format(wb_name)

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
        wbs_in_project = workbooks.findall(u'.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
        self.end_log_block()
        return wbs_in_project

    def query_workbook_views(self, wb_name_or_luid, p_name_or_luid=None, username_or_luid=None, usage=False):
        """
        :type wb_name_or_luid: unicode
        :type p_name_or_luid: unicode
        :type username_or_luid: unicode
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, p_name_or_luid, username_or_luid)
        vws = self.query_resource(u"workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        self.end_log_block()
        return vws

    def query_workbook_view(self, wb_name_or_luid, view_name_or_luid=None, view_content_url=None, p_name_or_luid=None, username_or_luid=None,
                            usage=False):
        """
        :type wb_name_or_luid: unicode
        :type p_name_or_luid: unicode
        :type username_or_luid: unicode
        :type view_name_or_luid: unicode
        :type view_content_url: unicode
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, p_name_or_luid, username_or_luid)
        vws = self.query_resource(u"workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        if view_content_url is not None:
            views_with_name = vws.findall(u'.//t:view[@contentUrl="{}"]'.format(view_content_url), self.ns_map)
        elif self.is_luid(view_name_or_luid):
            views_with_name = vws.findall(u'.//t:view[@id="{}"]'.format(view_name_or_luid), self.ns_map)
        else:
            views_with_name = vws.findall(u'.//t:view[@name="{}"]'.format(view_name_or_luid), self.ns_map)
        if len(views_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException(u'No view found with name {} in workbook {}').format(view_name_or_luid, wb_name_or_luid)
        elif len(views_with_name) > 1:
            self.end_log_block()
            raise MultipleMatchesFoundException(
                u'More than one view found by name {} in workbook {}. Use view_content_url parameter').format(view_name_or_luid, wb_name_or_luid)
        self.end_log_block()
        return views_with_name

    def query_workbook_view_luid(self, wb_name_or_luid, view_name=None, view_content_url=None, p_name_or_luid=None,
                                 username_or_luid=None, usage=False):
        """
        :type wb_name_or_luid: unicode
        :type p_name_or_luid: unicode
        :type username_or_luid: unicode
        :type view_name: unicode
        :type view_content_url: unicode
        :type usage: bool
        :rtype: unicode
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, p_name_or_luid, username_or_luid)
        vws = self.query_resource(u"workbooks/{}/views?includeUsageStatistics={}".format(wb_luid, str(usage).lower()))
        if view_content_url is not None:
            views_with_name = vws.findall(u'.//t:view[@contentUrl="{}"]'.format(view_content_url), self.ns_map)
        else:
            views_with_name = vws.findall(u'.//t:view[@name="{}"]'.format(view_name), self.ns_map)
        if len(views_with_name) == 0:
            self.end_log_block()
            raise NoMatchFoundException(u'No view found with name {} or content_url {} in workbook {}').format(view_name, view_content_url, wb_name_or_luid)
        elif len(views_with_name) > 1:
            self.end_log_block()
            raise MultipleMatchesFoundException(
                u'More than one view found by name {} in workbook {}. Use view_content_url parameter').format(view_name, view_content_url, wb_name_or_luid)
        view_luid = views_with_name[0].get(u'id')
        self.end_log_block()
        return view_luid

        # This should be the key to updating the connections in a workbook. Seems to return
    # LUIDs for connections and the datatypes, but no way to distinguish them
    def query_workbook_connections(self, wb_name_or_luid, p_name_or_luid=None, username_or_luid=None):
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, p_name_or_luid, username_or_luid)
        conns = self.query_resource(u"workbooks/{}/connections".format(wb_luid))
        self.end_log_block()
        return conns

    # Checks status of AD sync process
    def query_job(self, job_luid):
        """
        :type job_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        job = self.query_resource(u"jobs/{}".format(job_luid))
        self.end_log_block()
        return job

    #
    # End Workbook Query Methods
    #

    #

    #
    # Start of download / save methods
    #

    # Do not include file extension
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
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)

        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      p_name_or_luid=proj_name_or_luid)
        try:
            if filename_no_extension.find('.png') == -1:
                filename_no_extension += '.png'
            save_file = open(filename_no_extension, 'wb')
            url = self.build_api_url(u"workbooks/{}/views/{}/previewImage".format(wb_luid, view_luid))
            image = self.send_binary_get_request(url)
            save_file.write(image)
            save_file.close()
            self.end_log_block()

        # You might be requesting something that doesn't exist
        except RecoverableHTTPException as e:
            self.log(u"Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension))
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
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            if filename_no_extension.find('.png') == -1:
                filename_no_extension += '.png'
            save_file = open(filename_no_extension, 'wb')
            url = self.build_api_url(u"workbooks/{}/previewImage".format(wb_luid))
            image = self.send_binary_get_request(url)
            save_file.write(image)
            save_file.close()
            self.end_log_block()

        # You might be requesting something that doesn't exist, but unlikely
        except RecoverableHTTPException as e:
            self.log(u"Attempt to request preview image results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension))
            self.end_log_block()
            raise

    # Do not include file extension. Without filename, only returns the response
    def download_datasource(self, ds_name_or_luid, filename_no_extension, proj_name_or_luid=None):
        """"
        :type ds_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :return Filename of the saved file
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, project_name_or_luid=proj_name_or_luid)
        try:
            url = self.build_api_url(u"datasources/{}/content".format(ds_luid))
            ds = self.send_binary_get_request(url)
            extension = None
            if self.__last_response_content_type.find(u'application/xml') != -1:
                extension = u'.tds'
            elif self.__last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.tdsx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log(u"download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
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
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_workbook(self, wb_name_or_luid, filename_no_extension, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :return Filename of the save workbook
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            url = self.build_api_url(u"workbooks/{}/content".format(wb_luid))
            wb = self.send_binary_get_request(url)
            extension = None
            if self.__last_response_content_type.find(u'application/xml') != -1:
                extension = u'.twb'
            elif self.__last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.twbx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log(u"download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
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
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    #
    # End download / save methods
    #

    #
    # Create / Add Methods
    #

    def add_user_by_username(self, username, site_role=u'Unlicensed', update_if_exists=False):
        """
        :type username: unicode
        :type site_role: unicode
        :type update_if_exists: bool
        :rtype: unicode
        """
        self.start_log_block()
        # Check to make sure role that is passed is a valid role in the API
        if site_role not in self.site_roles:
            raise InvalidOptionException(u"{} is not a valid site role in Tableau Server".format(site_role))

        self.log(u"Adding {}".format(username))
        tsr = etree.Element(u"tsRequest")
        u = etree.Element(u"user")
        u.set(u"name", username)
        u.set(u"siteRole", site_role)
        tsr.append(u)

        url = self.build_api_url(u'users')
        try:
            new_user = self.send_add_request(url, tsr)
            new_user_luid = new_user.findall(u'.//t:user', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_user_luid
        # If already exists, update site role unless overridden.
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u"Username '{}' already exists on the server".format(username))
                if update_if_exists is True:
                    self.log(u'Updating {} to site role {}'.format(username, site_role))
                    self.update_user(username, site_role=site_role)
                    self.end_log_block()
                    return self.query_user_luid(username)
                else:
                    self.end_log_block()
                    raise AlreadyExistsException(u'Username already exists ', self.query_user_luid(username))
        except:
            self.end_log_block()
            raise

    # This is "Add User to Site", since you must be logged into a site.
    # Set "update_if_exists" to True if you want the equivalent of an 'upsert', ignoring the exceptions
    def add_user(self, username, fullname, site_role=u'Unlicensed', password=None, email=None, update_if_exists=False):
        """
        :type username: unicode
        :type fullname: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :rtype: unicode
        """
        self.start_log_block()
        try:
            # Add username first, then update with full name
            new_user_luid = self.add_user_by_username(username, site_role=site_role, update_if_exists=update_if_exists)
            self.update_user(new_user_luid, fullname, site_role, password, email)
            self.end_log_block()
            return new_user_luid
        except AlreadyExistsException as e:
            self.log(u"Username '{}' already exists on the server; no updates performed".format(username))
            self.end_log_block()
            return e.existing_luid

    # Returns the LUID of an existing group if one already exists
    def create_group(self, group_name):
        """
        :type group_name: unicode
        :rtype: unicode
        """
        self.start_log_block()

        tsr = etree.Element(u"tsRequest")
        g = etree.Element(u"group")
        g.set(u"name", group_name)
        tsr.append(g)

        url = self.build_api_url(u"groups")
        try:
            new_group = self.send_add_request(url, tsr)
            self.end_log_block()
            return new_group.findall(u'.//t:group', self.ns_map)[0].get("id")
        # If the name already exists, a HTTP 409 throws, so just find and return the existing LUID
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u'Group named {} already exists, finding and returning the LUID'.format(group_name))
                self.end_log_block()
                return self.query_group_luid(group_name)

    # Creating a synced ad group is completely different, use this method
    # The luid is only available in the Response header if bg sync. Nothing else is passed this way -- how to expose?
    def create_group_from_ad_group(self, ad_group_name, ad_domain_name, default_site_role=u'Unlicensed',
                                   sync_as_background=True):
        """
        :type ad_group_name: unicode
        :type ad_domain_name: unicode
        :type default_site_role: bool
        :type sync_as_background:
        :rtype: unicode
        """
        self.start_log_block()
        if default_site_role not in self.__site_roles:
            raise InvalidOptionException(u'"{}" is not an acceptable site role'.format(default_site_role))

        tsr = etree.Element(u"tsRequest")
        g = etree.Element(u"group")
        g.set(u"name", ad_group_name)
        i = etree.Element(u"import")
        i.set(u"source", u"ActiveDirectory")
        i.set(u"domainName", ad_domain_name)
        i.set(u"siteRole", default_site_role)
        g.append(i)
        tsr.append(g)

        url = self.build_api_url(u"groups/?asJob={}".format(str(sync_as_background).lower()))
        self.log(url)
        response = self.send_add_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall(u'.//t:job', self.ns_map)
            self.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            self.end_log_block()
            group = response.findall(u'.//t:group', self.ns_map)
            return group[0].get('id')

    def create_project(self, project_name, project_desc=None, no_return=False):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type no_return: bool
        :rtype: Project20
        """
        self.start_log_block()

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        p.set(u"name", project_name)

        if project_desc is not None:
            p.set(u'description', project_desc)
        tsr.append(p)

        url = self.build_api_url(u"projects")
        try:
            new_project = self.send_add_request(url, tsr)

            self.end_log_block()
            project_luid = new_project.findall(u'.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u'Project named {} already exists, finding and returning Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.get_published_project_object(project_name)

    # Both SiteName and ContentUrl must be unique to add a site
    def create_site(self, new_site_name, new_content_url, admin_mode=None, user_quota=None, storage_quota=None,
                    disable_subscriptions=None):
        """
        :type new_site_name: unicode
        :type new_content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :rtype: unicode
        """

        add_request = self.build_site_request_xml(new_site_name, new_content_url, admin_mode, user_quota,
                                                  storage_quota, disable_subscriptions)
        url = self.build_api_url(u"sites/",
                                 server_level=True)  # Site actions drop back out of the site ID hierarchy like login
        try:
            new_site = self.send_add_request(url, add_request)
            return new_site.findall(u'.//t:site', self.ns_map)[0].get("id")
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u"Site with content_url {} already exists".format(new_content_url))
                self.end_log_block()
                raise AlreadyExistsException(u"Site with content_url {} already exists".format(new_content_url),
                                             new_content_url)

    # Take a single user_luid string or a collection of luid_strings
    def add_users_to_group(self, username_or_luid_s, group_name_or_luid):
        """
        :type username_or_luid_s: list[unicode] or unicode
        :type group_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        group_name = u""
        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_name = group_name_or_luid
            group_luid = self.query_group_luid(group_name_or_luid)

        users = self.to_list(username_or_luid_s)
        for user in users:
            username = u""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)

            tsr = etree.Element(u"tsRequest")
            u = etree.Element(u"user")
            u.set(u"id", user_luid)
            tsr.append(u)

            url = self.build_api_url(u"groups/{}/users/".format(group_luid))
            try:
                self.log(u"Adding username {}, ID {} to group {}, ID {}".format(username, user_luid, group_name, group_luid))
                self.send_add_request(url, tsr)
            except RecoverableHTTPException as e:
                self.log(u"Recoverable HTTP exception {} with Tableau Error Code {}, skipping".format(str(e.http_code), e.tableau_error_code))
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
        url = self.build_api_url(u"workbooks/{}/tags".format(wb_luid))

        tsr = etree.Element(u"tsRequest")
        ts = etree.Element(u"tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element(u"tag")
            t.set(u"label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def add_workbook_to_user_favorites(self, favorite_name, wb_name_or_luid, username_or_luid, p_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type wb_name_or_luid: unicode
        :type username_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, p_name_or_luid, username_or_luid)

        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        tsr = etree.Element(u'tsRequest')
        f = etree.Element(u'favorite')
        f.set(u'label', favorite_name)
        w = etree.Element(u'workbook')
        w.set(u'id', wb_luid)
        f.append(w)
        tsr.append(f)

        url = self.build_api_url(u"favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def add_view_to_user_favorites(self, favorite_name, username_or_luid, view_name_or_luid=None, view_content_url=None,
                                   wb_name_or_luid=None, p_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type username_or_luid: unicode
        :type view_name_or_luid: unicode
        :type view_content_url: unicode
        :type wb_name_or_luid: unicode
        :type p_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            if wb_name_or_luid is None:
                raise InvalidOptionException(u'When passing a View Name instead of LUID, must also specify workbook name or luid')
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name_or_luid, view_content_url,
                                                      p_name_or_luid, username_or_luid)
            self.log(u'View luid found {}'.format(view_luid))

        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        tsr = etree.Element(u'tsRequest')
        f = etree.Element(u'favorite')
        f.set(u'label', favorite_name)
        v = etree.Element(u'view')
        v.set(u'id', view_luid)
        f.append(v)
        tsr.append(f)

        url = self.build_api_url(u"favorites/{}".format(user_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    #
    # End Add methods
    #

    #
    # Start Update Methods
    #

    def update_user(self, username_or_luid, full_name=None, site_role=None, password=None,
                    email=None):
        """
        :type username_or_luid: unicode
        :type full_name: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        tsr = etree.Element(u"tsRequest")
        u = etree.Element(u"user")
        if full_name is not None:
            u.set(u'fullName', full_name)
        if site_role is not None:
            u.set(u'siteRole', site_role)
        if email is not None:
            u.set(u'email', email)
        if password is not None:
            u.set(u'password', password)
        tsr.append(u)

        url = self.build_api_url(u"users/{}".format(user_luid))
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

        tsr = etree.Element(u"tsRequest")
        d = etree.Element(u"datasource")
        if new_datasource_name is not None:
            d.set(u'name', new_datasource_name)
        if new_project_luid is not None:
            p = etree.Element(u'project')
            p.set(u'id', new_project_luid)
            d.append(p)
        if new_owner_luid is not None:
            o = etree.Element(u'owner')
            o.set(u'id', new_owner_luid)
            d.append(o)

        tsr.append(d)

        url = self.build_api_url(u"datasources/{}".format(datasource_luid))
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
        url = self.build_api_url(u"datasources/{}/connection".format(datasource_luid))
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

        tsr = etree.Element(u"tsRequest")
        g = etree.Element(u"group")
        g.set(u"name", new_group_name)
        tsr.append(g)

        url = self.build_api_url(u"groups/{}".format(group_luid))
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
            error = u"'{}' passed for sync_as_background. Use True or False".format(str(sync_as_background).lower())
            raise InvalidOptionException(error)

        if default_site_role not in self.__site_roles:
            raise InvalidOptionException(u"'{}' is not a valid site role in Tableau".format(default_site_role))
        # Check that the group exists
        self.query_group(group_name_or_luid)
        tsr = etree.Element(u'tsRequest')
        g = etree.Element(u'group')
        g.set(u'name', ad_group_name)
        i = etree.Element(u'import')
        i.set(u'source', u'ActiveDirectory')
        i.set(u'domainName', ad_domain)
        i.set(u'siteRole', default_site_role)
        g.append(i)
        tsr.append(g)

        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_luid = self.query_group_luid(group_name_or_luid)
        url = self.build_api_url(
            u"groups/{}".format(group_luid) + u"?asJob={}".format(unicode(sync_as_background)).lower())
        response = self.send_update_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall(u'.//t:job', self.ns_map)
            self.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            group = response.findall(u'.//t:group', self.ns_map)
            self.end_log_block()
            return group[0].get('id')

    # Simplest method
    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None):
        """
        :type name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :rtype: Project20
        """
        self.start_log_block()
        if self.is_luid(name_or_luid):
            project_luid = name_or_luid
        else:
            project_luid = self.query_project_luid(name_or_luid)

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        if new_project_name is not None:
            p.set(u'name', new_project_name)
        if new_project_description is not None:
            p.set(u'description', new_project_description)
        tsr.append(p)

        url = self.build_api_url(u"projects/{}".format(project_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    # Can only update the site you are signed into, so take site_luid from the object
    def update_site(self, site_name=None, content_url=None, admin_mode=None, user_quota=None,
                    storage_quota=None, disable_subscriptions=None, state=None):
        """
        :type site_name: unicode
        :type content_url: unicode
        :type admin_mode: unicode
        :type user_quota: unicode
        :type storage_quota: unicode
        :type disable_subscriptions: bool
        :type state: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        tsr = self.build_site_request_xml(site_name, content_url, admin_mode, user_quota, storage_quota,
                                                     disable_subscriptions, state)
        url = self.build_api_url(u"")
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
        tsr = etree.Element(u"tsRequest")
        w = etree.Element(u"workbook")
        w.set(u'showTabs', unicode(show_tabs).lower())
        if new_project_luid is not None:
            p = etree.Element(u'project')
            p.set(u'id', new_project_luid)
            w.append(p)

        if new_owner_luid is not None:
            o = etree.Element(u'owner')
            o.set(u'id', new_owner_luid)
            w.append(o)
        tsr.append(w)

        url = self.build_api_url(u"workbooks/{}".format(workbook_luid))
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
        url = self.build_api_url(u"workbooks/{}/connections/{}".format(wb_luid, connection_luid))
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

            url = self.build_api_url(u"datasources/{}".format(datasource_luid))
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
            url = self.build_api_url(u"projects/{}".format(project_luid))
            self.send_delete_request(url)
        self.end_log_block()

    # Can only delete a site that you have signed into
    def delete_current_site(self):
        """
        :rtype:
        """
        self.start_log_block()
        url = self.build_api_url(u"sites/{}".format(self.site_luid), server_level=True)
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
            url = self.build_api_url(u"workbooks/{}".format(wb_luid))
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
            url = self.build_api_url(u"favorites/{}/workbooks/{}".format(user_luid, wb_luid))
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
            url = self.build_api_url(u"favorites/{}/views/{}".format(user_luid, view_luid))
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
        group_name = u""
        if self.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_name = group_name_or_luid
            group_luid = self.query_group_name(group_name_or_luid)
        users = self.to_list(username_or_luid_s)
        for user in users:
            username = u""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)
            url = self.build_api_url(u"groups/{}/users/{}".format(group_luid, user_luid))
            self.log(u'Removing user {}, id {} from group {}, id {}'.format(username, user_luid, group_name, group_luid))
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
            username = u""
            if self.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.query_user_luid(user)
            url = self.build_api_url(u"users/{}".format(user_luid))
            self.log(u'Removing user {}, id {} from site'.format(username, user_luid))
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
            url = self.build_api_url(u"workbooks/{}/tags/{}".format(wb_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count

    #
    # End Delete Methods
    #

    #
    # Start Publish methods -- workbook, datasources, file upload
    #

    ''' Publish process can go two way:
        (1) Initiate File Upload (2) Publish workbook/datasource (less than 64MB)
        (1) Initiate File Upload (2) Append to File Upload (3) Publish workbook to commit (over 64 MB)
    '''

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True):
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
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content(u'workbook', workbook_filename, workbook_name, project_luid, overwrite,
                                   connection_username, connection_password, save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds)
        workbook = xml.findall(u'.//t:workbook', self.ns_map)
        return workbook[0].get('id')

    def publish_datasource(self, ds_filename, ds_name, project_obj, overwrite=False, connection_username=None,
                           connection_password=None, save_credentials=True):
        """
        :type ds_filename: unicode
        :type ds_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :rtype: unicode
        """
        project_luid = project_obj.luid
        xml = self.publish_content(u'datasource', ds_filename, ds_name, project_luid, overwrite, connection_username,
                                   connection_password, save_credentials)
        datasource = xml.findall(u'.//t:datasource', self.ns_map)
        return datasource[0].get('id')

    # Main method for publishing a workbook. Should intelligently decide to chunk up if necessary
    # If a TableauDatasource or TableauWorkbook is passed, will upload from its content
    def publish_content(self, content_type, content_filename, content_name, project_luid, overwrite=False,
                        connection_username=None, connection_password=None, save_credentials=True, show_tabs=False,
                        check_published_ds=True):
        # Single upload limit in MB
        single_upload_limit = 20

        # If you need a temporary copy when fixing the published datasources
        temp_wb_filename = None

        # Must be 'workbook' or 'datasource'
        if content_type not in [u'workbook', u'datasource']:
            raise InvalidOptionException(u"content_type must be 'workbook' or 'datasource'")

        file_extension = None
        final_filename = None
        cleanup_temp_file = False

        for ending in [u'.twb', u'.twbx', u'.tde', u'.tdsx', u'.tds', u'.tde']:
            if content_filename.endswith(ending):
                file_extension = ending[1:]

                # If twb or twbx, open up and check for any published data sources
                if file_extension.lower() in [u'twb', u'twbx'] and check_published_ds is True:
                    self.log(u"Adjusting any published datasources")
                    t_file = TableauFile(content_filename, self.logger)
                    dses = t_file.tableau_document.datasources
                    for ds in dses:
                        # Set to the correct site
                        if ds.published is True:
                            self.log(u"Published datasource found")
                            self.log(u"Setting publish datasource repository to {}".format(self.site_content_url))
                            ds.published_ds_site = self.site_content_url

                    temp_wb_filename = t_file.save_new_file(u'temp_wb')
                    content_filename = temp_wb_filename
                    # Open the file to be uploaded
                try:
                    content_file = open(content_filename, 'rb')
                    file_size = os.path.getsize(content_filename)
                    file_size_mb = float(file_size) / float(1000000)
                    self.log(u"File {} is size {} MBs".format(content_filename, file_size_mb))
                    final_filename = content_filename

                    # Request type is mixed and require a boundary
                    boundary_string = self.generate_boundary_string()

                    # Create the initial XML portion of the request
                    publish_request = bytes("--{}\r\n".format(boundary_string).encode('utf-8'))
                    publish_request += bytes('Content-Disposition: name="request_payload"\r\n'.encode('utf-8'))
                    publish_request += bytes('Content-Type: text/xml\r\n\r\n'.encode('utf-8'))
                    publish_request += bytes('<tsRequest>\n<{} name="{}" '.format(content_type, content_name).encode('utf-8'))
                    if show_tabs is not False:
                        publish_request += bytes('showTabs="{}"'.format(str(show_tabs).lower()).encode('utf-8'))
                    publish_request += bytes('>\r\n'.encode('utf-8'))
                    if connection_username is not None and connection_password is not None:
                        publish_request += bytes('<connectionCredentials name="{}" password="{}" embed="{}" />\r\n'.format(
                            connection_username, connection_password, str(save_credentials).lower()).encode('utf-8'))
                    publish_request += bytes('<project id="{}" />\r\n'.format(project_luid).encode('utf-8'))
                    publish_request += bytes("</{}></tsRequest>\r\n".format(content_type).encode('utf-8'))
                    publish_request += bytes("--{}".format(boundary_string).encode('utf-8'))

                    # Upload as single if less than file_size_limit MB
                    if file_size_mb <= single_upload_limit:
                        # If part of a single upload, this if the next portion
                        self.log(u"Less than {} MB, uploading as a single call".format(str(single_upload_limit)))
                        publish_request += bytes('\r\n'.encode('utf-8'))
                        publish_request += bytes('Content-Disposition: name="tableau_{}"; filename="{}"\r\n'.format(
                            content_type, final_filename).encode('utf-8'))
                        publish_request += bytes('Content-Type: application/octet-stream\r\n\r\n'.encode('utf-8'))

                        # Content needs to be read unencoded from the file
                        content = content_file.read()

                        # Add to string as regular binary, no encoding
                        publish_request += content

                        publish_request += bytes("\r\n--{}--".format(boundary_string).encode('utf-8'))
                        url = self.build_api_url(u"{}s").format(content_type) + u"?overwrite={}".format(
                            str(overwrite).lower())
                        content_file.close()
                        if temp_wb_filename is not None:
                            os.remove(temp_wb_filename)
                        if cleanup_temp_file is True:
                            os.remove(final_filename)
                        return self.send_publish_request(url, publish_request, boundary_string)
                    # Break up into chunks for upload
                    else:
                        self.log(u"Greater than 10 MB, uploading in chunks")
                        upload_session_id = self.initiate_file_upload()

                        for piece in self.read_file_in_chunks(content_file):
                            self.log(u"Appending chunk to upload session {}".format(upload_session_id))
                            self.append_to_file_upload(upload_session_id, piece, final_filename)

                        url = self.build_api_url(u"{}s").format(content_type) + u"?uploadSessionId={}".format(
                            upload_session_id) + u"&{}Type={}".format(content_type,
                                                                     file_extension) + u"&overwrite={}".format(
                            str(overwrite).lower())
                        publish_request += bytes("--".encode('utf-8'))  # Need to finish off the last boundary
                        self.log(u"Finishing the upload with a publish request")
                        content_file.close()
                        if temp_wb_filename is not None:
                            os.remove(temp_wb_filename)
                        if cleanup_temp_file is True:
                            os.remove(final_filename)
                        return self.send_publish_request(url, publish_request, boundary_string)

                except IOError:
                    print u"Error: File '{}' cannot be opened to upload".format(content_filename)
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                u"File {} does not have an acceptable extension. Should be .twb,.twbx,.tde,.tdsx,.tds,.tde".format(
                    content_filename))

    def initiate_file_upload(self):
        url = self.build_api_url(u"fileUploads")
        xml = self.send_post_request(url)
        file_upload = xml.findall(u'.//t:fileUpload', self.ns_map)
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
        url = self.build_api_url(u"fileUploads/{}".format(upload_session_id))
        self.send_append_request(url, publish_request, boundary_string)
