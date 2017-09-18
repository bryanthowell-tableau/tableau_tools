# -*- coding: utf-8 -*-

import os

from ..tableau_base import *
from ..tableau_documents.tableau_file import TableauFile
from ..tableau_documents.tableau_workbook import TableauWorkbook
from ..tableau_documents.tableau_datasource import TableauDatasource
from ..tableau_exceptions import *
from rest_xml_request import RestXmlRequest
from published_content import Project20, Project21, Workbook, Datasource
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
            s.set(u'userQuota', user_quota)
        if state is not None:
            s.set(u'state', state)
        if storage_quota is not None:
            s.set(u'storageQuota', storage_quota)
        if disable_subscriptions is not None:
            s.set(u'disableSubscriptions', str(disable_subscriptions).lower())

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
            c.set(u'serverAddress="{}" ', new_server_address)
        if new_server_port is not None:
            c.set(u'serverPort="{}" ', new_server_port)
        if new_connection_username is not None:
            c.set(u'userName="{}" ', new_connection_username)
        if new_connection_username is not None:
            c.set(u'password="{}"', new_connection_password)
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

    def get_published_workbook_object(self, workbook_name_or_luid, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :rtype: wb_obj:Workbook
        """
        if self.is_luid(workbook_name_or_luid):
            luid = workbook_name_or_luid
        else:
            luid = self.query_datasource_luid(workbook_name_or_luid, project_name_or_luid)
        wb_obj = Workbook(luid, self, self.version, self.logger)
        return wb_obj

    def get_published_datasource_object(self, datasource_name_or_luid, project_name_or_luid):
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
        ds_obj = Datasource(luid, self, self.version, self.logger)
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
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
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
            api = RestXmlRequest(url, session_token, self.logger, ns_map_url=self.ns_map['t'])
        else:
            api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
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
        api = RestXmlRequest(api_call, self.token, self.logger, ns_map_url=self.ns_map['t'])
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
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
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

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
        api.xml_request = request
        api.http_verb = 'post'
        api.request_from_api(0)  # Zero disables paging, for all non queries
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def send_update_request(self, url, request):
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
        api.xml_request = request
        api.http_verb = u'put'
        api.request_from_api(0)  # Zero disables paging, for all non queries
        self.end_log_block()
        return api.get_response()

    def send_delete_request(self, url):
        self.start_log_block()
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
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

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
        api.set_publish_content(request, boundary_string)
        api.http_verb = u'post'
        api.request_from_api(0)
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def send_append_request(self, url, request, boundary_string):
        self.start_log_block()

        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])
        api.set_publish_content(request, boundary_string)
        api.http_verb = u'put'
        api.request_from_api(0)
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    # Used when the result is not going to be XML and you want to save the raw response as binary
    def send_binary_get_request(self, url):
        self.start_log_block()
        api = RestXmlRequest(url, self.token, self.logger, ns_map_url=self.ns_map['t'])

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
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(p_name_or_luid), self.ns_map)
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
            save_file = open(filename_no_extension + ".png", 'wb')
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
        try:
            self.__site_roles.index(site_role)
        except:
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
        new_site = self.send_add_request(url, add_request)
        return new_site.findall(u'.//t:site', self.ns_map)[0].get("id")

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
        if new_owner_luid is not None:
            o = etree.Element(u'owner')
            o.set(u'id', new_owner_luid)
        d.append(p)
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
        url = self.build_api_url(u"/")
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

        for ending in [u'.twb', u'.twbx', u'.tde', u'.tdsx', u'.tds']:
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
                        publish_request += b"--"  # Need to finish off the last boundary
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
                u"File {} does not have an acceptable extension. Should be .twb,.twbx,.tde,.tdsx,.tds".format(
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


class TableauRestApiConnection21(TableauRestApiConnection):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"9.2")

    def get_published_project_object(self, project_name_or_luid, project_xml_obj=None):
        """
        :type project_name_or_luid: unicode
        :type project_xml_obj: project_xml_obj
        :rtype: Project21
        """
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj_obj = Project21(luid, self, self.version, self.logger, content_xml_obj=project_xml_obj)
        return proj_obj

    def query_project(self, project_name_or_luid):
        """
        :type project_name_or_luid: unicode
        :rtype: Project21
        """
        self.start_log_block()
        if self.is_luid(project_name_or_luid):
            luid = project_name_or_luid
        else:
            luid = self.query_project_luid(project_name_or_luid)
        proj = self.get_published_project_object(luid, self.query_single_element_from_endpoint(u'project', project_name_or_luid))

        self.end_log_block()
        return proj

    def create_project(self, project_name, project_desc=None, locked_permissions=True, no_return=False):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type no_return: bool
        :rtype: Project21
        """
        self.start_log_block()

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        p.set(u"name", project_name)

        if project_desc is not None:
            p.set(u'description', project_desc)
        if locked_permissions is not False:
            p.set(u'contentPermissions', u"LockedToProject")
        tsr.append(p)

        url = self.build_api_url(u"projects")
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall(u'.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u'Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None,
                       locked_permissions=True):
        """
        :type name_or_luid: unicode
        :type new_project_name: unicode
        :type new_project_description: unicode
        :type locked_permissions: bool
        :rtype: Project21
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
        if locked_permissions is True:
            p.set(u'contentPermissions', u"LockedToProject")
        elif locked_permissions is False:
            p.set(u'contentPermissions', u"ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url(u"projects/{}".format(project_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def delete_groups(self, group_name_or_luid_s):
        """
        :type group_name_or_luid_s: list[unicode] or unicode
        :rtype:
        """
        self.start_log_block()
        groups = self.to_list(group_name_or_luid_s)
        for group_name_or_luid in groups:
            if group_name_or_luid == u'All Users':
                self.log(u'Cannot delete All Users group, skipping')
                continue
            if self.is_luid(group_name_or_luid):
                group_luid = group_name_or_luid
            else:
                group_luid = self.query_group_luid(group_name_or_luid)
            url = self.build_api_url(u"groups/{}".format(group_luid))
            self.send_delete_request(url)
        self.end_log_block()

class TableauRestApiConnection22(TableauRestApiConnection21):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection21.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"9.3")

    # Begin scheduler querying methods
    #

    def query_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_resource(u"schedules", server_level=True)
        self.end_log_block()
        return schedules

    def query_extract_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        extract_schedules = schedules.findall(u'.//t:schedule[@type="Extract"]', self.ns_map)
        self.end_log_block()
        return extract_schedules

    def query_subscription_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        subscription_schedules = schedules.findall(u'.//t:schedule[@type="Subscription"]', self.ns_map)
        self.end_log_block()
        return subscription_schedules

    def query_schedule_luid(self, schedule_name):
        """
        :type schedule_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_luid_by_name_from_endpoint(u'schedule', schedule_name, server_level=True)
        self.end_log_block()
        return luid

    def query_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_from_endpoint(u'schedule', schedule_name_or_luid, server_level=True)
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
        tasks = self.query_resource(u"schedules/{}/extracts".format(luid))
        self.end_log_block()
        return tasks



    #
    # End Scheduler Querying Methods
    #

    def query_views(self, usage=False):
        """
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        vws = self.query_resource(u"views?includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    # Did not implement any variations of query_workbook_views as it's still necessary to know the workbook to narrow
    # down to that particular view


class TableauRestApiConnection23(TableauRestApiConnection22):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection22.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.0")

    @staticmethod
    def build_site_request_xml(site_name=None, content_url=None, admin_mode=None, user_quota=None,
                               storage_quota=None, disable_subscriptions=None, state=None,
                               revision_history_enabled=None, revision_limit=None):
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
            s.set(u'userQuota', user_quota)
        if state is not None:
            s.set(u'state', state)
        if storage_quota is not None:
            s.set(u'storageQuota', storage_quota)
        if disable_subscriptions is not None:
            s.set(u'disableSubscriptions', str(disable_subscriptions).lower())
        if revision_history_enabled is not None:
            s.set(u'revisionHistoryEnabled', str(revision_history_enabled).lower())
        if revision_limit is not None:
            s.set(u'revisionLimit', revision_limit)

        tsr.append(s)
        return tsr

    # These are the new basic methods that use the Filter functionality introduced
    def query_resource(self, url_ending, server_level=False, filters=None, sorts=None, additional_url_ending=None):
        """
        :type url_ending: unicode
        :type server_level: bool
        :type filters: list[UrlFilter]
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        if filters is not None:
            if len(filters) > 0:
                filters_url = u"filter="
                for f in filters:
                    filters_url += f.get_filter_string() + u","
                filters_url = filters_url[:-1]

        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = u"sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + u","
                sorts_url = sorts_url[:-1]

        if sorts is not None and filters is not None:
            url_ending += u"?{}&{}".format(sorts_url, filters_url)
        elif sorts is not None:
            url_ending += u"?{}".format(sorts_url)
        elif filters is not None and len(filters) > 0:
            url_ending += u"?{}".format(filters_url)
        elif additional_url_ending is not None:
            url_ending += u"?"
        if additional_url_ending is not None:
            url_ending += additional_url_ending

        api_call = self.build_api_url(url_ending, server_level)
        api = RestXmlRequest(api_call, self.token, self.logger, ns_map_url=self.ns_map['t'])

        api.request_from_api()
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def query_elements_from_endpoint_with_filter(self, element_name, name_or_luid=None):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
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
            if self.is_luid(name_or_luid):
                elements = self.query_resource(u"{}s".format(element_name))
                luid = name_or_luid
                elements = elements.findall(u'.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
            else:
                elements = self.query_resource(u"{}s?filter=name:eq:{}".format(element_name, name_or_luid))
        self.end_log_block()
        return elements

    def query_single_element_from_endpoint_with_filter(self, element_name, name_or_luid=None):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        elements = self.query_elements_from_endpoint_with_filter(element_name, name_or_luid)

        if len(elements) == 1:
            self.end_log_block()
            return elements[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_single_element_luid_from_endpoint_with_filter(self, element_name, name):
        """
        :type element_name: unicode
        :type name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        elements = self.query_resource(u"{}s?filter=name:eq:{}".format(element_name, name))
        if len(elements) == 1:
            self.end_log_block()
            return elements[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name {}".format(element_name, name))

    # New methods with Filtering
    def query_users(self, last_login_filter=None, site_role_filter=None, sorts=None):
        """
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        filters = []
        if last_login_filter is not None:
            if last_login_filter.field != u'lastLogin':
                raise InvalidOptionException(u'last_login_filter must be a UrlFilter object set to last_login field')
            else:
                filters.append(last_login_filter)

        if site_role_filter is not None:
            if site_role_filter.field != u'siteRole':
                raise InvalidOptionException(u'site_role_filter must be a UrlFilter object set to site_role field')
            else:
                filters.append(site_role_filter)

        users = self.query_resource(u"users", filters=filters, sorts=sorts)
        self.log(u'Found {} users'.format(unicode(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        user = self.query_single_element_from_endpoint_with_filter(u"user", username_or_luid)
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
        else:
            user_luid = self.query_single_element_luid_from_endpoint_with_filter(u"user", username)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid



    # Filtering implemented for workbooks in 2.2
    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid=None, created_at_filter=None, updated_at_filter=None,
                        owner_name_filter=None, tags_filter=None, sorts=None):
        """
        :type username_or_luid: unicode
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        filters = []
        if updated_at_filter is not None:
            if updated_at_filter.field != u'updatedAt':
                raise InvalidOptionException(u'updated_at_filter must be a UrlFilter object set to updated_at field')
            else:
                filters.append(updated_at_filter)
        if created_at_filter is not None:
            if created_at_filter.field != u'createdAt':
                raise InvalidOptionException(u'created_at_filter must be a UrlFilter object set to created_at field')
            else:
                filters.append(created_at_filter)
        if owner_name_filter is not None:
            if owner_name_filter.field != u'ownerName':
                raise InvalidOptionException(u'owner_name_filter must be a UrlFilter object set to owner_name field')
            else:
                filters.append(owner_name_filter)
        if tags_filter is not None:
            if tags_filter.field != u'tags':
                raise InvalidOptionException(u'tags_filter must be a UrlFilter object set to tags field')
            else:
                filters.append(tags_filter)

        if username_or_luid is not None:
            wbs = self.query_resource(u"users/{}/workbooks".format(user_luid))
        else:
            wbs = self.query_resource(u"workbooks".format(user_luid), sorts=sorts, filters=filters)
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

    def query_workbook(self, wb_name_or_luid, p_name_or_luid=None, username_or_luid=None):
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
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/:project[@id="{}"]/..'.format(p_name_or_luid), self.ns_map)
            else:
                wb_in_proj = workbooks.findall(u'.//t:workbook[@name="{}"]/t:project[@name="{}"]/..'.format(p_name_or_luid), self.ns_map)
            if len(wb_in_proj) == 0:
                self.end_log_block()
                raise NoMatchFoundException(u'No workbook found with name {} in project {}').format(wb_name_or_luid, p_name_or_luid)
            wb_luid = wb_in_proj[0].get("id")
            wb = self.query_resource(u"workbooks/{}".format(wb_luid))
            self.end_log_block()
            return wb

    # Filtering implemented in 2.2
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
        subscriptions = self.query_resource(u'subscriptions')
        filters_dict = {}
        if subscription_subject is not None:
            filters_dict[u'subject'] = u'[@subject="{}"]'.format(subscription_subject)
        if schedule_name_or_luid is not None:
            if self.is_luid(schedule_name_or_luid):
                filters_dict[u'sched'] = u'schedule[@id="{}"'.format(schedule_name_or_luid)
            else:
                filters_dict[u'sched'] = u'schedule[@user="{}"'.format(schedule_name_or_luid)
        if username_or_luid is not None:
            if self.is_luid(username_or_luid):
                filters_dict[u'user'] = u'user[@id="{}"]'.format(username_or_luid)
            else:
                filters_dict[u'user'] = u'user[@name="{}"]'.format(username_or_luid)
        if view_or_workbook is not None:
            if view_or_workbook not in [u'View', u'Workbook']:
                raise InvalidOptionException(u"view_or_workbook must be 'Workbook' or 'View'")
            # Does this search make sense my itself?

        if content_name_or_luid is not None:
            if self.is_luid(content_name_or_luid):
                filters_dict[u'content_luid'] = u'content[@id="{}"'.format(content_name_or_luid)
            else:
                if view_or_workbook is None:
                    raise InvalidOptionException(u'view_or_workbook must be specified for content: "Workook" or "View"')
                if view_or_workbook == u'View':
                    if wb_name_or_luid is None:
                        raise InvalidOptionException(u'Must include wb_name_or_luid for a View name lookup')
                    content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                                 p_name_or_luid=project_name_or_luid)
                elif view_or_workbook == u'Workbook':
                    content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid)
                filters_dict[u'content_luid'] = u'content[@id="{}"'.format(content_luid)

        if u'subject' in filters_dict:
            subscriptions = subscriptions.findall(u'.//t:subscription{}'.format(filters_dict[u'subject']))
        if u'user' in filters_dict:
            subscriptions = subscriptions.findall(u'.//t:subscription/{}/..'.format(filters_dict[u'user']))
        if u'sched' in filters_dict:
            subscriptions = subscriptions.findall(u'.//t:subscription/{}/..'.format(filters_dict[u'sched']))
        if u'content_luid' in filters_dict:
            subscriptions = subscriptions.findall(u'.//t:subscription/{}/..'.format(filters_dict[u'content_luid']))
        self.end_log_block()
        return subscriptions

    def create_subscription(self, subscription_subject, view_or_workbook, content_name_or_luid, schedule_name_or_luid,
                            username_or_luid, project_name_or_luid=None, wb_name_or_luid=None):
        """
        :type subscription_subject: unicode
        :type view_or_workbook: unicode
        :type content_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type username_or_luid: unicode
        :type project_name_or_luid: unicode
        :type wb_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if view_or_workbook not in [u'View', u'Workbook']:
            raise InvalidOptionException(u"view_or_workbook must be 'Workbook' or 'View'")

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
            if view_or_workbook == u'View':
                if wb_name_or_luid is None:
                    raise InvalidOptionException(u'Must include wb_name_or_luid for a View name lookup')
                content_luid = self.query_workbook_view_luid(wb_name_or_luid, content_name_or_luid,
                                                             p_name_or_luid=project_name_or_luid, username_or_luid=user_luid)
            elif view_or_workbook == u'Workbook':
                content_luid = self.query_workbook_luid(content_name_or_luid, project_name_or_luid, user_luid)
            else:
                raise InvalidOptionException(u"view_or_workbook must be 'Workbook' or 'View'")

        tsr = etree.Element(u'tsRequest')
        s = etree.Element(u'subscription')
        s.set(u'subject', subscription_subject)
        c = etree.Element(u'content')
        c.set(u'type', view_or_workbook)
        c.set(u'id', content_luid)
        sch = etree.Element(u'schedule')
        sch.set(u'id', schedule_luid)
        u = etree.Element(u'user')
        u.set(u'id', user_luid)
        s.append(c)
        s.append(sch)
        s.append(u)
        tsr.append(s)

        url = self.build_api_url(u'subscriptions')
        try:
            new_subscription = self.send_add_request(url, tsr)
            new_subscription_luid = new_subscription.findall(u'.//t:subscription', self.ns_map)[0].get("id")
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
        luid = self.create_subscription(subscription_subject, u'Workbook', wb_name_or_luid, schedule_name_or_luid,
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
        luid = self.create_subscription(subscription_subject, u'View', view_name_or_luid, schedule_name_or_luid,
                                        username_or_luid, wb_name_or_luid=wb_name_or_luid, project_name_or_luid=project_name_or_luid)
        self.end_log_block()
        return luid

    def update_subscription(self, subscription_luid, subject=None, schedule_luid=None):
        if subject is None and schedule_luid is None:
            raise InvalidOptionException(u"You must pass one of subject or schedule_luid, or both")
        request = u'<tsRequest>'
        request += u'<subscripotion '
        if subject is not None:
            request += u'subject="{}" '.format(subject)
        request += u'>'
        if schedule_luid is not None:
            request += u'<schedule id="{}" />'.format(schedule_luid)
        request += u'</tsRequest>'

        url = self.build_api_url(u"subscriptions/{}".format(subscription_luid))
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
            url = self.build_api_url(u"subscriptions/{}".format(subscription_luid))
            self.send_delete_request(url)
        self.end_log_block()

    #
    # End Subscription Methods
    #

    #
    # Begin Schedule Methods
    #

    def create_schedule(self, name, extract_or_subscription, frequency, parallel_or_serial, priority, start_time=None,
                        end_time=None, interval_value_s=None, interval_hours_minutes=None):
        """
        :type name: unicode
        :type extract_or_subscription: unicode
        :type frequency: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :type start_time: unicode
        :type end_time: unicode
        :type interval_value_s: unicode or List[unicode]
        :type interval_hours_minutes: unicode
        :rtype:
        """
        self.start_log_block()
        if extract_or_subscription not in [u'Extract', u'Subscription']:
            raise InvalidOptionException(u"extract_or_subscription can only be 'Extract' or 'Subscription'")
        if priority < 1 or priority > 100:
            raise InvalidOptionException(u"priority must be an integer between 1 and 100")
        if parallel_or_serial not in [u'Parallel', u'Serial']:
            raise InvalidOptionException(u"parallel_or_serial must be 'Parallel' or 'Serial'")
        if frequency not in [u'Hourly', u'Daily', u'Weekly', u'Monthly']:
            raise InvalidOptionException(u"frequency must be 'Hourly', 'Daily', 'Weekly' or 'Monthly'")
        tsr = etree.Element(u'tsRequest')
        s = etree.Element(u'schedule')
        s.set(u'name', name)
        s.set(u'priority', unicode(priority))
        s.set(u'type', extract_or_subscription)
        s.set(u'frequency', frequency)
        s.set(u'executionOrder', parallel_or_serial)
        fd = etree.Element(u'frequencyDetails')
        fd.set(u'start', start_time)
        if end_time is not None:
            fd.set(u'end', end_time)
        intervals = etree.Element(u'intervals')

        # Daily does not need an interval value

        if interval_value_s is not None:
            ivs = self.to_list(interval_value_s)
            for i in ivs:
                interval = etree.Element(u'interval')
                if frequency == u'Hourly':
                    if interval_hours_minutes is None:
                        raise InvalidOptionException(u'Hourly must set interval_hours_minutes to "hours" or "minutes"')
                    interval.set(interval_hours_minutes, i)
                if frequency == u'Weekly':
                    interval.set(u'weekDay', i)
                if frequency == u'Monthly':
                    interval.set(u'monthDay', i)
                intervals.append(interval)

        fd.append(intervals)
        s.append(fd)
        tsr.append(s)

        # Schedule requests happen at the server rather than site level, like a login
        url = self.build_api_url(u"schedules", server_level=True)
        try:
            new_schedule = self.send_add_request(url, tsr)
            new_schedule_luid = new_schedule.findall(u'.//t:schedule', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_schedule_luid
        except RecoverableHTTPException as e:
            if e.tableau_error_code == u'409021':
                raise AlreadyExistsException(u'Schedule Already exists on the server', None)

    def update_schedule(self, schedule_name_or_luid, new_name=None, frequency=None, parallel_or_serial=None,
                        priority=None, start_time=None, end_time=None, interval_value_s=None, interval_hours_minutes=None):
        """
        :type schedule_name_or_luid: unicode
        :type new_name: unicode
        :type frequency: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :type start_time: unicode
        :type end_time: unicode
        :type interval_value_s: unicode or List[unicode]
        :type interval_hours_minutes: unicode
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        s = etree.Element(u'schedule')
        if new_name is not None:
            s.set(u'name', new_name)
        if priority is not None:
            if priority < 1 or priority > 100:
                raise InvalidOptionException(u"priority must be an integer between 1 and 100")
            s.set(u'priority', unicode(priority))
        if frequency is not None:
            s.set(u'frequency', frequency)
        if parallel_or_serial is not None:
            if parallel_or_serial not in [u'Parallel', u'Serial']:
                raise InvalidOptionException(u"parallel_or_serial must be 'Parallel' or 'Serial'")
            s.set(u'executionOrder', parallel_or_serial)
        if frequency is not None:
            if frequency not in [u'Hourly', u'Daily', u'Weekly', u'Monthly']:
                raise InvalidOptionException(u"frequency must be 'Hourly', 'Daily', 'Weekly' or 'Monthly'")
            fd = etree.Element(u'frequencyDetails')
            fd.set(u'start', start_time)
            if end_time is not None:
                fd.set(u'end', end_time)
            intervals = etree.Element(u'intervals')

            # Daily does not need an interval value

            if interval_value_s is not None:
                ivs = self.to_list(interval_value_s)
                for i in ivs:
                    interval = etree.Element(u'interval')
                    if frequency == u'Hourly':
                        if interval_hours_minutes is None:
                            raise InvalidOptionException(u'Hourly must set interval_hours_minutes to "hours" or "minutes"')
                        interval.set(interval_hours_minutes, i)
                    if frequency == u'Weekly':
                        interval.set(u'weekDay', i)
                    if frequency == u'Monthly':
                        interval.set(u'monthDay', i)
                    intervals.append(interval)

            fd.append(intervals)
            s.append(fd)
        tsr.append(s)

        # Schedule requests happen at the server rather than site level, like a login
        url = self.build_api_url(u"schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def create_daily_extract_schedule(self, name, start_time, priority=1, parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Extract', u'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_daily_subscription_schedule(self, name, start_time, priority=1, parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Subscription', u'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_weekly_extract_schedule(self, name, weekday_s, start_time, priority=1, parallel_or_serial=u'Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param weekday_s: Use 'Monday', 'Tuesday' etc.
        :type weekday_s: List[unicode] or unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, u'Extract', u'Weekly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_weekly_subscription_schedule(self, name, weekday_s, start_time, priority=1,
                                            parallel_or_serial=u'Parallel'):
        """
        :type name: unicode
        :type parallel_or_serial: unicode
        :type priority: int
        :param start_time: Must be in HH:MM:SS format
        :type start_time: unicode
        :param weekday_s: Use 'Monday', 'Tuesday' etc.
        :type weekday_s: List[unicode] or unicode
        :rtype: unicode
        """
        self.start_log_block()

        luid = self.create_schedule(name, u'Subscription', u'Weekly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_monthly_extract_schedule(self, name, day_of_month, start_time, priority=1,
                                        parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Extract', u'Monthly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_monthly_subscription_schedule(self, name, day_of_month, start_time, priority=1,
                                             parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Subscription', u'Monthly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_hourly_extract_schedule(self, name, interval_hours_or_minutes, interval, start_time, end_time,
                                       priority=1, parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Extract', u'Hourly', parallel_or_serial, priority, start_time, end_time,
                                    interval, interval_hours_or_minutes)
        self.end_log_block()
        return luid

    def create_hourly_subscription_schedule(self, name, interval_hours_or_minutes, interval, start_time, end_time,
                                            priority=1, parallel_or_serial=u'Parallel'):
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

        luid = self.create_schedule(name, u'Subscription', u'Hourly', parallel_or_serial, priority, start_time, end_time,
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
        url = self.build_api_url(u"schedules/{}".format(schedule_luid), server_level=True)
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

            tsr = etree.Element(u'tsRequest')
            f = etree.Element(u'favorite')
            f.set(u'label', favorite_name)
            d = etree.Element(u'datasource')
            d.set(u'id', datasource_luid)
            f.append(d)
            tsr.append(f)

            url = self.build_api_url(u"favorites/{}".format(user_luid))
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
            url = self.build_api_url(u"favorites/{}/datasources/{}".format(user_luid, ds_luid))
            self.send_delete_request(url)
        self.end_log_block()

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
        url = self.build_api_url(u"/")
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

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
        for ending in [u'.png', ]:
            if image_filename.endswith(ending):
                file_extension = ending[1:]

                # Open the file to be uploaded
                try:
                    content_file = open(image_filename, 'rb')

                except IOError:
                    print u"Error: File '{}' cannot be opened to upload".format(image_filename)
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                u"File {} is not PNG. Use PNG image.".format(image_filename))

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
        url = self.build_api_url(u'')[:-1]
        return self.send_publish_request(url, publish_request, boundary_string)

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

        url = self.build_api_url(u'')[:-1]
        return self.send_publish_request(url, publish_request, boundary_string)

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
        wb_revisions = self.query_resource(u'workbooks/{}/revisions'.format(wb_luid))
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
        wb_revisions = self.query_resource(u'workbooks/{}/revisions'.format(ds_luid))
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
        url = self.build_api_url(u"datasources/{}/revisions/{}".format(ds_luid, unicode(revision_number)))
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
        url = self.build_api_url(u"workbooks/{}/revisions/{}".format(wb_luid, unicode(revision_number)))
        self.send_delete_request(url)
        self.end_log_block()

    # Do not include file extension. Without filename, only returns the response
    def download_datasource_revision(self, ds_name_or_luid, revision_number, filename_no_extension, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type revision_number: int
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        try:
            url = self.build_api_url(u"datasources/{}/revisions/{}/content".format(ds_luid, unicode(revision_number)))
            ds = self.send_binary_get_request(url)
            extension = None
            if self.__last_response_content_type.find(u'application/xml') != -1:
                extension = u'.tds'
            elif self.__last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.tdsx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log(u"download_datasource resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
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
            return save_filename
        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_workbook_revision(self, wb_name_or_luid, revision_number, filename_no_extension, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type revision_number: int
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        try:
            url = self.build_api_url(u"workbooks/{}/revisions/{}/content".format(wb_luid, unicode(revision_number)))
            wb = self.send_binary_get_request(url)
            extension = None
            if self.__last_response_content_type.find(u'application/xml') != -1:
                extension = u'.twb'
            elif self.__last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.twbx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
        except RecoverableHTTPException as e:
            self.log(u"download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code,
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
            return save_filename

        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise
        self.end_log_block()

    #
    # End Revision Methods
    #


class TableauRestApiConnection24(TableauRestApiConnection23):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection23.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.1")

    def query_server_info(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        server_info = self.query_resource(u"serverinfo", server_level=True)
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
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        filters = []
        if updated_at_filter is not None:
            if updated_at_filter.field != u'updatedAt':
                raise InvalidOptionException(u'updated_at_filter must be a UrlFilter object set to updated_at field')
            else:
                filters.append(updated_at_filter)
        if created_at_filter is not None:
            if created_at_filter.field != u'createdAt':
                raise InvalidOptionException(u'created_at_filter must be a UrlFilter object set to created_at field')
            else:
                filters.append(created_at_filter)
        if tags_filter is not None:
            if tags_filter.field != u'tags':
                raise InvalidOptionException(u'tags_filter must be a UrlFilter object set to tags field')
            else:
                filters.append(tags_filter)
        vws = self.query_resource(u"views", filters=filters, sorts=sorts,
                                  additional_url_ending=u"includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    def query_view(self, vw_name_or_luid):
        """
        :type vw_name_or_luid:
        :rtype: etree.Element
        """
        self.start_log_block()
        vw = self.query_single_element_from_endpoint_with_filter(u'view', vw_name_or_luid)
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

        filters = []
        if updated_at_filter is not None:
            if updated_at_filter.field != u'updatedAt':
                raise InvalidOptionException(u'updated_at_filter must be a UrlFilter object set to updatedAt field')
            else:
                filters.append(updated_at_filter)
        if created_at_filter is not None:
            if created_at_filter.field != u'createdAt':
                raise InvalidOptionException(u'created_at_filter must be a UrlFilter object set to createdAt field')
            else:
                filters.append(created_at_filter)
        if datasource_type_filter is not None:
            if datasource_type_filter.field != u'type':
                raise InvalidOptionException(u'datasource_type_filter must be a UrlFilter object set to type field')
            else:
                filters.append(datasource_type_filter)
        if tags_filter is not None:
            if tags_filter.field != u'tags':
                raise InvalidOptionException(u'tags_filter must be a UrlFilter object set to tags field')
            else:
                filters.append(tags_filter)

        ds = self.query_resource(u'datasources', filters=filters, sorts=sorts)
        self.end_log_block()
        return ds

    def query_datasource_luid(self, datasource_name, project_name_or_luid=None):
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

    def query_datasource(self, ds_name_or_luid, proj_name_or_luid=None):
        filter = "filter here"


class TableauRestApiConnection25(TableauRestApiConnection24):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection24.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u'10.2')

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
        favorites = self.query_resource(u"favorites/{}/".format(user_luid))

        self.end_log_block()
        return favorites

    def create_project(self, project_name, project_desc=None, locked_permissions=True, publish_samples=False,
                       no_return=False):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :type no_return: bool
        :rtype: Project21
        """
        self.start_log_block()

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        p.set(u"name", project_name)

        if project_desc is not None:
            p.set(u'description', project_desc)
        if locked_permissions is not False:
            p.set(u'contentPermissions', u"LockedToProject")
        tsr.append(p)

        url = self.build_api_url(u"projects")
        if publish_samples is True:
            url += u'?publishSamples=true'
        try:
            new_project = self.send_add_request(url, tsr)
            self.end_log_block()
            project_luid = new_project.findall(u'.//t:project', self.ns_map)[0].get("id")
            if no_return is False:
                return self.get_published_project_object(project_luid, new_project)
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log(u'Project named {} already exists, finding and returning the Published Project Object'.format(project_name))
                self.end_log_block()
                if no_return is False:
                    return self.query_project(project_name)

    def update_project(self, name_or_luid, new_project_name=None, new_project_description=None,
                       locked_permissions=True, publish_samples=False):
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

        tsr = etree.Element(u"tsRequest")
        p = etree.Element(u"project")
        if new_project_name is not None:
            p.set(u'name', new_project_name)
        if new_project_description is not None:
            p.set(u'description', new_project_description)
        if locked_permissions is True:
            p.set(u'contentPermissions', u"LockedToProject")
        elif locked_permissions is False:
            p.set(u'contentPermissions', u"ManagedByOwner")

        tsr.append(p)

        url = self.build_api_url(u"projects/{}".format(project_luid))
        if publish_samples is True:
            url += u'?publishSamples=true'

        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return self.get_published_project_object(project_luid, response)

    def query_view_image(self, view_name_or_luid, save_filename_no_extension, high_resolution=False,
                         wb_name_or_luid=None, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type save_filename_no_extension: unicode
        :type high_resolution: bool
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      p_name_or_luid=proj_name_or_luid)
        self.end_log_block()


class TableauRestApiConnection26(TableauRestApiConnection25):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection25.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.3")

    def get_extract_refresh_tasks(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_tasks = self.query_resource(u'tasks/extractRefreshes')
        self.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_task = self.query_resource(u'tasks/extractRefreshes/{}'.format(task_luid))
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
        tasks_on_sched = tasks.findall(u'.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.ns_map)
        if len(tasks_on_sched) == 0:
            self.end_log_block()
            raise NoMatchFoundException(
                u"No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.end_log_block()

    def run_extract_refresh_task(self, task_luid):
        """
        :task task_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        tsr = etree.Element(u'tsRequest')

        url = self.build_api_url(u'tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.send_add_request(url, tsr)
        self.end_log_block()
        return response.findall(u'.//t:job', self.ns_map)[0].get(u"id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get(u'id'))
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

        extracts_for_wb = tasks.findall(u'.//t:extract/workbook[@id="{}"]..'.format(wb_luid), self.ns_map)

        for extract in extracts_for_wb:
            self.run_extract_refresh_task(extract.get(u'id'))
        self.end_log_block()

    # Check if this actually works
    def run_extract_refresh_for_datasource(self, ds_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
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
            ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid, username_or_luid)
        tasks = self.get_extract_refresh_tasks()
        print tasks
        extracts_for_ds = tasks.findall(u'.//t:extract/datasource[@id="{}"]..'.format(ds_luid), self.ns_map)
        # print extracts_for_wb
        for extract in extracts_for_ds:
            self.run_extract_refresh_task(extract.get(u'id'))
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
        url = self.build_api_url(u"datasources/{}/tags".format(ds_luid))

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
            url = self.build_api_url(u"views/{}/tags/{}".format(ds_luid, tag))
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
        url = self.build_api_url(u"views/{}/tags".format(vw_luid))

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
            url = self.build_api_url(u"views/{}/tags/{}".format(vw_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count


class UrlFilter:
    def __init__(self, field, operator, values):
        self.field = field
        self.operator = operator
        self.values = values

    def get_filter_string(self):
        """
        :rtype: unicode
        """
        if len(self.values) == 0:
            raise InvalidOptionException(u'Must pass in at least one value for the filter')
        elif len(self.values) == 1:
            value_string = self.values[0]
        else:
            value_string = u",".join(self.values)
            value_string = u"[{}]".format(value_string)
        url = u"{}:{}:{}".format(self.field, self.operator, value_string)
        return url

    @staticmethod
    def create_name_filter(name):
        """
        :type name: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'name', u'eq', [name, ])

    @staticmethod
    def create_owner_name_filter(owner_name):
        """
        :type owner_name: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'ownerName', u'eq', [owner_name, ])

    @staticmethod
    def create_site_role_filter(site_role):
        """
        :type site_role: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'siteRole', u'eq', [site_role, ])

    @staticmethod
    def create_datasource_type_filter(ds_type):
        """
        :type ds_type: unicode
        :rtype: UrlFilter
        """
        return UrlFilter(u'type', u'eq', [ds_type, ])

    @staticmethod
    def create_last_login_filter(operator, last_login_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param last_login_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type last_login_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'lastLogin', operator, [last_login_time, ])

    @staticmethod
    def create_created_at_filter(operator, created_at_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param created_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type created_at_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'createdAt', operator, [created_at_time, ])

    @staticmethod
    def create_updated_at_filter(operator, updated_at_time):
        """
        :param operator: Should be one of 'eq', 'gt', 'gte', 'lt', 'lte'
        :type operator: unicode
        :param updated_at_time: ISO 8601 representation of time like 2016-01-01T00:00:00:00Z
        :type updated_at_time: unicode
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'gt', u'gte', u'lt', u'lte']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'gt', 'gte', 'lt', 'lte' ")
        # Convert to the correct time format

        return UrlFilter(u'updatedAt', operator, [updated_at_time, ])

    @staticmethod
    def create_tags_filter(operator, tags):
        """
        :param operator: Should be 'eq' for single tag match, 'in' for multiple tag match
        :type operator: unicode
        :type tags: list[unicode]
        :rtype: UrlFilter
        """
        comparison_operators = [u'eq', u'in']
        if operator not in comparison_operators:
            raise InvalidOptionException(u"operator must be one of 'eq', 'in' ")
        if operator == u'eq' and len(tags) > 1:
            raise InvalidOptionException(u'Use "in" operator for multiple tag search')
        return UrlFilter(u'tags', operator, tags)


class Sort:
    def __init__(self, field, direction):
        """
        :type field: unicode
        :param direction: must be asc or desc
        :type direction: uniode
        """
        self.field = field
        if direction not in [u'asc', u'desc']:
            raise InvalidOptionException(u'Sort direction must be asc or desc')
        self.direction = direction

    def get_sort_string(self):
        """
        :rtype: unicode
        """
        sort_string = u'{}:{}'.format(self.field, self.direction)
        return sort_string


class ContentDeployer:
    def __init__(self):
        self._current_site_index = 0
        self.sites = []

    def __iter__(self):
        """
        :rtype: TableauRestApiConnection
        """
        return self.sites[self._current_site_index]

    def add_site(self, t_rest_api_connection):
        """
        :type t_rest_api_connection: TableauRestApiConnection
        """
        self.sites.append(t_rest_api_connection)

    @property
    def current_site(self):
        """
        :rtype: TableauRestApiConnection
        """
        return self.sites[self._current_site_index]

    @current_site.setter
    def current_site(self, site_content_url):
        i = 0
        for site in self.sites:
            if site.site_content_url == site_content_url:
                self._current_site_index = i
            i += 1

    def next(self):
        if self._current_site_index < len(self.sites):
            self._current_site_index += 1
        else:
            raise StopIteration()

