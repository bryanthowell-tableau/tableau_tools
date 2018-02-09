from tableau_rest_api_connection_24 import *


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

    ###
    ### Fields can be used to limit or expand details can be brought in
    ###

    # These are the new basic methods, now implementing fields
    def query_resource(self, url_ending, server_level=False, filters=None, sorts=None, additional_url_ending=None,
                       fields=None):
        """
        :type url_ending: unicode
        :type server_level: bool
        :type filters: list[UrlFilter]
        :type sorts: list[Sort]
        :type additional_url_ending: unicode
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        url_endings = []
        if filters is not None:
            if len(filters) > 0:
                filters_url = u"filter="
                for f in filters:
                    filters_url += f.get_filter_string() + u","
                filters_url = filters_url[:-1]
                url_endings.append(filters_url)
        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = u"sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + u","
                sorts_url = sorts_url[:-1]
                url_endings.append(sorts_url)
        if fields is not None:
            if len(fields) > 0:
                fields_url = u"fields="
                for field in fields:
                    fields_url += u"{},".format(field)
                fields_url = fields_url[:-1]
                url_endings.append(fields_url)
        if additional_url_ending is not None:
            url_endings.append(additional_url_ending)

        first = True
        if len(url_endings) > 0:
            for ending in url_endings:
                if first is True:
                    url_ending += u"?{}".format(ending)
                    first = False
                else:
                    url_ending += u"&{}".format(ending)

        api_call = self.build_api_url(url_ending, server_level)
        api = RestXmlRequest(api_call, self.token, self.logger, ns_map_url=self.ns_map['t'])

        api.request_from_api()
        xml = api.get_response()  # return Element rather than ElementTree
        self.end_log_block()
        return xml

    def query_elements_from_endpoint_with_filter(self, element_name, name_or_luid=None, all_fields=True):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
        :type all_fields: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = [u'workbook', u'user', u'datasource', u'site']
        if element_name in singular_endpoints and self.is_luid(name_or_luid):
            if all_fields is True:
                element = self.query_resource(u"{}s/{}?fields=_all_".format(element_name, name_or_luid))
            else:
                element = self.query_resource(u"{}s/{}".format(element_name, name_or_luid))
            self.end_log_block()
            return element
        else:
            if self.is_luid(name_or_luid):
                if all_fields is True:
                    elements = self.query_resource(u"{}s?fields=_all_".format(element_name))
                else:
                    elements = self.query_resource(u"{}s".format(element_name))
                luid = name_or_luid
                elements = elements.findall(u'.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
            else:
                elements = self.query_resource(u"{}s?filter=name:eq:{}&fields=_all_".format(element_name, name_or_luid))
        self.end_log_block()
        return elements

    def query_single_element_from_endpoint_with_filter(self, element_name, name_or_luid=None, all_fields=True):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
        :type all_fields: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        elements = self.query_elements_from_endpoint_with_filter(element_name, name_or_luid, all_fields=all_fields)

        if len(elements) == 1:
            self.end_log_block()
            return elements[0]
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_single_element_luid_from_endpoint_with_filter(self, element_name, name, optimize_with_field=False):
        """
        :type element_name: unicode
        :type name: unicode
        :type optimize_with_field: bool
        :rtype: unicode
        """
        self.start_log_block()
        if optimize_with_field is True:
            elements = self.query_resource(u"{}s?filter=name:eq:{}&fields=id".format(element_name, name))
        else:
            elements = self.query_resource(u"{}s?filter=name:eq:{}".format(element_name, name))
        if len(elements) == 1:
            self.end_log_block()
            return elements[0].get(u"id")
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name {}".format(element_name, name))

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
                fields = [u'_all_']

        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter,
                         u'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        ds = self.query_resource(u'datasources', filters=filters, sorts=sorts, fields=fields)
        self.end_log_block()
        return ds

    def query_workbooks(self, username_or_luid=None, all_fields=True, created_at_filter=None, updated_at_filter=None,
                        owner_name_filter=None, tags_filter=None, sorts=None, fields=None):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :type created_at_filter: UrlFilter
        :type updated_at_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type tags_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = [u'_all_']

        if username_or_luid is None:
            user_luid = self.user_luid
        elif self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter,
                         u'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource(u"users/{}/workbooks".format(user_luid))
        else:
            wbs = self.query_resource(u"workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields)
        self.end_log_block()
        return wbs

    # Alias added in
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

    # New methods with Filtering
    def query_users(self, all_fields=True, last_login_filter=None, site_role_filter=None, sorts=None, fields=None):
        """
        :type all_fields: bool
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = [u'_all_']

        filter_checks = {u'lastLogin': last_login_filter, u'siteRole': site_role_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource(u"users", filters=filters, sorts=sorts, fields=fields)
        self.log(u'Found {} users'.format(unicode(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid, all_fields=True):
        """
        :type username_or_luid: unicode
        :type all_fields: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        user = self.query_single_element_from_endpoint_with_filter(u"user", username_or_luid, all_fields=all_fields)
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
            user_luid = self.query_single_element_luid_from_endpoint_with_filter(u"user", username,
                                                                                 optimize_with_field=True)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid