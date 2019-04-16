from tableau_rest_api_connection_24 import *
import urllib

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
        favorites = self.query_resource_json(u"favorites/{}/".format(user_luid), page_number=page_number)

        self.end_log_block()
        return favorites

    def create_project(self, project_name=None, project_desc=None, locked_permissions=True, publish_samples=False,
                       no_return=False, direct_xml_request=None):
        """
        :type project_name: unicode
        :type project_desc: unicode
        :type locked_permissions: bool
        :type publish_samples: bool
        :type no_return: bool
        :type direct_xml_request: etree.Element
        :rtype: Project21
        """
        self.start_log_block()
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
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
                new_key = u"vf_{}".format(key)
                # Check if this just a string
                if isinstance(view_filter_map[key], basestring):
                    value = view_filter_map[key]
                else:
                    value = ",".join(map(unicode,view_filter_map[key]))
                final_filter_map[new_key] = value

            additional_url_params = u"?" + urllib.urlencode(final_filter_map)
            if high_resolution is True:
                additional_url_params += u"&resolution=high"

        else:
            additional_url_params = u""
            if high_resolution is True:
                additional_url_params += u"?resolution=high"
        try:

            url = self.build_api_url(u"views/{}/{}{}".format(view_luid, download_type, additional_url_params))
            binary_result = self.send_binary_get_request(url)

            self.end_log_block()
            return binary_result
        except RecoverableHTTPException as e:
            self.log(u"Attempt to request results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
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
        image = self._query_data_file(u'image', view_name_or_luid=view_name_or_luid, high_resolution=high_resolution,
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
                self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension))
                self.end_log_block()
                raise
        else:
            raise InvalidOptionException(
                u'This method is for saving response to file. Must include filename_no_extension parameter')


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
        self._request_obj.set_response_type(u'xml')
        self._request_obj.url = api_call
        self._request_obj.http_verb = u'get'
        self._request_obj.request_from_api()
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        self._request_obj.url = None
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

    # These are the new basic methods that use the Filter functionality introduced
    def query_resource_json(self, url_ending, server_level=False, filters=None, sorts=None, additional_url_ending=None,
                            fields=None, page_number=None):
        """
        :type url_ending: unicode
        :type server_level: bool
        :type filters: list[UrlFilter]
        :type sorts: list[Sort]
        :type additional_url_ending: unicode
        :type fields: list[unicode]
        :type page_number: int
        :rtype: json
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
        if self._request_json_obj is None:
            self._request_json_obj = RestJsonRequest(token=self.token, logger=self.logger,
                                                     verify_ssl_cert=self.verify_ssl_cert)
        self._request_json_obj.http_verb = u'get'
        self._request_json_obj.url = api_call
        self._request_json_obj.request_from_api(page_number=page_number)
        json_response = self._request_json_obj.get_response()  # return JSON as string
        self._request_obj.url = None
        self.end_log_block()
        return json_response

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

        datasources = self.query_resource(u'datasources', filters=filters, sorts=sorts, fields=fields)

        # If there is a project filter
        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            dses_in_project = datasources.findall(u'.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
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
                fields = [u'_all_']

        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter,
                         u'type': datasource_type_filter}
        filters = self._check_filter_objects(filter_checks)

        datasources = self.query_resource_json(u'datasources', filters=filters, sorts=sorts, fields=fields,
                                               page_number=page_number)

        self.end_log_block()
        return datasources

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

        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                project_luid = project_name_or_luid
            else:
                project_luid = self.query_project_luid(project_name_or_luid)
            wbs_in_project = wbs.findall(u'.//t:project[@id="{}"]/..'.format(project_luid), self.ns_map)
            wbs = etree.Element(self.ns_prefix + 'workbooks')
            for wb in wbs_in_project:
                wbs.append(wb)
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
            wbs = self.query_resource_json(u"users/{}/workbooks".format(user_luid), sorts=sorts, filters=filters,
                                           fields=fields, page_number=page_number)
        else:
            wbs = self.query_resource_json(u"workbooks".format(user_luid), sorts=sorts, filters=filters, fields=fields,
                                           page_number=page_number)

        self.end_log_block()
        return wbs

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
                fields = [u'_all_']

        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource(u"views", filters=filters, sorts=sorts, fields=fields,
                                  additional_url_ending=u"includeUsageStatistics={}".format(str(usage).lower()))
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
                fields = [u'_all_']

        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter}
        filters = self._check_filter_objects(filter_checks)

        vws = self.query_resource_json(u"views", filters=filters, sorts=sorts, fields=fields,
                                  additional_url_ending=u"includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

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
                fields = [u'_all_']

        filter_checks = {u'lastLogin': last_login_filter, u'siteRole': site_role_filter, u'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource(u"users", filters=filters, sorts=sorts, fields=fields)
        self.log(u'Found {} users'.format(unicode(len(users))))
        self.end_log_block()
        return users

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

    # New methods with Filtering
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
                fields = [u'_all_']

        filter_checks = {u'lastLogin': last_login_filter, u'siteRole': site_role_filter, u'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource_json(u"users", filters=filters, sorts=sorts, fields=fields, page_number=page_number)

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
                url = self.build_api_url(u"datasources/{}/content?includeExtract=False".format(ds_luid))
            else:
                url = self.build_api_url(u"datasources/{}/content".format(ds_luid))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find(u'application/xml') != -1:
                extension = u'.tds'
            elif self._last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.tdsx'
            self.log(u'Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
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
                url = self.build_api_url(u"workbooks/{}/content?includeExtract=False".format(wb_luid))
            else:
                url = self.build_api_url(u"workbooks/{}/content".format(wb_luid))
            wb = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find(u'application/xml') != -1:
                extension = u'.twb'
            elif self._last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.twbx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
            self.log(
                u'Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
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
                url = self.build_api_url(u"datasources/{}/revisions/{}/content?includeExtract=False".format(ds_luid,
                                                                                                            unicode(revision_number)))
            else:
                url = self.build_api_url(
                    u"datasources/{}/revisions/{}/content".format(ds_luid, unicode(revision_number)))
            ds = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find(u'application/xml') != -1:
                extension = u'.tds'
            elif self._last_response_content_type.find(u'application/octet-stream') != -1:
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
            self.end_log_block()
            return save_filename
        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
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
                url = self.build_api_url(u"workbooks/{}/revisions/{}/content?includeExtract=False".format(wb_luid,
                                                                                                          unicode(revision_number)))
            else:
                url = self.build_api_url(u"workbooks/{}/revisions/{}/content".format(wb_luid, unicode(revision_number)))
            wb = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find(u'application/xml') != -1:
                extension = u'.twb'
            elif self._last_response_content_type.find(u'application/octet-stream') != -1:
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
            self.end_log_block()
            return save_filename

        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            self.end_log_block()
            raise