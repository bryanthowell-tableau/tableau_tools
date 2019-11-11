from .rest_api_base import *
class DatasourceMethods(TableauRestApiBase):

    def query_datasources(self, project_name_or_luid: Optional[str] = None, all_fields: Optional[bool] = True,
                          updated_at_filter: Optional[UrlFilter] = None, created_at_filter: Optional[UrlFilter] = None,
                          tags_filter: Optional[UrlFilter] = None, datasource_type_filter: Optional[UrlFilter] = None,
                          sorts: Optional[List[Sort]] = None, fields: Optional[List[str]] = None) -> etree.Element:

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
        :type sorts: List[Sort]
        :type fields: List[unicode]
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
            datasources_with_content_url = datasources_with_name.findall(
                './/t:datasource[@contentUrl="{}"]'.format(content_url), self.ns_map)
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
                    raise MultipleMatchesFoundException(
                        'More than one datasource found by name {} without a project specified'.format(datasource_name))
            # If Project_name is specified was filtered above, so find the name
            else:
                if self.is_luid(project_name_or_luid):
                    ds_in_proj = datasources_with_name.findall('.//t:project[@id="{}"]/..'.format(project_name_or_luid),
                                                               self.ns_map)
                else:
                    ds_in_proj = datasources_with_name.findall(
                        './/t:project[@name="{}"]/..'.format(project_name_or_luid),
                        self.ns_map)
                if len(ds_in_proj) == 1:
                    self.end_log_block()
                    return ds_in_proj[0].get("id")
                else:
                    self.end_log_block()
                    raise NoMatchFoundException(
                        "No datasource found with name {} in project {}".format(datasource_name, project_name_or_luid))

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

    # query_datasource and query_datasource_luid can't be improved because filtering doesn't take a Project Name/LUID

    #
    # End Datasource Query Methods
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

    #
    # Tags
    #

    # Tags can be scalar string or list
    def add_tags_to_datasource(self, ds_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type tag_s: List[unicode]
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
        :type tag_s: List[unicode] or unicode
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