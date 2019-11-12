from .rest_api_base import *

# These find LUIDs from real names or other aspects. They get added to the RestApiBase class because methods on
# almost any different object might need a LUID from any of the others
class LookupMethods():
    def query_user_luid(self, username: str) -> str:
        self.start_log_block()
        if username in self.username_luid_cache:
            user_luid = self.username_luid_cache[username]
        else:
            user_luid = self.query_luid_from_name(content_type="user", name=username)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid

    # Datasources in different projects can have the same 'pretty name'.
    def query_datasource_luid(self, datasource_name: str, project_name_or_luid: Optional[str] = None,
                              content_url: Optional[str] = None) -> str:
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
            if len(datasources_with_name) == 1:
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

    def query_group_luid(self, group_name: str) -> str:
        self.start_log_block()
        if group_name in self.group_name_luid_cache:
            group_luid = self.group_name_luid_cache[group_name]
            self.log('Found group name {} in cache with luid {}'.format(group_name, group_luid))
        else:
            group_luid = self.query_single_element_luid_from_endpoint_with_filter('group', group_name)
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_luid

    def query_project_luid(self, project_name: str) -> str:
        self.start_log_block()
        project_luid = self.query_luid_from_name(content_type='project', name=project_name)
        self.end_log_block()
        return project_luid

    def query_schedule_luid(self, schedule_name: str) -> str:
        self.start_log_block()
        luid = self.query_single_element_luid_by_name_from_endpoint('schedule', schedule_name, server_level=True)
        self.end_log_block()
        return luid

    def query_workbook_view_luid(self, wb_name_or_luid: str, view_name: Optional[str] = None,
                                 view_content_url: Optional[str] = None, proj_name_or_luid: Optional[str] = None,
                                 username_or_luid: Optional[str] = None, usage: Optional[bool] = False)-> str:
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException('Usage can only be set to True or False')
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