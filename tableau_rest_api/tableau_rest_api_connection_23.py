from .tableau_rest_api_connection_22 import *


class TableauRestApiConnection23(TableauRestApiConnection22):
    def __init__(self, server, username, password, site_content_url=""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection22.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version("10.0")

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

    # These are the new basic methods that use the Filter functionality introduced
    def query_resource(self, url_ending, server_level=False, filters=None, sorts=None, additional_url_ending=None):
        """
        :type url_ending: unicode
        :type server_level: bool
        :type filters: list[UrlFilter]
        :type sorts: list[Sort]
        :type additional_url_ending: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if filters is not None:
            if len(filters) > 0:
                filters_url = "filter="
                for f in filters:
                    filters_url += f.get_filter_string() + ","
                filters_url = filters_url[:-1]

        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = "sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + ","
                sorts_url = sorts_url[:-1]

        if sorts is not None and filters is not None:
            url_ending += "?{}&{}".format(sorts_url, filters_url)
        elif sorts is not None:
            url_ending += "?{}".format(sorts_url)
        elif filters is not None and len(filters) > 0:
            url_ending += "?{}".format(filters_url)
        elif additional_url_ending is not None:
            url_ending += "?"
        if additional_url_ending is not None:
            url_ending += additional_url_ending

        api_call = self.build_api_url(url_ending, server_level)
        self._request_obj.set_response_type('xml')
        self._request_obj.http_verb = 'get'
        self._request_obj.url = api_call
        self._request_obj.request_from_api()
        xml = self._request_obj.get_response()  # return Element rather than ElementTree
        self._request_obj.url = None

        return xml

    def query_elements_from_endpoint_with_filter(self, element_name, name_or_luid=None):
        """
        :type element_name: unicode
        :type name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        # A few elements have singular endpoints
        singular_endpoints = ['workbook', 'user', 'datasource', 'site']
        if element_name in singular_endpoints and self.is_luid(name_or_luid):
            element = self.query_resource("{}s/{}".format(element_name, name_or_luid))
            self.end_log_block()
            return element
        else:
            if self.is_luid(name_or_luid):
                elements = self.query_resource("{}s".format(element_name))
                luid = name_or_luid
                elements = elements.findall('.//t:{}[@id="{}"]'.format(element_name, luid), self.ns_map)
            else:
                elements = self.query_resource("{}s?filter=name:eq:{}".format(element_name, name_or_luid))
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
            raise NoMatchFoundException("No {} found with name or luid {}".format(element_name, name_or_luid))

    def query_single_element_luid_from_endpoint_with_filter(self, element_name, name):
        """
        :type element_name: unicode
        :type name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        elements = self.query_resource("{}s?filter=name:eq:{}".format(element_name, name))
        if len(elements) == 1:
            self.end_log_block()
            return elements[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name {}".format(element_name, name))

    # These are the new basic methods that use the Filter functionality introduced
    def query_resource_json(self, url_ending, server_level=False, filters=None, sorts=None, additional_url_ending=None,
                            page_number=None):
        """
        :type url_ending: unicode
        :type server_level: bool
        :type filters: list[UrlFilter]
        :type sorts: list[Sort]
        :type additional_url_ending: unicode
        :type page_number: int
        :rtype: etree.Element
        """
        self.start_log_block()
        if filters is not None:
            if len(filters) > 0:
                filters_url = "filter="
                for f in filters:
                    filters_url += f.get_filter_string() + ","
                filters_url = filters_url[:-1]

        if sorts is not None:
            if len(sorts) > 0:
                sorts_url = "sort="
                for sort in sorts:
                    sorts_url += sort.get_sort_string() + ","
                sorts_url = sorts_url[:-1]

        if sorts is not None and filters is not None:
            url_ending += "?{}&{}".format(sorts_url, filters_url)
        elif sorts is not None:
            url_ending += "?{}".format(sorts_url)
        elif filters is not None and len(filters) > 0:
            url_ending += "?{}".format(filters_url)
        elif additional_url_ending is not None:
            url_ending += "?"
        if additional_url_ending is not None:
            url_ending += additional_url_ending

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

    # Alias added in
    def get_users(self, last_login_filter=None, site_role_filter=None, sorts=None):
        """
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        return self.query_users(last_login_filter=last_login_filter, site_role_filter=site_role_filter,
                                sorts=sorts)

    # New methods with Filtering
    def query_users(self, last_login_filter=None, site_role_filter=None, sorts=None):
        """
        :type last_login_filter: UrlFilter
        :type site_role_filter: UrlFilter
        :type sorts: list[Sort]
        :rtype: etree.Element
        """
        self.start_log_block()
        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource("users", filters=filters, sorts=sorts)
        self.log('Found {} users'.format(str(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid):
        """
        :type username_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        user = self.query_single_element_from_endpoint_with_filter("user", username_or_luid)
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
            user_luid = self.query_single_element_luid_from_endpoint_with_filter("user", username)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid

    # Filtering implemented for workbooks in 2.2
    # This uses the logged in username for convenience by default
    def query_workbooks(self, username_or_luid=None, project_name_or_luid=None, created_at_filter=None, updated_at_filter=None,
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

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'tags': tags_filter,
                         'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

        if username_or_luid is not None:
            wbs = self.query_resource("users/{}/workbooks".format(user_luid))
        else:
            wbs = self.query_resource("workbooks".format(user_luid), sorts=sorts, filters=filters)
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
            url = self.build_api_url("datasources/{}/revisions/{}/content".format(ds_luid, str(revision_number)))
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