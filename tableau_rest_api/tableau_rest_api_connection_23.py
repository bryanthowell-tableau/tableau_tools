from tableau_rest_api_connection_22 import *


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
            s.set(u'userQuota', unicode(user_quota))
        if state is not None:
            s.set(u'state', state)
        if storage_quota is not None:
            s.set(u'storageQuota', unicode(storage_quota))
        if disable_subscriptions is not None:
            s.set(u'disableSubscriptions', str(disable_subscriptions).lower())
        if revision_history_enabled is not None:
            s.set(u'revisionHistoryEnabled', str(revision_history_enabled).lower())
        if revision_limit is not None:
            s.set(u'revisionLimit', unicode(revision_limit))

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

    # Check method for filter objects
    @staticmethod
    def _check_filter_objects(filter_checks):
        filters = []
        for f in filter_checks:
            if filter_checks[f] is not None:
                if filter_checks[f].field != f:
                    raise InvalidOptionException(u'A {} filter must be UrlFilter object set to {} field').format(f)
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
        filter_checks = {u'lastLogin': last_login_filter, u'siteRole': site_role_filter}
        filters = self._check_filter_objects(filter_checks)

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

        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'tags': tags_filter,
                         u'ownerName': owner_name_filter}
        filters = self._check_filter_objects(filter_checks)

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

    # Filtering implemented in 2.2
    # query_workbook and query_workbook_luid can't be improved because filtering doesn't take a Project Name/LUID


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
        :type interval_value_s: unicode or list[unicode]
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
        :type interval_value_s: unicode or list[unicode]
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

    def disable_schedule(self, schedule_name_or_luid):
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        s = etree.Element(u'schedule')
        s.set(u'state', u'Suspended')
        tsr.append(s)

        url = self.build_api_url(u"schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def enable_schedule(self, schedule_name_or_luid):
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        s = etree.Element(u'schedule')
        s.set(u'state', u'Active')
        tsr.append(s)

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
        :type weekday_s: list[unicode] or unicode
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
        :type weekday_s: list[unicode] or unicode
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
        url = self.build_api_url(u"")
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