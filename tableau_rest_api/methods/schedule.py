from .rest_api_base import *
class ScheduleMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_schedules(self) -> etree.Element:
        self.start_log_block()
        schedules = self.query_resource("schedules", server_level=True)
        self.end_log_block()
        return schedules

    def query_schedules_json(self, page_number: Optional[int] = None)-> str:
        self.start_log_block()
        schedules = self.query_resource_json("schedules", server_level=True, page_number=page_number)
        self.end_log_block()
        return schedules

    def query_extract_schedules(self) -> etree.Element:
        self.start_log_block()
        schedules = self.query_schedules()
        extract_schedules = schedules.findall('.//t:schedule[@type="Extract"]', self.ns_map)
        self.end_log_block()
        return extract_schedules

    def query_subscription_schedules(self) -> etree.Element:
        self.start_log_block()
        schedules = self.query_schedules()
        subscription_schedules = schedules.findall('.//t:schedule[@type="Subscription"]', self.ns_map)
        self.end_log_block()
        return subscription_schedules



    def query_schedule(self, schedule_name_or_luid: str) -> etree.Element:
        self.start_log_block()
        schedule = self.query_single_element_from_endpoint('schedule', schedule_name_or_luid, server_level=True)
        self.end_log_block()
        return schedule

    def query_extract_refresh_tasks_by_schedule(self, schedule_name_or_luid: str) -> etree.Element:
        self.start_log_block()
        luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.query_resource("schedules/{}/extracts".format(luid))
        self.end_log_block()
        return tasks

    def create_schedule(self, name: Optional[str] = None, extract_or_subscription: Optional[str] = None,
                        frequency: Optional[str] = None, parallel_or_serial: Optional[str] = None,
                        priority: Optional[int] = None, start_time: Optional[str] = None,
                        end_time: Optional[str] = None, interval_value_s: Optional[Union[List[str], str]] = None,
                        interval_hours_minutes: Optional[int] = None,
                        direct_xml_request: Optional[etree.Element] = None) -> str:
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
                            raise InvalidOptionException(
                                'Hourly must set interval_hours_minutes to "hours" or "minutes"')
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

    def update_schedule(self, schedule_name_or_luid: str, new_name: Optional[str] = None,
                        frequency: Optional[str] = None, parallel_or_serial: Optional[str] = None,
                        priority: Optional[int] = None, start_time: Optional[str] = None,
                        end_time: Optional[str] = None, interval_value_s: Optional[Union[List[str], str]] = None,
                        interval_hours_minutes: Optional[int] = None,
                        direct_xml_request: Optional[etree.Element] = None) -> etree.Element:
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
                                raise InvalidOptionException(
                                    'Hourly must set interval_hours_minutes to "hours" or "minutes"')
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
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def disable_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
        luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        s = etree.Element('schedule')
        s.set('state', 'Suspended')
        tsr.append(s)

        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def enable_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
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

class SubscriptionMethods(TableauRestApiBase):
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

class ScheduleMethods27(ScheduleMethods):
    pass

class ScheduleMethods28(ScheduleMethods27):
    def add_workbook_to_schedule(self, wb_name_or_luid, schedule_name_or_luid, proj_name_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)

        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        t = etree.Element('task')
        er = etree.Element('extractRefresh')
        w = etree.Element('workbook')
        w.set('id', wb_luid)
        er.append(w)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/workbooks".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()

    def add_datasource_to_schedule(self, ds_name_or_luid, schedule_name_or_luid, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)

        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element('tsRequest')
        t = etree.Element('task')
        er = etree.Element('extractRefresh')
        d = etree.Element('datasource')
        d.set('id', ds_luid)
        er.append(d)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/datasources".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()

class ScheduleMethods30(ScheduleMethods28):
    pass

class ScheduleMethods31(ScheduleMethods30):
    pass

class ScheduleMethods32(ScheduleMethods31):
    pass

class ScheduleMethods33(ScheduleMethods32):
    pass

class ScheduleMethods34(ScheduleMethods33):
    pass

class ScheduleMethods35(ScheduleMethods34):
    pass

class ScheduleMethods36(ScheduleMethods35):
    pass