from .rest_api_base import *


class ScheduleMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_schedules(self) -> ET.Element:
        self.start_log_block()
        schedules = self.query_resource("schedules", server_level=True)
        self.end_log_block()
        return schedules

    def query_schedules_json(self, page_number: Optional[int] = None)-> Dict:
        self.start_log_block()
        schedules = self.query_resource_json("schedules", server_level=True, page_number=page_number)
        self.end_log_block()
        return schedules

    def query_extract_schedules(self) -> ET.Element:
        self.start_log_block()
        schedules = self.query_schedules()
        extract_schedules = schedules.findall('.//t:schedule[@type="Extract"]', self.ns_map)
        self.end_log_block()
        return extract_schedules

    def query_subscription_schedules(self) -> ET.Element:
        self.start_log_block()
        schedules = self.query_schedules()
        subscription_schedules = schedules.findall('.//t:schedule[@type="Subscription"]', self.ns_map)
        self.end_log_block()
        return subscription_schedules



    def query_schedule(self, schedule_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        schedule = self.query_single_element_from_endpoint('schedule', schedule_name_or_luid, server_level=True)
        self.end_log_block()
        return schedule



    def create_schedule(self, name: Optional[str] = None, extract_or_subscription: Optional[str] = None,
                        frequency: Optional[str] = None, parallel_or_serial: Optional[str] = None,
                        priority: Optional[int] = None, start_time: Optional[str] = None,
                        end_time: Optional[str] = None, interval_value_s: Optional[Union[List[str], str]] = None,
                        interval_hours_minutes: Optional[str] = None,
                        direct_xml_request: Optional[ET.Element] = None) -> str:
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
            tsr = ET.Element('tsRequest')
            s = ET.Element('schedule')
            s.set('name', name)
            s.set('priority', str(priority))
            s.set('type', extract_or_subscription)
            s.set('frequency', frequency)
            s.set('executionOrder', parallel_or_serial)
            fd = ET.Element('frequencyDetails')
            fd.set('start', start_time)
            if end_time is not None:
                fd.set('end', end_time)
            intervals = ET.Element('intervals')

            # Daily does not need an interval value

            if interval_value_s is not None:
                ivs = self.to_list(interval_value_s)
                for i in ivs:
                    interval = ET.Element('interval')
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
            self.end_log_block()
            if e.tableau_error_code == '409021':
                raise AlreadyExistsException('Schedule With this Name Already exists on the server', None)
            else:
                raise e

    def update_schedule(self, schedule_name_or_luid: str, new_name: Optional[str] = None,
                        frequency: Optional[str] = None, parallel_or_serial: Optional[str] = None,
                        priority: Optional[int] = None, start_time: Optional[str] = None,
                        end_time: Optional[str] = None, interval_value_s: Optional[Union[List[str], str]] = None,
                        interval_hours_minutes: Optional[str] = None,
                        direct_xml_request: Optional[ET.Element] = None) -> ET.Element:
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element('tsRequest')
            s = ET.Element('schedule')
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
                fd = ET.Element('frequencyDetails')
                fd.set('start', start_time)
                if end_time is not None:
                    fd.set('end', end_time)
                intervals = ET.Element('intervals')

                # Daily does not need an interval value

                if interval_value_s is not None:
                    ivs = self.to_list(interval_value_s)
                    for i in ivs:
                        interval = ET.Element('interval')
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

        try:
            response = self.send_update_request(url, tsr)
            self.end_log_block()
            return response
        except RecoverableHTTPException as e:
            self.end_log_block()
            if e.tableau_error_code == '409021':
                raise AlreadyExistsException('Schedule With this Name Already exists on the server', None)
            else:
                raise e

    def disable_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
        luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = ET.Element('tsRequest')
        s = ET.Element('schedule')
        s.set('state', 'Suspended')
        tsr.append(s)

        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def enable_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
        luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = ET.Element('tsRequest')
        s = ET.Element('schedule')
        s.set('state', 'Active')
        tsr.append(s)

        url = self.build_api_url("schedules/{}".format(luid), server_level=True)
        self.send_update_request(url, tsr)
        self.end_log_block()

    def create_daily_extract_schedule(self, name: str, start_time: str, priority: Optional[int]  = 1,
                                      parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        # Check the time format at some point

        luid = self.create_schedule(name, 'Extract', 'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_daily_subscription_schedule(self, name: str, start_time: str, priority: Optional[int]  = 1,
                                      parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        # Check the time format at some point

        luid = self.create_schedule(name, 'Subscription', 'Daily', parallel_or_serial, priority, start_time)
        self.end_log_block()
        return luid

    def create_weekly_extract_schedule(self, name: str, weekday_s: Union[List[str], str], start_time: str,
                                       priority: Optional[int]  = 1,
                                       parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        luid = self.create_schedule(name, 'Extract', 'Weekly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_weekly_subscription_schedule(self, name: str, weekday_s: Union[List[str], str], start_time: str,
                                       priority: Optional[int]  = 1,
                                       parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        luid = self.create_schedule(name, 'Subscription', 'Weekly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=weekday_s)
        self.end_log_block()
        return luid

    def create_monthly_extract_schedule(self, name: str, day_of_month: str, start_time: str,
                                        priority: Optional[int] = 1,
                                        parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        luid = self.create_schedule(name, 'Extract', 'Monthly', parallel_or_serial, priority, start_time=start_time,
                                    interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_monthly_subscription_schedule(self, name: str, day_of_month: str, start_time: str,
                                             priority: Optional[int] = 1,
                                             parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        luid = self.create_schedule(name, 'Subscription', 'Monthly', parallel_or_serial, priority,
                                    start_time=start_time, interval_value_s=day_of_month)
        self.end_log_block()
        return luid

    def create_hourly_extract_schedule(self, name: str, interval_hours_or_minutes: str, interval: str, start_time: str,
                                       end_time: str, priority: Optional[int] = 1,
                                       parallel_or_serial: Optional[str] = 'Parallel') -> str:

        self.start_log_block()
        luid = self.create_schedule(name, 'Extract', 'Hourly', parallel_or_serial, priority, start_time, end_time,
                                    interval, interval_hours_or_minutes)
        self.end_log_block()
        return luid

    def create_hourly_subscription_schedule(self, name: str, interval_hours_or_minutes: str, interval: str, start_time: str,
                                            end_time: str, priority: Optional[int] = 1,
                                            parallel_or_serial: Optional[str] = 'Parallel') -> str:
        self.start_log_block()
        luid = self.create_schedule(name, 'Subscription', 'Hourly', parallel_or_serial, priority, start_time, end_time,
                                    interval, interval_hours_or_minutes)
        self.end_log_block()
        return luid

    def delete_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
        schedule_luid = self.query_schedule_luid(schedule_name_or_luid)
        url = self.build_api_url("schedules/{}".format(schedule_luid), server_level=True)
        self.send_delete_request(url)
        self.end_log_block()


class ScheduleMethods27(ScheduleMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

class ScheduleMethods28(ScheduleMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

    def add_workbook_to_schedule(self, wb_name_or_luid: str, schedule_name_or_luid: str,
                                 proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = ET.Element('tsRequest')
        t = ET.Element('task')
        er = ET.Element('extractRefresh')
        w = ET.Element('workbook')
        w.set('id', wb_luid)
        er.append(w)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/workbooks".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    def add_datasource_to_schedule(self, ds_name_or_luid: str, schedule_name_or_luid: str,
                                   proj_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()

        ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)
        schedule_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = ET.Element('tsRequest')
        t = ET.Element('task')
        er = ET.Element('extractRefresh')
        d = ET.Element('datasource')
        d.set('id', ds_luid)
        er.append(d)
        t.append(er)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/datasources".format(schedule_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

class ScheduleMethods30(ScheduleMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class ScheduleMethods31(ScheduleMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

class ScheduleMethods32(ScheduleMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class ScheduleMethods33(ScheduleMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class ScheduleMethods34(ScheduleMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class ScheduleMethods35(ScheduleMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class ScheduleMethods36(ScheduleMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base