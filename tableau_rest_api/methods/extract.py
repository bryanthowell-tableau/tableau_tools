from .rest_api_base import *


class ExtractMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def get_extract_refresh_tasks(self) -> ET.Element:
        self.start_log_block()
        extract_tasks = self.query_resource('tasks/extractRefreshes')
        self.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid: str) -> ET.Element:
        self.start_log_block()
        extract_task = self.query_resource('tasks/extractRefreshes/{}'.format(task_luid))
        self.start_log_block()
        return extract_task

    # From API 2.6. This gives back much more information
    def get_extract_refresh_tasks_on_schedule(self, schedule_name_or_luid: str) -> ET.Element:
        self.start_log_block()
        schedule_luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        tasks_on_sched = tasks.findall('.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.ns_map)
        if len(tasks_on_sched) == 0:
            self.end_log_block()
            raise NoMatchFoundException(
                "No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.end_log_block()
        return tasks_on_sched

    # From API 2.2, commented out for lack of being useful compared to _on_schedule above
    #def query_extract_refresh_tasks_in_a_schedule(self, schedule_name_or_luid: str) -> ET.Element:
    #    self.start_log_block()
    #    luid = self.query_schedule_luid(schedule_name_or_luid)
    #   tasks = self.query_resource("schedules/{}/extracts".format(luid))
    #    self.end_log_block()
    #    return tasks

    def run_extract_refresh_task(self, task_luid:str) -> str:
        self.start_log_block()
        tsr = ET.Element('tsRequest')
        url = self.build_api_url('tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.send_add_request(url, tsr)
        self.end_log_block()
        return response.findall('.//t:job', self.ns_map)[0].get("id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid: str):
        self.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    def run_extract_refresh_for_workbook(self, wb_name_or_luid: str, proj_name_or_luid: Optional[str] = None):
        self.start_log_block()

        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid)
        tasks = self.get_extract_refresh_tasks()

        extracts_for_wb = tasks.findall('.//t:extract/workbook[@id="{}"]..'.format(wb_luid), self.ns_map)

        for extract in extracts_for_wb:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    # Check if this actually works
    def run_extract_refresh_for_datasource(self, ds_name_or_luid: str, proj_name_or_luid: Optional[str] = None):
        self.start_log_block()
        ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        extracts_for_ds = tasks.findall('.//t:extract/datasource[@id="{}"]..'.format(ds_luid), self.ns_map)
        # print extracts_for_wb
        for extract in extracts_for_ds:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    # Checks status of AD sync process or extract
    def query_job(self, job_luid: str) -> ET.Element:
        self.start_log_block()
        job = self.query_resource("jobs/{}".format(job_luid))
        self.end_log_block()
        return job
    
class ExtractMethods27(ExtractMethods):
    def __init__(self, rest_api_base: TableauRestApiBase27):
        self.rest_api_base = rest_api_base

class ExtractMethods28(ExtractMethods27):
    def __init__(self, rest_api_base: TableauRestApiBase28):
        self.rest_api_base = rest_api_base

    def update_datasource_now(self, ds_name_or_luid: str,
                              project_name_or_luid: Optional[str] = None) -> ET.Element:

        self.start_log_block()
        ds_luid = self.query_datasource_luid(ds_name_or_luid, project_name_or_luid=project_name_or_luid)

        # Has an empty request but is POST because it makes a
        tsr = ET.Element('tsRequest')

        url = self.build_api_url('datasources/{}/refresh'.format(ds_luid))
        response = self.send_add_request(url, tsr)

        self.end_log_block()
        return response

    def update_workbook_now(self, wb_name_or_luid: str, project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid=project_name_or_luid)

        # Has an empty request but is POST because it makes a
        tsr = ET.Element('tsRequest')

        url = self.build_api_url('workbooks/{}/refresh'.format(wb_luid))
        response = self.send_add_request(url, tsr)

        self.end_log_block()
        return response

    def run_extract_refresh_for_workbook(self, wb_name_or_luid: str,
                                         proj_name_or_luid: Optional[str] = None) -> ET.Element:
        return self.update_workbook_now(wb_name_or_luid, proj_name_or_luid)

    # Use the specific refresh rather than the schedule task in 2.8
    def run_extract_refresh_for_datasource(self, ds_name_or_luid: str,
                                           proj_name_or_luid: Optional[str] = None) -> ET.Element:
        return self.update_datasource_now(ds_name_or_luid, proj_name_or_luid)


class ExtractMethods30(ExtractMethods28):
    def __init__(self, rest_api_base: TableauRestApiBase30):
        self.rest_api_base = rest_api_base

class ExtractMethods31(ExtractMethods30):
    def __init__(self, rest_api_base: TableauRestApiBase31):
        self.rest_api_base = rest_api_base

    def query_jobs(self, progress_filter: Optional[UrlFilter] = None, job_type_filter: Optional[UrlFilter] = None,
                   created_at_filter: Optional[UrlFilter] = None, started_at_filter: Optional[UrlFilter] = None,
                   ended_at_filter: Optional[UrlFilter] = None, title_filter: Optional[UrlFilter] = None,
                   subtitle_filter: Optional[UrlFilter] = None,
                   notes_filter: Optional[UrlFilter] = None) -> ET.Element:
        self.start_log_block()
        filter_checks = {'progress': progress_filter, 'jobType': job_type_filter,
                         'createdAt': created_at_filter, 'title': title_filter,
                         'notes': notes_filter, 'endedAt': ended_at_filter,
                         'subtitle': subtitle_filter, 'startedAt': started_at_filter}
        filters = self._check_filter_objects(filter_checks)

        jobs = self.query_resource("jobs", filters=filters)
        self.log('Found {} jobs'.format(str(len(jobs))))
        self.end_log_block()
        return jobs

    def cancel_job(self, job_luid: str):
        self.start_log_block()
        url = self.build_api_url("jobs/{}".format(job_luid))
        self.send_update_request(url, None)
        self.end_log_block()

class ExtractMethods32(ExtractMethods31):
    def __init__(self, rest_api_base: TableauRestApiBase32):
        self.rest_api_base = rest_api_base

class ExtractMethods33(ExtractMethods32):
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

class ExtractMethods34(ExtractMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class ExtractMethods35(ExtractMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

    def encrypt_extracts(self):
        self.start_log_block()
        url = self.build_api_url('encrypt-extracts')
        self.send_post_request(url=url)
        self.end_log_block()

    def decrypt_extracts(self):
        self.start_log_block()
        url = self.build_api_url('decrypt-extracts')
        self.send_post_request(url=url)
        self.end_log_block()

    def reencrypt_extracts(self):
        self.start_log_block()
        url = self.build_api_url('renecrypt-extracts')
        self.send_post_request(url=url)
        self.end_log_block()

class ExtractMethods36(ExtractMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base