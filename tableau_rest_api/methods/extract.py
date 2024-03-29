from .rest_api_base import *


class ExtractMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def get_extract_refresh_tasks(self) -> ET.Element:
        self.rest.start_log_block()
        extract_tasks = self.rest.query_resource('tasks/extractRefreshes')
        self.rest.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid: str) -> ET.Element:
        self.rest.start_log_block()
        extract_task = self.rest.query_resource('tasks/extractRefreshes/{}'.format(task_luid))
        self.rest.start_log_block()
        return extract_task

    # From API 2.6. This gives back much more information
    def get_extract_refresh_tasks_on_schedule(self, schedule_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        schedule_luid = self.rest.query_schedule_luid(schedule_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        tasks_on_sched = tasks.findall('.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.rest.ns_map)
        if len(tasks_on_sched) == 0:
            self.rest.end_log_block()
            raise NoMatchFoundException(
                "No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.rest.end_log_block()
        return tasks_on_sched

    # From API 2.2, commented out for lack of being useful compared to _on_schedule above
    #def query_extract_refresh_tasks_in_a_schedule(self, schedule_name_or_luid: str) -> ET.Element:
    #    self.start_log_block()
    #    luid = self.query_schedule_luid(schedule_name_or_luid)
    #   tasks = self.query_resource("schedules/{}/extracts".format(luid))
    #    self.end_log_block()
    #    return tasks

    def run_extract_refresh_task(self, task_luid:str) -> str:
        self.rest.start_log_block()
        tsr = ET.Element('tsRequest')
        url = self.rest.build_api_url('tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.rest.send_add_request(url, tsr)
        self.rest.end_log_block()
        return response.findall('.//t:job', self.rest.ns_map)[0].get("id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid: str):
        self.rest.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get('id'))
        self.rest.end_log_block()

    def run_extract_refresh_for_workbook(self, wb_name_or_luid: str,
                                         proj_name_or_luid: Optional[str] = None) -> ET.Element:
        return self.update_workbook_now(wb_name_or_luid, proj_name_or_luid)

    # Use the specific refresh rather than the schedule task in 2.8
    def run_extract_refresh_for_datasource(self, ds_name_or_luid: str,
                                           proj_name_or_luid: Optional[str] = None) -> ET.Element:
        return self.update_datasource_now(ds_name_or_luid, proj_name_or_luid)

    # Checks status of AD sync process or extract
    def query_job(self, job_luid: str) -> ET.Element:
        self.rest.start_log_block()
        job = self.rest.query_resource("jobs/{}".format(job_luid))
        self.rest.end_log_block()
        return job

    def update_datasource_now(self, ds_name_or_luid: str,
                              project_name_or_luid: Optional[str] = None) -> ET.Element:

        self.rest.start_log_block()
        ds_luid = self.rest.query_datasource_luid(ds_name_or_luid, project_name_or_luid=project_name_or_luid)

        # Has an empty request but is POST because it makes a
        tsr = ET.Element('tsRequest')

        url = self.rest.build_api_url('datasources/{}/refresh'.format(ds_luid))
        response = self.rest.send_add_request(url, tsr)

        self.rest.end_log_block()
        return response

    def update_workbook_now(self, wb_name_or_luid: str, project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.rest.start_log_block()
        wb_luid = self.rest.query_workbook_luid(wb_name_or_luid, proj_name_or_luid=project_name_or_luid)

        # Has an empty request but is POST because it makes a
        tsr = ET.Element('tsRequest')

        url = self.rest.build_api_url('workbooks/{}/refresh'.format(wb_luid))
        response = self.rest.send_add_request(url, tsr)

        self.rest.end_log_block()
        return response

    def query_jobs(self, progress_filter: Optional[UrlFilter] = None, job_type_filter: Optional[UrlFilter] = None,
                   created_at_filter: Optional[UrlFilter] = None, started_at_filter: Optional[UrlFilter] = None,
                   ended_at_filter: Optional[UrlFilter] = None, title_filter: Optional[UrlFilter] = None,
                   subtitle_filter: Optional[UrlFilter] = None,
                   notes_filter: Optional[UrlFilter] = None) -> ET.Element:
        self.rest.start_log_block()
        filter_checks = {'progress': progress_filter, 'jobType': job_type_filter,
                         'createdAt': created_at_filter, 'title': title_filter,
                         'notes': notes_filter, 'endedAt': ended_at_filter,
                         'subtitle': subtitle_filter, 'startedAt': started_at_filter}
        filters = self.rest._check_filter_objects(filter_checks)

        jobs = self.rest.query_resource("jobs", filters=filters)
        self.rest.log('Found {} jobs'.format(str(len(jobs))))
        self.rest.end_log_block()
        return jobs

    def cancel_job(self, job_luid: str):
        self.rest.start_log_block()
        url = self.rest.build_api_url("jobs/{}".format(job_luid))
        self.rest.send_update_request(url, None)
        self.rest.end_log_block()



class ExtractMethods35(ExtractMethods):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest = rest_api_base

    def encrypt_extracts(self):
        self.rest.start_log_block()
        url = self.rest.build_api_url('encrypt-extracts')
        self.rest.send_post_request(url=url)
        self.rest.end_log_block()

    def decrypt_extracts(self):
        self.rest.start_log_block()
        url = self.rest.build_api_url('decrypt-extracts')
        self.rest.send_post_request(url=url)
        self.rest.end_log_block()

    def reencrypt_extracts(self):
        self.rest.start_log_block()
        url = self.rest.build_api_url('renecrypt-extracts')
        self.rest.send_post_request(url=url)
        self.rest.end_log_block()

