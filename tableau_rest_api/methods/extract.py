from .rest_api_base import *
class ExtractMethods(TableauRestApiBase):
    def get_extract_refresh_tasks(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_tasks = self.query_resource('tasks/extractRefreshes')
        self.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_task = self.query_resource('tasks/extractRefreshes/{}'.format(task_luid))
        self.start_log_block()
        return extract_task

    def get_extract_refresh_tasks_on_schedule(self, schedule_name_or_luid):
        """
        :param schedule_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            schedule_luid = schedule_name_or_luid
        else:
            schedule_luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        tasks_on_sched = tasks.findall('.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.ns_map)
        if len(tasks_on_sched) == 0:
            self.end_log_block()
            raise NoMatchFoundException(
                "No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.end_log_block()

    def run_extract_refresh_task(self, task_luid):
        """
        :task task_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        tsr = etree.Element('tsRequest')

        url = self.build_api_url('tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.send_add_request(url, tsr)
        self.end_log_block()
        return response.findall('.//t:job', self.ns_map)[0].get("id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    def run_extract_refresh_for_workbook(self, wb_name_or_luid, proj_name_or_luid=None, username_or_luid=None):
        """
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(wb_name_or_luid):
            wb_luid = wb_name_or_luid
        else:
            wb_luid = self.query_workbook_luid(wb_name_or_luid, proj_name_or_luid, username_or_luid)
        tasks = self.get_extract_refresh_tasks()

        extracts_for_wb = tasks.findall('.//t:extract/workbook[@id="{}"]..'.format(wb_luid), self.ns_map)

        for extract in extracts_for_wb:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

        # Check if this actually works

    def run_extract_refresh_for_datasource(self, ds_name_or_luid, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type proj_name_or_luid: unicode
        :type username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_datasource_luid(ds_name_or_luid, proj_name_or_luid)
        tasks = self.get_extract_refresh_tasks()
        print(tasks)
        extracts_for_ds = tasks.findall('.//t:extract/datasource[@id="{}"]..'.format(ds_luid), self.ns_map)
        # print extracts_for_wb
        for extract in extracts_for_ds:
            self.run_extract_refresh_task(extract.get('id'))
        self.end_log_block()

    # Checks status of AD sync process or extract
    def query_job(self, job_luid):
        """
        :type job_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        job = self.query_resource("jobs/{}".format(job_luid))
        self.end_log_block()
        return job