from tableau_rest_api_connection_25 import *


class TableauRestApiConnection26(TableauRestApiConnection25):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection25.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"10.3")

    def get_extract_refresh_tasks(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_tasks = self.query_resource(u'tasks/extractRefreshes')
        self.end_log_block()
        return extract_tasks

    def get_extract_refresh_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        extract_task = self.query_resource(u'tasks/extractRefreshes/{}'.format(task_luid))
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
        tasks_on_sched = tasks.findall(u'.//t:schedule[@id="{}"]/..'.format(schedule_luid), self.ns_map)
        if len(tasks_on_sched) == 0:
            self.end_log_block()
            raise NoMatchFoundException(
                u"No extract refresh tasks found on schedule {}".format(schedule_name_or_luid))
        self.end_log_block()

    def run_extract_refresh_task(self, task_luid):
        """
        :task task_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        tsr = etree.Element(u'tsRequest')

        url = self.build_api_url(u'tasks/extractRefreshes/{}/runNow'.format(task_luid))
        response = self.send_add_request(url, tsr)
        self.end_log_block()
        return response.findall(u'.//t:job', self.ns_map)[0].get(u"id")

    def run_all_extract_refreshes_for_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        extracts = self.query_extract_refresh_tasks_by_schedule(schedule_name_or_luid)
        for extract in extracts:
            self.run_extract_refresh_task(extract.get(u'id'))
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

        extracts_for_wb = tasks.findall(u'.//t:extract/workbook[@id="{}"]..'.format(wb_luid), self.ns_map)

        for extract in extracts_for_wb:
            self.run_extract_refresh_task(extract.get(u'id'))
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
        print tasks
        extracts_for_ds = tasks.findall(u'.//t:extract/datasource[@id="{}"]..'.format(ds_luid), self.ns_map)
        # print extracts_for_wb
        for extract in extracts_for_ds:
            self.run_extract_refresh_task(extract.get(u'id'))
        self.end_log_block()

    # Tags can be scalar string or list
    def add_tags_to_datasource(self, ds_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type tag_s: list[unicode]
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(ds_name_or_luid):
            ds_luid = ds_name_or_luid
        else:
            ds_luid = self.query_workbook_luid(ds_name_or_luid, proj_name_or_luid)
        url = self.build_api_url(u"datasources/{}/tags".format(ds_luid))

        tsr = etree.Element(u"tsRequest")
        ts = etree.Element(u"tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element(u"tag")
            t.set(u"label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def delete_tags_from_datasource(self, ds_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type ds_name_or_luid: unicode
        :type tag_s: list[unicode] or unicode
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
            url = self.build_api_url(u"views/{}/tags/{}".format(ds_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count

    # Tags can be scalar string or list
    def add_tags_to_view(self, view_name_or_luid, workbook_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :type tag_s: list[unicode]
        :type proj_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()

        if self.is_luid(view_name_or_luid):
            vw_luid = view_name_or_luid
        else:
            vw_luid = self.query_workbook_view_luid(workbook_name_or_luid, view_name_or_luid, proj_name_or_luid)
        url = self.build_api_url(u"views/{}/tags".format(vw_luid))

        tsr = etree.Element(u"tsRequest")
        ts = etree.Element(u"tags")
        tags = self.to_list(tag_s)
        for tag in tags:
            t = etree.Element(u"tag")
            t.set(u"label", tag)
            ts.append(t)
        tsr.append(ts)

        tag_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return tag_response

    def delete_tags_from_view(self, view_name_or_luid, workbook_name_or_luid, tag_s, proj_name_or_luid=None):
        """
        :type view_name_or_luid: unicode
        :type workbook_name_or_luid: unicode
        :type tag_s: list[unicode] or unicode
        :type proj_name_or_luid: unicode
        :rtype: int
        """
        self.start_log_block()
        tags = self.to_list(tag_s)
        if self.is_luid(view_name_or_luid):
            vw_luid = view_name_or_luid
        else:
            vw_luid = self.query_workbook_view_luid(view_name_or_luid, workbook_name_or_luid, proj_name_or_luid)
        deleted_count = 0
        for tag in tags:
            url = self.build_api_url(u"views/{}/tags/{}".format(vw_luid, tag))
            deleted_count += self.send_delete_request(url)
        self.end_log_block()
        return deleted_count
