from tableau_rest_api_connection_21 import *


class TableauRestApiConnection22(TableauRestApiConnection21):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection21.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"9.3")

    # Begin scheduler querying methods
    #

    def query_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_resource(u"schedules", server_level=True)
        self.end_log_block()
        return schedules

    def query_schedules_json(self, page_number=None):
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        schedules = self.query_resource_json(u"schedules", server_level=True, page_number=page_number)
        self.end_log_block()
        return schedules

    def query_extract_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        extract_schedules = schedules.findall(u'.//t:schedule[@type="Extract"]', self.ns_map)
        self.end_log_block()
        return extract_schedules

    def query_subscription_schedules(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        schedules = self.query_schedules()
        subscription_schedules = schedules.findall(u'.//t:schedule[@type="Subscription"]', self.ns_map)
        self.end_log_block()
        return subscription_schedules

    def query_schedule_luid(self, schedule_name):
        """
        :type schedule_name: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_luid_by_name_from_endpoint(u'schedule', schedule_name, server_level=True)
        self.end_log_block()
        return luid

    def query_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        luid = self.query_single_element_from_endpoint(u'schedule', schedule_name_or_luid, server_level=True)
        self.end_log_block()
        return luid

    def query_extract_refresh_tasks_by_schedule(self, schedule_name_or_luid):
        """
        :type schedule_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(schedule_name_or_luid):
            luid = schedule_name_or_luid
        else:
            luid = self.query_schedule_luid(schedule_name_or_luid)
        tasks = self.query_resource(u"schedules/{}/extracts".format(luid))
        self.end_log_block()
        return tasks

    #
    # End Scheduler Querying Methods
    #

    def query_views(self, usage=False):
        """
        :type usage: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        vws = self.query_resource(u"views?includeUsageStatistics={}".format(str(usage).lower()))
        self.end_log_block()
        return vws

    def query_views_json(self, usage=False, page_number=None):
        """
        :type usage: bool
        :rtype: json
        """
        self.start_log_block()
        if usage not in [True, False]:
            raise InvalidOptionException(u'Usage can only be set to True or False')
        vws = self.query_resource_json(u"views?includeUsageStatistics={}".format(str(usage).lower()),
                                       page_number=page_number)
        self.end_log_block()
        return vws

    # Did not implement any variations of query_workbook_views as it's still necessary to know the workbook to narrow
    # down to that particular view