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