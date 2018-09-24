from tableau_rest_api_connection_30 import *


class TableauRestApiConnection31(TableauRestApiConnection30):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection30.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"2018.2")

    def query_jobs(self, progress_filter=None, job_type_filter=None, created_at_filter=None, started_at_filter=None,
                   ended_at_filter=None, title_filter=None, subtitle_filter=None, notes_filter=None):
        """
        :type progress_filter: UrlFilter
        :type job_type_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type started_at_filter: UrlFilter
        :type title_filter: UrlFilter
        :type notes_filter: UrlFilter
        :type subtitle_filter: UrlFilter
        :type ended_at_filter: UrlFilter
        :rtype: etree.Element
        """
        self.start_log_block()
        filter_checks = {u'progress': progress_filter, u'jobType': job_type_filter,
                         u'createdAt': created_at_filter, u'title': title_filter,
                         u'notes': notes_filter, u'endedAt': ended_at_filter,
                         u'subtitle': subtitle_filter, u'startedAt': started_at_filter}
        filters = self._check_filter_objects(filter_checks)

        jobs = self.query_resource(u"jobs", filters=filters)
        self.log(u'Found {} jobs'.format(unicode(len(jobs))))
        self.end_log_block()
        return jobs

    def cancel_job(self, job_luid):
        """
        :type job_luid: unicode
        :return:
        """
        self.start_log_block()
        url = self.build_api_url(u"jobs/{}".format(job_luid))
        self.send_update_request(url, None)
        self.end_log_block()

    def add_project_to_user_favorites(self, favorite_name, proj_name_or_luid=None):
        """
        :type favorite_name: unicode
        :type proj_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(proj_name_or_luid):
            proj_luid = proj_name_or_luid
        else:
            proj_luid = self.query_project_luid(proj_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        f = etree.Element(u'favorite')
        f.set(u'label', favorite_name)
        w = etree.Element(u'project')
        w.set(u'id', proj_luid)
        f.append(w)
        tsr.append(f)

        url = self.build_api_url(u"favorites/{}".format(proj_luid))
        update_response = self.send_update_request(url, tsr)
        self.end_log_block()
        return update_response

    def delete_projects_from_user_favorites(self, proj_name_or_luid_s, username_or_luid):
        """
        :type proj_name_or_luid_s: list[unicode] or unicode
        :type username_or_luid: unicode
        :rtype:
        """
        self.start_log_block()
        projs = self.to_list(proj_name_or_luid_s)
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        for proj in projs:
            if self.is_luid(proj):
                proj_luid = proj
            else:
                proj_luid = self.query_project_luid(proj)
            url = self.build_api_url(u"favorites/{}/projects/{}".format(user_luid, proj_luid))
            self.send_delete_request(url)
        self.end_log_block()