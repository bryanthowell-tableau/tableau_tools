from tableau_rest_api_connection_31 import *


class TableauRestApiConnection32(TableauRestApiConnection31):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection31.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"2018.3")

    def query_data_driven_alerts(self):
        self.start_log_block()
        alerts = self.query_resource(u"dataAlerts")
        self.end_log_block()
        return alerts

    def query_data_driven_alerts_for_view(self, view_luid):
        self.start_log_block()
        alerts = self.query_resource(u"dataAlerts?filter=viewId:eq:{}".format(view_luid))
        self.end_log_block()
        return alerts

    def query_data_driven_alert_details(self, data_alert_luid):
        self.start_log_block()
        alert_details = self.query_resource(u"dataAlerts/{}".format(data_alert_luid))
        self.end_log_block()
        return alert_details

    def delete_data_driven_alert(self, data_alert_luid):
        self.start_log_block()
        url = self.build_api_url(u"dataAlerts/{}".format(data_alert_luid))
        self.send_delete_request(url)
        self.end_log_block()

    def add_user_to_data_driven_alert(self, data_alert_luid, username_or_luid):
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        tsr = etree.Element(u"tsRequest")
        u = etree.Element(u"user")
        u.set(u"id", user_luid)
        tsr.append(u)
        url = self.build_api_url(u'dataAlerts/{}/users'.format(data_alert_luid))
        self.send_add_request(url, tsr)
        self.end_log_block()

    def update_data_driven_alert(self, data_alert_luid, subject=None, frequency=None, owner_username_or_luid=None):
        """
        :type subject: unicode
        :type frequency: unicode
        :type owner_username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        tsr = etree.Element(u"tsRequest")
        d = etree.Element(u"dataAlert")
        if subject is not None:
            d.set(u"subject", subject)

        if frequency is not None:
            frequency = frequency.lower()
            allowed_frequency = (u'once', u'frequently', u'hourly', u'daily', u'weekly')
            if frequency not in allowed_frequency:
                raise InvalidOptionException(u'frequency must be once, frequently, hourly, daily or weekly')
            d.set(u'frequency', frequency)

        if owner_username_or_luid is not None:
            if self.is_luid(owner_username_or_luid):
                owner_luid = owner_username_or_luid
            else:
                owner_luid = self.query_user_luid(owner_username_or_luid)
            o = etree.Element(u'owner')
            o.set(u"id", owner_luid)
            d.append(o)

        tsr.append(d)
        url = self.build_api_url(u"dataAlerts/{}".format(data_alert_luid))
        response = self.send_update_request(url, tsr)
        self.end_log_block()
        return response

    def delete_user_from_data_driven_alert(self, data_alert_luid, username_or_luid):
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)

        url = self.build_api_url(u'dataAlerts/{}/users/{}'.format(data_alert_luid, user_luid))
        self.send_delete_request(url)
        self.end_log_block()

    # In 3.2, you can hide views from publishing
    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, async_publish=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False, views_to_hide_list=None):
        """
        :type workbook_filename: unicode
        :type workbook_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type show_tabs: bool
        :param check_published_ds: Set to False to improve publish speed if you KNOW there are no published data sources
        :type check_published_ds: bool
        :type oauth_flag: bool:
        :type views_to_hide_list: list[unicode]
        :
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content(u'workbook', workbook_filename, workbook_name, project_luid,
                                   {u"overwrite": overwrite, u"asJob": async_publish}, connection_username,
                                   connection_password, save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag,
                                   views_to_hide_list=views_to_hide_list)
        if async_publish is True:
            job = xml.findall(u'.//t:job', self.ns_map)
            return job[0].get(u'id')
        else:
            workbook = xml.findall(u'.//t:workbook', self.ns_map)
            return workbook[0].get(u'id')
