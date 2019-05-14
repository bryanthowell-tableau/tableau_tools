from tableau_rest_api_connection_32 import *
from url_filter import UrlFilter33

class TableauRestApiConnection32(TableauRestApiConnection31):
    def __init__(self, server, username, password, site_content_url=u""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection31.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version(u"2019.1")

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, async_publish=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False, views_to_hide_list=None, generate_thumbnails_as_username_or_luid=None):
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
        :type generate_thumbnails_as_username_or_luid: unicode
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content(u'workbook', workbook_filename, workbook_name, project_luid,
                                   {u"overwrite": overwrite, u"asJob": async_publish}, connection_username,
                                   connection_password, save_credentials, show_tabs=show_tabs,
                                   check_published_ds=check_published_ds, oauth_flag=oauth_flag,
                                   views_to_hide_list=views_to_hide_list,
                                   generate_thumbnails_as_username_or_luid=generate_thumbnails_as_username_or_luid)
        if async_publish is True:
            job = xml.findall(u'.//t:job', self.ns_map)
            return job[0].get(u'id')
        else:
            workbook = xml.findall(u'.//t:workbook', self.ns_map)
            return workbook[0].get(u'id')


    # Flow Methods Start

    def query_flow_luid(self, flow_name, project_name_or_luid=None):
        """
        :type flow_name: unicode
        :type project_name_or_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()

        flow_name_filter = UrlFilter33.create_name_filter(flow_name)

        flows = self.query_flows_for_a_site(flow_name_filter=flow_name_filter,
                                            project_name_or_luid=project_name_or_luid)
        # There should only be one flow here if any found
        if len(flows) == 1:
            self.end_log_block()
            return flows[0].get(u"id")
        else:
            self.end_log_block()
            raise NoMatchFoundException(u"No {} found with name {}".format(flows, flow_name))

    def query_flows_for_a_site(self, project_name_or_luid=None, all_fields=True, updated_at_filter=None,
                               created_at_filter=None, flow_name_filter=None, owner_name_filter=None, sorts=None,
                               fields=None):
        """
        :type project_name_or_luid: unicode
        :type all_fields: bool
        :type updated_at_filter: UrlFilter
        :type created_at_filter: UrlFilter
        :type flow_name_filter: UrlFilter
        :type owner_name_filter: UrlFilter
        :type sorts: list[Sort]
        :type fields: list[unicode]
        :rtype: etree.Element
        """
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = [u'_all_']

        # If create a ProjectName filter inherently if necessary
        project_name_filter = None
        if project_name_or_luid is not None:
            if not self.is_luid(project_name_or_luid):
                project_name = project_name_or_luid
            else:
                project = self.query_project_xml_object(project_name_or_luid)
                project_name = project.get(u'name')
            project_name_filter = UrlFilter33.create_project_name_equals_filter(project_name)

        filter_checks = {u'updatedAt': updated_at_filter, u'createdAt': created_at_filter, u'name': flow_name_filter,
                         u'ownerName': owner_name_filter, u'projectName': project_name_filter}
        filters = self._check_filter_objects(filter_checks)

        flows = self.query_resource(u'flows', filters=filters, sorts=sorts, fields=fields)

        self.end_log_block()
        return flows

    def query_flows_for_a_user(self, username_or_luid, is_owner_flag=False):
        """
        :type username_or_luid: unicode
        :type is_owner_flag: bool
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(username_or_luid):
            user_luid = username_or_luid
        else:
            user_luid = self.query_user_luid(username_or_luid)
        additional_url_params = u""
        if is_owner_flag is True:
            additional_url_params += u"?ownedBy=true"

        flows = self.query_resource(u'users/{}/flows{}'.format(user_luid, additional_url_params))
        self.end_log_block()
        return flows

    def query_flow(self, flow_name_or_luid, project_name_or_luid=None):
        """
        :type flow_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid, project_name_or_luid=project_name_or_luid)

        flow = self.query_resource(u'flows/{}'.format(flow_luid))

        self.end_log_block()
        return flow

    def query_flow_connections(self, flow_name_or_luid, project_name_or_luid=None):
        """
        :type flow_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid, project_name_or_luid=project_name_or_luid)

        connections = self.query_resource(u'flows/{}/connections'.format(flow_luid))

        self.end_log_block()
        return connections


    def get_flow_run_tasks(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        tasks = self.query_resource(u'tasks/runFlow')
        self.end_log_block()
        return tasks

    def get_flow_run_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        task = self.query_resource(u'tasks/runFlow/{}'.format(task_luid))
        self.end_log_block()
        return task

    def run_flow_now(self, flow_name_or_luid, flow_output_step_ids=None):
        """
        :type flow_name_or_luid: unicode
        :type flow_output_step_ids: list[unicode]
        :rtype unicode
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid)

        additional_url_params = u""

        # Implement once documentation is back up and going
        if flow_output_step_ids is not None:
            pass

        tsr = etree.Element(u'tsRequest')
        url = self.build_api_url(u"flows/{}/run{}".format(flow_luid, additional_url_params))
        job_luid = self.send_add_request(url, tsr)
        self.end_log_block()
        return job_luid

    def run_flow_task(self, task_luid):
        """
        :type task_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        url = self.build_api_url(u'tasks/runFlow/{}/runNow'.format(task_luid))
        response = self.send_post_request(url)
        self.end_log_block()
        return response


    def update_flow(self, flow_name_or_luid, project_name_or_luid=None, owner_username_or_luid=None):
        """
        :type flow_name_or_luid: unicode
        :type project_name_or_luid: unicode
        :type owner_username_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if project_name_or_luid is None and owner_username_or_luid is None:
            raise InvalidOptionException(u'Must include at least one change, either project or owner or both')

        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        f = etree.Element(u'flow')
        if project_name_or_luid is not None:
            if self.is_luid(project_name_or_luid):
                proj_luid = project_name_or_luid
            else:
                proj_luid = self.query_project_luid(project_name_or_luid)
            p = etree.Element(u'project')
            p.set(u'id', proj_luid)
            f.append(p)

        if owner_username_or_luid is not None:
            if self.is_luid(owner_username_or_luid):
                owner_luid = owner_username_or_luid
            else:
                owner_luid = self.query_user_luid(owner_username_or_luid)

            o = etree.Element(u'owner')
            o.set(u'id', owner_luid)
            f.append(o)

        tsr.append(f)

        url = self.build_api_url(u'flows/{}'.format(flow_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    def update_flow_connection(self, flow_luid, flow_connection_luid,  server_address=None, port=None, connection_username=None,
                               connection_password=None, embed_password=False):
        """
        :type flow_luid: unicode
        :type flow_connection_luid: unicode
        :type server_address: unicode
        :type port: unicode
        :type connection_username: unicode
        :type connection_password: unicode
        :type embed_password: unicode
        :rtype: etree.Element
        """
        self.start_log_block()

        tsr = etree.Element(u'tsRequest')
        c = etree.Element(u'connection')
        updates_count = 0
        if server_address is not None:
            c.set(u'serverAddress', server_address)
            updates_count += 1
        if port is not None:
            c.set(u'port', port)
            updates_count += 1
        if connection_username is not None:
            c.set(u'userName', connection_username)
            updates_count += 1
        if connection_password is not None:
            c.set(u'password', connection_password)
            updates_count += 1
        if embed_password is True:
            c.set(u'embedPassword', u'true')
            updates_count += 1

        if updates_count == 0:
            return InvalidOptionException(u'Must specify at least one element to update')

        tsr.append(c)
        url = self.build_api_url(u'flows/{}/connections/{}'.format(flow_luid, flow_connection_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    def delete_flow(self, flow_name_or_luid):
        """
        :type flow_name_or_luid: unicode
        :return:
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid)
        url = self.build_api_url(u"flows/{}".format(flow_luid))
        self.send_delete_request(url)
        self.end_log_block()

    def add_flow_task_to_schedule(self, flow_name_or_luid, schedule_name_or_luid):
        """
        :type flow_name_or_luid: unicode
        :type schedule_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid)

        if self.is_luid(schedule_name_or_luid):
            sched_luid = schedule_name_or_luid
        else:
            sched_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = etree.Element(u'tsRequest')
        t = etree.Element(u'task')
        fr = etree.Element(u'flowRun')
        f = etree.Element(u'flow')
        f.set(u'id', flow_luid)
        fr.append(f)
        t.append(fr)
        tsr.append(t)

        url = self.build_api_url(u"schedules/{}/flows".format(sched_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_flow(self, flow_name_or_luid, filename_no_extension, proj_name_or_luid=None):
        """
        :type flow_name_or_luid: unicode
        :type filename_no_extension: unicode
        :type proj_name_or_luid: unicode
        :return Filename of the save workbook
        :rtype: unicode
        """
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_workbook_luid(flow_name_or_luid, proj_name_or_luid)
        try:

            url = self.build_api_url(u"flows/{}/content".format(flow_luid))
            flow = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find(u'application/xml') != -1:
                extension = u'.tfl'
            elif self._last_response_content_type.find(u'application/octet-stream') != -1:
                extension = u'.tflx'
            if extension is None:
                raise IOError(u'File extension could not be determined')
            self.log(
                u'Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
        except RecoverableHTTPException as e:
            self.log(u"download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise
        except:
            self.end_log_block()
            raise
        try:

            save_filename = filename_no_extension + extension

            save_file = open(save_filename, 'wb')
            save_file.write(flow)
            save_file.close()

        except IOError:
            self.log(u"Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    def publish_flow(self, flow_filename, flow_name, project_obj, overwrite=False, connection_username=None,
                     connection_password=None, save_credentials=True, description=None, oauth_flag=False):
        """
        :type flow_filename: unicode
        :type flow_name: unicode
        :type project_obj: Project20 or Project21 or Project28
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type description: unicode
        :type oauth_flag: bool
        :rtype: unicode
        """
        project_luid = project_obj.luid
        xml = self.publish_content(u'flow', flow_filename, flow_name, project_luid, {u"overwrite": overwrite},
                                   connection_username, connection_password, save_credentials, oauth_flag=oauth_flag,
                                   description=description)
        flow = xml.findall(u'.//t:flow', self.ns_map)
        return flow[0].get('id')

    # Flow Methods End