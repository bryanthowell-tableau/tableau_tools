from .rest_api_base import *

# First Flow Methods appear in API 3.3
class FlowMethods33():
    def __init__(self, rest_api_base: TableauRestApiBase33):
        self.rest_api_base = rest_api_base

    def __getattr__(self, attr):
        return getattr(self.rest_api_base, attr)

    def query_flow_luid(self, flow_name: str, project_name_or_luid: Optional[str] = None) -> str:
        self.start_log_block()

        flow_name_filter = UrlFilter33.create_name_filter(flow_name)

        flows = self.query_flows_for_a_site(flow_name_filter=flow_name_filter,
                                            project_name_or_luid=project_name_or_luid)
        # There should only be one flow here if any found
        if len(flows) == 1:
            self.end_log_block()
            return flows[0].get("id")
        else:
            self.end_log_block()
            raise NoMatchFoundException("No {} found with name {}".format(flows, flow_name))

    def query_flows_for_a_site(self, project_name_or_luid: Optional[str] = None, all_fields: bool = True,
                               updated_at_filter: Optional[UrlFilter] = None,
                               created_at_filter: Optional[UrlFilter] = None,
                               flow_name_filter: Optional[UrlFilter] = None,
                               owner_name_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                               fields: Optional[List[str]] = None) -> ET.Element:
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        # If create a ProjectName filter inherently if necessary
        project_name_filter = None
        if project_name_or_luid is not None:
            if not self.is_luid(project_name_or_luid):
                project_name = project_name_or_luid
            else:
                project = self.query_project_xml_object(project_name_or_luid)
                project_name = project.get('name')
            project_name_filter = UrlFilter33.create_project_name_equals_filter(project_name)

        filter_checks = {'updatedAt': updated_at_filter, 'createdAt': created_at_filter, 'name': flow_name_filter,
                         'ownerName': owner_name_filter, 'projectName': project_name_filter}
        filters = self._check_filter_objects(filter_checks)

        flows = self.query_resource('flows', filters=filters, sorts=sorts, fields=fields)

        self.end_log_block()
        return flows

    def query_flows_for_a_user(self, username_or_luid: str, is_owner_flag: bool = False) -> ET.Element:
        self.start_log_block()
        user_luid = self.query_user_luid(username_or_luid)
        additional_url_params = ""
        if is_owner_flag is True:
            additional_url_params += "?ownedBy=true"

        flows = self.query_resource('users/{}/flows{}'.format(user_luid, additional_url_params))
        self.end_log_block()
        return flows

    def query_flow(self, flow_name_or_luid: str, project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        flow_luid = self.query_flow_luid(flow_name_or_luid, project_name_or_luid=project_name_or_luid)

        flow = self.query_resource('flows/{}'.format(flow_luid))

        self.end_log_block()
        return flow

    def query_flow_connections(self, flow_name_or_luid: str,
                               project_name_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        flow_luid = self.query_flow_luid(flow_name_or_luid, project_name_or_luid=project_name_or_luid)
        connections = self.query_resource('flows/{}/connections'.format(flow_luid))
        self.end_log_block()
        return connections


    def get_flow_run_tasks(self) -> ET.Element:
        self.start_log_block()
        tasks = self.query_resource('tasks/runFlow')
        self.end_log_block()
        return tasks

    def get_flow_run_task(self, task_luid: str) -> ET.Element:
        self.start_log_block()
        task = self.query_resource('tasks/runFlow/{}'.format(task_luid))
        self.end_log_block()
        return task

    def run_flow_now(self, flow_name_or_luid: str, flow_output_step_ids: Optional[List[str]] = None) -> str:
        self.start_log_block()
        if self.is_luid(flow_name_or_luid):
            flow_luid = flow_name_or_luid
        else:
            flow_luid = self.query_flow_luid(flow_name_or_luid)

        additional_url_params = ""

        # Implement once documentation is back up and going
        if flow_output_step_ids is not None:
            pass

        tsr = ET.Element('tsRequest')
        url = self.build_api_url("flows/{}/run{}".format(flow_luid, additional_url_params))
        job_luid = self.send_add_request(url, tsr)
        self.end_log_block()
        return job_luid

    def run_flow_task(self, task_luid: str) -> ET.Element:
        self.start_log_block()
        url = self.build_api_url('tasks/runFlow/{}/runNow'.format(task_luid))
        response = self.send_post_request(url)
        self.end_log_block()
        return response


    def update_flow(self, flow_name_or_luid: str, project_name_or_luid: Optional[str] = None,
                    owner_username_or_luid: Optional[str] = None) -> ET.Element:
        self.start_log_block()
        if project_name_or_luid is None and owner_username_or_luid is None:
            raise InvalidOptionException('Must include at least one change, either project or owner or both')

        if self.is_luid(flow_name_or_luid):
            flow_luid = self.query_flow_luid(flow_name_or_luid)

        tsr = ET.Element('tsRequest')
        f = ET.Element('flow')
        if project_name_or_luid is not None:
            proj_luid = self.query_project_luid(project_name_or_luid)
            p = ET.Element('project')
            p.set('id', proj_luid)
            f.append(p)

        if owner_username_or_luid is not None:
            owner_luid = self.query_user_luid(owner_username_or_luid)

            o = ET.Element('owner')
            o.set('id', owner_luid)
            f.append(o)

        tsr.append(f)

        url = self.build_api_url('flows/{}'.format(flow_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    def update_flow_connection(self, flow_luid: str, flow_connection_luid: str,  server_address: Optional[str] = None,
                               port: Optional[str] = None, connection_username: Optional[str] = None,
                               connection_password: Optional[str] = None,
                               embed_password: bool = False) -> ET.Element:
        self.start_log_block()

        tsr = ET.Element('tsRequest')
        c = ET.Element('connection')
        updates_count = 0
        if server_address is not None:
            c.set('serverAddress', server_address)
            updates_count += 1
        if port is not None:
            c.set('port', port)
            updates_count += 1
        if connection_username is not None:
            c.set('userName', connection_username)
            updates_count += 1
        if connection_password is not None:
            c.set('password', connection_password)
            updates_count += 1
        if embed_password is True:
            c.set('embedPassword', 'true')
            updates_count += 1

        if updates_count == 0:
            raise InvalidOptionException('Must specify at least one element to update')

        tsr.append(c)
        url = self.build_api_url('flows/{}/connections/{}'.format(flow_luid, flow_connection_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    def delete_flow(self, flow_name_or_luid: str):
        self.start_log_block()
        flow_luid = self.query_flow_luid(flow_name_or_luid)
        url = self.build_api_url("flows/{}".format(flow_luid))
        self.send_delete_request(url)
        self.end_log_block()

    def add_flow_task_to_schedule(self, flow_name_or_luid: str, schedule_name_or_luid: str) -> str:
        self.start_log_block()
        flow_luid = self.query_flow_luid(flow_name_or_luid)
        sched_luid = self.query_schedule_luid(schedule_name_or_luid)

        tsr = ET.Element('tsRequest')
        t = ET.Element('task')
        fr = ET.Element('flowRun')
        f = ET.Element('flow')
        f.set('id', flow_luid)
        fr.append(f)
        t.append(fr)
        tsr.append(t)

        url = self.build_api_url("schedules/{}/flows".format(sched_luid))
        response = self.send_update_request(url, tsr)

        self.end_log_block()
        return response

    # Do not include file extension, added automatically. Without filename, only returns the response
    # Use no_obj_return for save without opening and processing
    def download_flow(self, flow_name_or_luid: str, filename_no_extension: str,
                      proj_name_or_luid: Optional[str] = None) -> str:
        self.start_log_block()
        flow_luid = self.query_workbook_luid(flow_name_or_luid, proj_name_or_luid)
        try:

            url = self.build_api_url("flows/{}/content".format(flow_luid))
            flow = self.send_binary_get_request(url)
            extension = None
            if self._last_response_content_type.find('application/xml') != -1:
                extension = '.tfl'
            elif self._last_response_content_type.find('application/octet-stream') != -1:
                extension = '.tflx'
            if extension is None:
                raise IOError('File extension could not be determined')
            self.log(
                'Response type was {} so extension will be {}'.format(self._last_response_content_type, extension))
        except RecoverableHTTPException as e:
            self.log("download_workbook resulted in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
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
            self.log("Error: File '{}' cannot be opened to save to".format(filename_no_extension + extension))
            raise

        self.end_log_block()
        return save_filename

    def publish_flow(self, flow_filename: str, flow_name: str, project_obj: Project,
                           overwrite: bool = False, connection_username: Optional[str] = None,
                           connection_password: Optional[str] = None, save_credentials: bool = True,
                           oauth_flag: bool = False, description: Optional[str] = None) -> str:
        project_luid = project_obj.luid
        xml = self._publish_content(content_type='flow', content_filename=flow_filename, content_name=flow_name,
                                   project_luid=project_luid, url_params={"overwrite": overwrite},
                                   connection_username=connection_username, connection_password=connection_password,
                                   save_credentials=save_credentials, oauth_flag=oauth_flag, description=description)
        flow = xml.findall('.//t:flow', self.ns_map)
        return flow[0].get('id')

class FlowMethods34(FlowMethods33):
    def __init__(self, rest_api_base: TableauRestApiBase34):
        self.rest_api_base = rest_api_base

class FlowMethods35(FlowMethods34):
    def __init__(self, rest_api_base: TableauRestApiBase35):
        self.rest_api_base = rest_api_base

class FlowMethods36(FlowMethods35):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest_api_base = rest_api_base