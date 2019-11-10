from .rest_api_base import *

class PublishingMethods(TableauRestApiBase):
    #
    # Start Publish methods -- workbook, datasources, file upload
    #

    ''' Publish process can go two way:
        (1) Initiate File Upload (2) Publish workbook/datasource (less than 64MB)
        (1) Initiate File Upload (2) Append to File Upload (3) Publish workbook to commit (over 64 MB)
    '''

    def publish_workbook(self, workbook_filename, workbook_name, project_obj, overwrite=False, connection_username=None,
                         connection_password=None, save_credentials=True, show_tabs=True, check_published_ds=True,
                         oauth_flag=False):
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
        :type oauth_flag: bool
        :rtype: unicode
        """

        project_luid = project_obj.luid
        xml = self.publish_content('workbook', workbook_filename, workbook_name, project_luid,
                                   {"overwrite": overwrite}, connection_username, connection_password,
                                   save_credentials, show_tabs=show_tabs, check_published_ds=check_published_ds,
                                   oauth_flag=oauth_flag)
        workbook = xml.findall('.//t:workbook', self.ns_map)
        return workbook[0].get('id')

    def publish_datasource(self, ds_filename, ds_name, project_obj, overwrite=False, connection_username=None,
                           connection_password=None, save_credentials=True, oauth_flag=False):
        """
        :type ds_filename: unicode
        :type ds_name: unicode
        :type project_obj: Project20 or Project21
        :type overwrite: bool
        :type connection_username: unicode
        :type connection_password: unicode
        :type save_credentials: bool
        :type oauth_flag: bool
        :rtype: unicode
        """
        project_luid = project_obj.luid
        xml = self.publish_content('datasource', ds_filename, ds_name, project_luid, {"overwrite": overwrite},
                                   connection_username, connection_password, save_credentials, oauth_flag=oauth_flag)
        datasource = xml.findall('.//t:datasource', self.ns_map)
        return datasource[0].get('id')

    # Main method for publishing a workbook. Should intelligently decide to chunk up if necessary
    # If a TableauDatasource or TableauWorkbook is passed, will upload from its content
    def publish_content(self, content_type, content_filename, content_name, project_luid, url_params=None,
                        connection_username=None, connection_password=None, save_credentials=True, show_tabs=False,
                        check_published_ds=True, oauth_flag=False, generate_thumbnails_as_username_or_luid=None,
                        description=None, views_to_hide_list=None):
        # Single upload limit in MB
        single_upload_limit = 20

        # If you need a temporary copy when fixing the published datasources
        temp_wb_filename = None

        # Must be 'workbook' or 'datasource'
        if content_type not in ['workbook', 'datasource', 'flow']:
            raise InvalidOptionException("content_type must be 'workbook',  'datasource', or 'flow' ")

        file_extension = None
        final_filename = None
        cleanup_temp_file = False

        for ending in ['.twb', '.twbx', '.tde', '.tdsx', '.tds', '.tde', '.hyper', '.tfl', '.tflx']:
            if content_filename.endswith(ending):
                file_extension = ending[1:]

                # If twb or twbx, open up and check for any published data sources
                if file_extension.lower() in ['twb', 'twbx'] and check_published_ds is True:
                    self.log("Adjusting any published datasources")
                    t_file = TableauFile(content_filename, self.logger)
                    dses = t_file.tableau_document.datasources
                    for ds in dses:
                        # Set to the correct site
                        if ds.published is True:
                            self.log("Published datasource found")
                            self.log("Setting publish datasource repository to {}".format(self.site_content_url))
                            ds.published_ds_site = self.site_content_url

                    temp_wb_filename = t_file.save_new_file('temp_wb')
                    content_filename = temp_wb_filename
                    # Open the file to be uploaded
                try:
                    content_file = open(content_filename, 'rb')
                    file_size = os.path.getsize(content_filename)
                    file_size_mb = float(file_size) / float(1000000)
                    self.log("File {} is size {} MBs".format(content_filename, file_size_mb))
                    final_filename = content_filename

                    # Request type is mixed and require a boundary
                    boundary_string = self.generate_boundary_string()

                    # Create the initial XML portion of the request
                    publish_request = bytes("--{}\r\n".format(boundary_string).encode('utf-8'))
                    publish_request += bytes('Content-Disposition: name="request_payload"\r\n'.encode('utf-8'))
                    publish_request += bytes('Content-Type: text/xml\r\n\r\n'.encode('utf-8'))

                    # Build publish request in ElementTree then convert at publish
                    publish_request_xml = etree.Element('tsRequest')
                    # could be either workbook, datasource, or flow
                    t1 = etree.Element(content_type)
                    t1.set('name', content_name)
                    if show_tabs is not False:
                        t1.set('showTabs', str(show_tabs).lower())
                    if generate_thumbnails_as_username_or_luid is not None:
                        if self.is_luid(generate_thumbnails_as_username_or_luid):
                            thumbnail_user_luid = generate_thumbnails_as_username_or_luid
                        else:
                            thumbnail_user_luid = self.query_user_luid(generate_thumbnails_as_username_or_luid)
                        t1.set('generateThumbnailsAsUser', thumbnail_user_luid)

                    if connection_username is not None:
                        cc = etree.Element('connectionCredentials')
                        cc.set('name', connection_username)
                        if oauth_flag is True:
                            cc.set('oAuth', "True")
                        if connection_password is not None:
                            cc.set('password', connection_password)
                        cc.set('embed', str(save_credentials).lower())
                        t1.append(cc)

                    # Views to Hide in Workbooks from 3.2
                    if views_to_hide_list is not None:
                        if len(views_to_hide_list) > 0:
                            vs = etree.Element('views')
                            for view_name in views_to_hide_list:
                                v = etree.Element('view')
                                v.set('name', view_name)
                                v.set('hidden', 'true')
                            t1.append(vs)

                    # Description only allowed for Flows as of 3.3
                    if description is not None:
                         t1.set('description', description)
                    p = etree.Element('project')
                    p.set('id', project_luid)
                    t1.append(p)
                    publish_request_xml.append(t1)

                    encoded_request = etree.tostring(publish_request_xml, encoding='utf-8')

                    publish_request += bytes(encoded_request)
                    publish_request += bytes("\r\n--{}".format(boundary_string).encode('utf-8'))

                    # Upload as single if less than file_size_limit MB
                    if file_size_mb <= single_upload_limit:
                        # If part of a single upload, this if the next portion
                        self.log("Less than {} MB, uploading as a single call".format(str(single_upload_limit)))
                        publish_request += bytes('\r\n'.encode('utf-8'))
                        publish_request += bytes('Content-Disposition: name="tableau_{}"; filename="{}"\r\n'.format(
                            content_type, final_filename).encode('utf-8'))
                        publish_request += bytes('Content-Type: application/octet-stream\r\n\r\n'.encode('utf-8'))

                        # Content needs to be read unencoded from the file
                        content = content_file.read()

                        # Add to string as regular binary, no encoding
                        publish_request += content

                        publish_request += bytes("\r\n--{}--".format(boundary_string).encode('utf-8'))

                        url = self.build_api_url("{}s").format(content_type)

                        # Allow additional parameters on the publish url
                        if len(url_params) > 0:
                            additional_params = '?'
                            i = 1
                            for param in url_params:
                                if i > 1:
                                    additional_params += "&"
                                additional_params += "{}={}".format(param, str(url_params[param]).lower())
                                i += 1
                            url += additional_params

                        content_file.close()
                        if temp_wb_filename is not None:
                            os.remove(temp_wb_filename)
                        if cleanup_temp_file is True:
                            os.remove(final_filename)
                        return self.send_publish_request(url, None, publish_request, boundary_string)
                    # Break up into chunks for upload
                    else:
                        self.log("Greater than 10 MB, uploading in chunks")
                        upload_session_id = self.initiate_file_upload()

                        # Upload each chunk
                        for piece in self.read_file_in_chunks(content_file):
                            self.log("Appending chunk to upload session {}".format(upload_session_id))
                            self.append_to_file_upload(upload_session_id, piece, final_filename)

                        # Finalize the publish
                        url = self.build_api_url("{}s").format(content_type) + "?uploadSessionId={}".format(
                            upload_session_id) + "&{}Type={}".format(content_type, file_extension)

                        # Allow additional parameters on the publish url
                        if len(url_params) > 0:
                            additional_params = '&'
                            i = 1
                            for param in url_params:
                                if i > 1:
                                    additional_params += "&"
                                additional_params += "{}={}".format(param, str(url_params[param]).lower())
                                i += 1
                            url += additional_params

                        publish_request += bytes("--".encode('utf-8'))  # Need to finish off the last boundary
                        self.log("Finishing the upload with a publish request")
                        content_file.close()
                        if temp_wb_filename is not None:
                            os.remove(temp_wb_filename)
                        if cleanup_temp_file is True:
                            os.remove(final_filename)
                        return self.send_publish_request(url, None, publish_request, boundary_string)

                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(content_filename))
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                "File {} does not have an acceptable extension. Should be .twb,.twbx,.tde,.tdsx,.tds,.tde".format(
                    content_filename))

    def initiate_file_upload(self):
        url = self.build_api_url("fileUploads")
        xml = self.send_post_request(url)
        file_upload = xml.findall('.//t:fileUpload', self.ns_map)
        return file_upload[0].get("uploadSessionId")

    # Uploads a chunk to an already started session
    def append_to_file_upload(self, upload_session_id, content, filename):
        boundary_string = self.generate_boundary_string()
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: name="request_payload"\r\n'
        publish_request += 'Content-Type: text/xml\r\n\r\n'
        publish_request += '\r\n'
        publish_request += "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: name="tableau_file"; filename="{}"\r\n'.format(
            filename)
        publish_request += 'Content-Type: application/octet-stream\r\n\r\n'

        publish_request += content

        publish_request += "\r\n--{}--".format(boundary_string)
        url = self.build_api_url("fileUploads/{}".format(upload_session_id))
        self.send_append_request(url, publish_request, boundary_string)