from .rest_api_base import *

class ServerMethods(TableauRestApiBase):

    def query_server_info(self):
        """
        :rtype: etree.Element
        """
        self.start_log_block()
        server_info = self.query_resource("serverinfo", server_level=True)
        self.end_log_block()
        return server_info

    def query_server_version(self):
        """
        :rtype:
        """
        self.start_log_block()
        server_info = self.query_server_info()
        # grab the server number

    def query_api_version(self):
        self.start_log_block()
        server_info = self.query_server_info()
        # grab api version number

    def update_online_site_logo(self, image_filename):
        """
        :type image_filename: unicode
        :rtype:
        """
        # Request type is mixed and require a boundary
        boundary_string = self.generate_boundary_string()
        for ending in ['.png', ]:
            if image_filename.endswith(ending):
                file_extension = ending[1:]

                # Open the file to be uploaded
                try:
                    content_file = open(image_filename, 'rb')

                except IOError:
                    print("Error: File '{}' cannot be opened to upload".format(image_filename))
                    raise

        if file_extension is None:
            raise InvalidOptionException(
                "File {} is not PNG. Use PNG image.".format(image_filename))

        # Create the initial XML portion of the request
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: form-data; name="site_logo"; filename="new_site_logo.png"\r\n'
        publish_request += 'Content-Type: application/octet-stream\r\n\r\n'

        publish_request += "--{}".format(boundary_string)

        # Content needs to be read unencoded from the file
        content = content_file.read()

        # Add to string as regular binary, no encoding
        publish_request += content

        publish_request += "\r\n--{}--".format(boundary_string)
        url = self.build_api_url('')[:-1]
        return self.send_publish_request(url, publish_request, None, boundary_string)

    def restore_online_site_logo(self):
        """
        :rtype:
        """
        # Request type is mixed and require a boundary
        boundary_string = self.generate_boundary_string()

        # Create the initial XML portion of the request
        publish_request = "--{}\r\n".format(boundary_string)
        publish_request += 'Content-Disposition: form-data; name="site_logo"; filename="empty.txt"\r\n'
        publish_request += 'Content-Type: text/plain\r\n\r\n'

        publish_request += "--{}".format(boundary_string)

        url = self.build_api_url('')[:-1]
        return self.send_publish_request(url, publish_request, None, boundary_string)