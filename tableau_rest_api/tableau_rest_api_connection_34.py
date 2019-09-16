from .tableau_rest_api_connection_33 import *
from .url_filter import UrlFilter33

class TableauRestApiConnection34(TableauRestApiConnection33):
    def __init__(self, server, username, password, site_content_url=""):
        """
        :type server: unicode
        :type username: unicode
        :type password: unicode
        :type site_content_url: unicode
        """
        TableauRestApiConnection33.__init__(self, server, username, password, site_content_url)
        self.set_tableau_server_version("2019.2")

    # Generic implementation of all the CSV/PDF/PNG requests
    def _query_data_file(self, download_type, view_name_or_luid, high_resolution=None, view_filter_map=None,
                         wb_name_or_luid=None, proj_name_or_luid=None, max_age_minutes=None):
        """
        :type view_name_or_luid: unicode
        :type high_resolution: bool
        :type view_filter_map: dict
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid
        :type max_age_minutes: int
        :rtype:
        """
        self.start_log_block()
        if self.is_luid(view_name_or_luid):
            view_luid = view_name_or_luid
        else:
            view_luid = self.query_workbook_view_luid(wb_name_or_luid, view_name=view_name_or_luid,
                                                      proj_name_or_luid=proj_name_or_luid)

        if view_filter_map is not None:
            final_filter_map = {}
            for key in view_filter_map:
                new_key = "vf_{}".format(key)
                # Check if this just a string
                if isinstance(view_filter_map[key], str):
                    value = view_filter_map[key]
                else:
                    value = ",".join(map(str,view_filter_map[key]))
                final_filter_map[new_key] = value

            additional_url_params = "?" + urllib.parse.urlencode(final_filter_map)
            if high_resolution is True:
                additional_url_params += "&resolution=high"

        else:
            additional_url_params = ""
            if high_resolution is True:
                additional_url_params += "?resolution=high"
        try:

            url = self.build_api_url("views/{}/{}{}".format(view_luid, download_type, additional_url_params))
            binary_result = self.send_binary_get_request(url)

            self.end_log_block()
            return binary_result
        except RecoverableHTTPException as e:
            self.log("Attempt to request results in HTTP error {}, Tableau Code {}".format(e.http_code, e.tableau_error_code))
            self.end_log_block()
            raise

    def query_view_image(self, view_name_or_luid, high_resolution=False, view_filter_map=None,
                         wb_name_or_luid=None, proj_name_or_luid=None, max_age_minutes=None):
        """
        :type view_name_or_luid: unicode
        :type high_resolution: bool
        :type view_filter_map: dict
        :type wb_name_or_luid: unicode
        :type proj_name_or_luid
        :type max_age_minutes: int
        :rtype:
        """
        self.start_log_block()
        image = self._query_data_file('image', view_name_or_luid=view_name_or_luid, high_resolution=high_resolution,
                                      view_filter_map=view_filter_map, wb_name_or_luid=wb_name_or_luid,
                                      proj_name_or_luid=proj_name_or_luid)
        self.end_log_block()
        return image