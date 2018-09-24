class ContentDeployer:
    def __init__(self):
        self._current_site_index = 0
        self.sites = []

    def __iter__(self):
        """
        :rtype: TableauRestApiConnection
        """
        return self.sites[self._current_site_index]

    def add_site(self, t_rest_api_connection):
        """
        :type t_rest_api_connection: TableauRestApiConnection
        """
        self.sites.append(t_rest_api_connection)

    @property
    def current_site(self):
        """
        :rtype: TableauRestApiConnection
        """
        return self.sites[self._current_site_index]

    @current_site.setter
    def current_site(self, site_content_url):
        i = 0
        for site in self.sites:
            if site.site_content_url == site_content_url:
                self._current_site_index = i
            i += 1

    def next(self):
        if self._current_site_index < len(self.sites):
            self._current_site_index += 1
        else:
            raise StopIteration()