from .rest_api_base import *
class UserMethods(TableauRestApiBase):
    #
    # Start User Querying Methods
    #

    # The reference has this name, so for consistency adding an alias
    def get_users(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                  site_role_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                  fields: Optional[List[str] ] =None) -> etree.Element:
        return self.query_users(all_fields=all_fields, last_login_filter=last_login_filter,
                                site_role_filter=site_role_filter, sorts=sorts, fields=fields)


    def query_users(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                    site_role_filter: Optional[UrlFilter] = None, username_filter: Optional[UrlFilter] = None,
                    sorts: Optional[List[Sort]] = None, fields: Optional[List[str] ] =None) -> etree.Element:
        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource("users", filters=filters, sorts=sorts, fields=fields)
        self.log('Found {} users'.format(str(len(users))))
        self.end_log_block()
        return users

    # The reference has this name, so for consistency adding an alias
    def get_users_json(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                       site_role_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                       fields: Optional[List[str] ] =None, page_number: Optional[int] = None) -> str:
        return self.query_users_json(all_fields=all_fields, last_login_filter=last_login_filter,
                                     site_role_filter=site_role_filter, sorts=sorts, fields=fields, page_number=page_number)

    def query_users_json(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                         site_role_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                         fields: Optional[List[str] ] =None, page_number: Optional[int] = None) -> str:

        self.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self._check_filter_objects(filter_checks)

        users = self.query_resource_json("users", filters=filters, sorts=sorts, fields=fields, page_number=page_number)

        self.log('Found {} users'.format(str(len(users))))
        self.end_log_block()
        return users

    def query_user(self, username_or_luid: str, all_fields: bool = True) -> etree.Element:
        self.start_log_block()
        user = self.query_single_element_from_endpoint_with_filter("user", username_or_luid, all_fields=all_fields)
        user_luid = user.get("id")
        username = user.get('name')
        self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user

    def query_user_luid(self, username):
        """
        :type username: unicode
        :rtype: unicode
        """
        self.start_log_block()
        if username in self.username_luid_cache:
            user_luid = self.username_luid_cache[username]
        else:
            user_luid = self.query_luid_from_name(content_type="user", name=username)
            self.username_luid_cache[username] = user_luid
        self.end_log_block()
        return user_luid

    def query_username(self, user_luid):
        """
        :type user_luid: unicode
        :rtype: unicode
        """
        self.start_log_block()
        try:
            luid_index = list(self.username_luid_cache.values()).index(user_luid)
            username = list(self.username_luid_cache.keys())[luid_index]
        except ValueError as e:
            user = self.query_user(user_luid)
            username = user.get('name')

        self.end_log_block()
        return username

    def query_users_in_group(self, group_name_or_luid):
        """
        :type group_name_or_luid: unicode
        :rtype: etree.Element
        """
        self.start_log_block()
        luid = self.query_group_luid(group_name_or_luid)
        users = self.query_resource("groups/{}/users".format(luid))
        self.end_log_block()
        return users

    #
    # End User Querying Methods
    #