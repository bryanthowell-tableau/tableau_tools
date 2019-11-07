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
    def add_user_by_username(self, username=None, site_role='Unlicensed', auth_setting=None, update_if_exists=False,
                             direct_xml_request=None):
        """
        :type username: unicode
        :type site_role: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        # Check to make sure role that is passed is a valid role in the API
        if site_role not in self.site_roles:
            raise InvalidOptionException("{} is not a valid site role in Tableau Server".format(site_role))

        if auth_setting is not None:
            if auth_setting not in ['SAML', 'ServerDefault']:
                raise InvalidOptionException('auth_setting must be either "SAML" or "ServerDefault"')
        self.log("Adding {}".format(username))
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            u = etree.Element("user")
            u.set("name", username)
            u.set("siteRole", site_role)
            if auth_setting is not None:
                u.set('authSetting', auth_setting)
            tsr.append(u)

        url = self.build_api_url('users')
        try:
            new_user = self.send_add_request(url, tsr)
            new_user_luid = new_user.findall('.//t:user', self.ns_map)[0].get("id")
            self.end_log_block()
            return new_user_luid
        # If already exists, update site role unless overridden.
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log("Username '{}' already exists on the server".format(username))
                if update_if_exists is True:
                    self.log('Updating {} to site role {}'.format(username, site_role))
                    self.update_user(username, site_role=site_role)
                    self.end_log_block()
                    return self.query_user_luid(username)
                else:
                    self.end_log_block()
                    raise AlreadyExistsException('Username already exists ', self.query_user_luid(username))
        except:
            self.end_log_block()
            raise

    # This is "Add User to Site", since you must be logged into a site.
    # Set "update_if_exists" to True if you want the equivalent of an 'upsert', ignoring the exceptions
    def add_user(self, username=None, fullname=None, site_role='Unlicensed', password=None, email=None,
                 auth_setting=None,
                 update_if_exists=False, direct_xml_request=None):
        """
        :type username: unicode
        :type fullname: unicode
        :type site_role: unicode
        :type password: unicode
        :type email: unicode
        :type update_if_exists: bool
        :type auth_setting: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        try:
            # Add username first, then update with full name
            if direct_xml_request is not None:
                # Parse to second level, should be
                new_user_tsr = etree.Element('tsRequest')
                new_user_u = etree.Element('user')
                for t in direct_xml_request:
                    if t.tag != 'user':
                        raise InvalidOptionException('Must submit a tsRequest with a user element')
                    for a in t.attrib:
                        if a in ['name', 'siteRole', 'authSetting']:
                            new_user_u.set(a, t.attrib[a])
                new_user_tsr.append(new_user_u)
                new_user_luid = self.add_user_by_username(direct_xml_request=new_user_tsr)

                update_tsr = etree.Element('tsRequest')
                update_u = etree.Element('user')
                for t in direct_xml_request:
                    for a in t.attrib:
                        if a in ['fullName', 'email', 'password', 'siteRole', 'authSetting']:
                            update_u.set(a, t.attrib[a])
                update_tsr.append(update_u)
                self.update_user(username_or_luid=new_user_luid, direct_xml_request=update_tsr)

            else:
                new_user_luid = self.add_user_by_username(username, site_role=site_role,
                                                          update_if_exists=update_if_exists, auth_setting=auth_setting)
                self.update_user(new_user_luid, fullname, site_role, password, email)
            self.end_log_block()
            return new_user_luid
        except AlreadyExistsException as e:
            self.log("Username '{}' already exists on the server; no updates performed".format(username))
            self.end_log_block()
            return e.existing_luid
