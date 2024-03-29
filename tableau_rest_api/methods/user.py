from .rest_api_base import *


class UserMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest = rest_api_base
    
    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)
    
    # The reference has this name, so for consistency adding an alias
    def get_users(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                  site_role_filter: Optional[UrlFilter] = None, sorts: Optional[List[Sort]] = None,
                  fields: Optional[List[str] ] =None) -> ET.Element:
        return self.query_users(all_fields=all_fields, last_login_filter=last_login_filter,
                                site_role_filter=site_role_filter, sorts=sorts, fields=fields)

    def query_users(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                    site_role_filter: Optional[UrlFilter] = None, username_filter: Optional[UrlFilter] = None,
                    sorts: Optional[List[Sort]] = None, fields: Optional[List[str] ] =None) -> ET.Element:
        self.rest.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self.rest._check_filter_objects(filter_checks)

        users = self.rest.query_resource("users", filters=filters, sorts=sorts, fields=fields)
        self.rest.log('Found {} users'.format(str(len(users))))
        self.rest.end_log_block()
        return users

    # The reference has this name, so for consistency adding an alias
    def get_users_json(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                       site_role_filter: Optional[UrlFilter] = None, username_filter: Optional[UrlFilter] = None,
                       sorts: Optional[List[Sort]] = None, fields: Optional[List[str] ] =None,
                       page_number: Optional[int] = None) -> Dict:
        return  self.query_users_json(all_fields=all_fields, last_login_filter=last_login_filter,
                                     site_role_filter=site_role_filter, username_filter=username_filter, sorts=sorts,
                                     fields=fields, page_number=page_number)

    def query_users_json(self, all_fields: bool = True, last_login_filter: Optional[UrlFilter] = None,
                         site_role_filter: Optional[UrlFilter] = None, username_filter: Optional[UrlFilter] = None,
                         sorts: Optional[List[Sort]] = None, fields: Optional[List[str]] = None,
                         page_number: Optional[int] = None) -> Dict:

        self.rest.start_log_block()
        if fields is None:
            if all_fields is True:
                fields = ['_all_']

        filter_checks = {'lastLogin': last_login_filter, 'siteRole': site_role_filter, 'name': username_filter}
        filters = self.rest._check_filter_objects(filter_checks)

        users = self.rest.query_resource_json("users", filters=filters, sorts=sorts, fields=fields, page_number=page_number)

        self.rest.log('Found {} users'.format(str(len(users))))
        self.rest.end_log_block()
        return users

    def query_user(self, username_or_luid: str, all_fields: bool = True) -> ET.Element:
        self.rest.start_log_block()
        user = self.rest.query_single_element_from_endpoint_with_filter("user", username_or_luid, all_fields=all_fields)
        user_luid = user.get("id")
        username = user.get('name')
        self.rest.username_luid_cache[username] = user_luid
        self.rest.end_log_block()
        return user

    def query_username(self, user_luid: str) -> str:
        self.rest.start_log_block()
        try:
            luid_index = list(self.rest.username_luid_cache.values()).index(user_luid)
            username = list(self.rest.username_luid_cache.keys())[luid_index]
        except ValueError as e:
            user = self.query_user(user_luid)
            username = user.get('name')

        self.rest.end_log_block()
        return username

    def add_user_by_username(self, username: Optional[str] = None, site_role: Optional[str] = 'Unlicensed',
                             auth_setting: Optional[str] = None, update_if_exists: bool = False,
                             direct_xml_request: Optional[ET.Element] = None) -> str:
        self.rest.start_log_block()

        # Check to make sure role that is passed is a valid role in the API
        if site_role not in self.rest.site_roles:
            raise InvalidOptionException("{} is not a valid site role in Tableau Server".format(site_role))

        if auth_setting is not None:
            if auth_setting not in ['SAML', 'ServerDefault']:
                raise InvalidOptionException('auth_setting must be either "SAML" or "ServerDefault"')
        self.rest.log("Adding {}".format(username))
        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element("tsRequest")
            u = ET.Element("user")
            u.set("name", username)
            u.set("siteRole", site_role)
            if auth_setting is not None:
                u.set('authSetting', auth_setting)
            tsr.append(u)

        url = self.rest.build_api_url('users')
        try:
            new_user = self.rest.send_add_request(url, tsr)
            new_user_luid = new_user.findall('.//t:user',  self.rest.ns_map)[0].get("id")
            self.rest.end_log_block()
            return new_user_luid
        # If already exists, update site role unless overridden.
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.rest.log("Username '{}' already exists on the server".format(username))
                if update_if_exists is True:
                    self.rest.log('Updating {} to site role {}'.format(username, site_role))
                    self.update_user(username, site_role=site_role)
                    self.rest.end_log_block()
                    return self.rest.query_user_luid(username)
                else:
                    self.rest.end_log_block()
                    raise AlreadyExistsException('Username already exists ',  self.rest.query_user_luid(username))
        except:
            self.rest.end_log_block()
            raise

    # This is "Add User to Site", since you must be logged into a site.
    # Set "update_if_exists" to True if you want the equivalent of an 'upsert', ignoring the exceptions
    def add_user(self, username: Optional[str] = None, fullname: Optional[str] = None,
                 site_role: Optional[str] = 'Unlicensed', password: Optional[str] = None,
                 email: Optional[str] = None, auth_setting: Optional[str] = None,
                 update_if_exists: bool = False, direct_xml_request: Optional[ET.Element] = None) -> str:

        self.rest.start_log_block()

        try:
            # Add username first, then update with full name
            if direct_xml_request is not None:
                # Parse to second level, should be
                new_user_tsr = ET.Element('tsRequest')
                new_user_u = ET.Element('user')
                for t in direct_xml_request:
                    if t.tag != 'user':
                        raise InvalidOptionException('Must submit a tsRequest with a user element')
                    for a in t.attrib:
                        if a in ['name', 'siteRole', 'authSetting']:
                            new_user_u.set(a, t.attrib[a])
                new_user_tsr.append(new_user_u)
                new_user_luid = self.add_user_by_username(direct_xml_request=new_user_tsr)

                update_tsr = ET.Element('tsRequest')
                update_u = ET.Element('user')
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
            self.rest.end_log_block()
            return new_user_luid
        except AlreadyExistsException as e:
            self.rest.log("Username '{}' already exists on the server; no updates performed".format(username))
            self.rest.end_log_block()
            return e.existing_luid

    def update_user(self, username_or_luid: str, full_name: Optional[str] = None, site_role: Optional[str] =None,
                    password: Optional[str] = None,
                    email: Optional[str] = None, direct_xml_request: Optional[ET.Element] = None) -> ET.Element:

        self.rest.start_log_block()
        user_luid =  self.rest.query_user_luid(username_or_luid)

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element("tsRequest")
            u = ET.Element("user")
            if full_name is not None:
                u.set('fullName', full_name)
            if site_role is not None:
                u.set('siteRole', site_role)
            if email is not None:
                u.set('email', email)
            if password is not None:
                u.set('password', password)
            tsr.append(u)

        url = self.rest.build_api_url("users/{}".format(user_luid))
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response

    # Can take collection or single user_luid string
    def remove_users_from_site(self,  username_or_luid_s: Union[List[str], str]):
        self.rest.start_log_block()
        users = self.rest.to_list(username_or_luid_s)
        for user in users:
            user_luid = self.rest.query_user_luid(user)
            url = self.rest.build_api_url("users/{}".format(user_luid))
            self.rest.log('Removing user id {} from site'.format(user_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def unlicense_users(self, username_or_luid_s: Union[List[str], str]):
        self.rest.start_log_block()
        users = self.rest.to_list(username_or_luid_s)
        for user in users:
            user_luid = self.rest.query_user_luid(user)
            self.rest.update_user(username_or_luid=user_luid, site_role="Unlicensed")
        self.rest.end_log_block()
