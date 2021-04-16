from .rest_api_base import *

class GroupMethods():
    def __init__(self, rest_api_base: TableauRestApiBase):
        self.rest: TableauRestApiBase = rest_api_base

    #def __getattr__(self, attr):
    #    return getattr(self.rest_api_base, attr)

    def query_groups(self, filters: Optional[List[UrlFilter]] = None,
                     sorts: Optional[List[Sort]] = None) -> ET.Element:
        
        self.rest.start_log_block()
        groups = self.rest.query_resource("groups", filters=filters, sorts=sorts)
        for group in groups:
            # Add to group-name : luid cache
            group_luid = group.get("id")
            group_name = group.get('name')
            self.rest.group_name_luid_cache[group_name] = group_luid
        self.rest.end_log_block()
        return groups

    # # No basic verb for querying a single group, so run a query_groups

    def query_groups_json(self, filters: Optional[List[UrlFilter]] = None,
                     sorts: Optional[List[Sort]] = None, page_number: Optional[int] = None) -> Dict:

            self.rest.start_log_block()
            groups = self.rest.query_resource_json("groups", filters=filters, sorts=sorts, page_number=page_number)
            self.rest.end_log_block()
            return groups

    def query_group(self, group_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        group = self.rest.query_single_element_from_endpoint_with_filter('group', group_name_or_luid)
        # Add to group_name : luid cache
        group_luid = group.get("id")
        group_name = group.get('name')
        self.rest.group_name_luid_cache[group_name] = group_luid

        self.rest.end_log_block()
        return group

    # Returns the LUID of an existing group if one already exists
    def create_group(self, group_name: Optional[str] = None, direct_xml_request: Optional[ET.Element] = None) -> str:
        self.rest.start_log_block()

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = ET.Element("tsRequest")
            g = ET.Element("group")
            g.set("name", group_name)
            tsr.append(g)

        url = self.rest.build_api_url("groups")
        try:
            new_group = self.rest.send_add_request(url, tsr)
            self.rest.end_log_block()
            return new_group.findall('.//t:group', self.rest.ns_map)[0].get("id")
        # If the name already exists, a HTTP 409 throws, so just find and return the existing LUID
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.rest.log('Group named {} already exists, finding and returning the LUID'.format(group_name))
                self.rest.end_log_block()
                return self.rest.query_group_luid(group_name)

    # Creating a synced ad group is completely different, use this method
    # The luid is only available in the Response header if bg sync. Nothing else is passed this way -- how to expose?
    def create_group_from_ad_group(self, ad_group_name: str, ad_domain_name: str,
                                   default_site_role: Optional[str] = 'Unlicensed',
                                   sync_as_background: bool = True) -> str:
        self.rest.start_log_block()
        if default_site_role not in self.rest._site_roles:
            raise InvalidOptionException('"{}" is not an acceptable site role'.format(default_site_role))

        tsr = ET.Element("tsRequest")
        g = ET.Element("group")
        g.set("name", ad_group_name)
        i = ET.Element("import")
        i.set("source", "ActiveDirectory")
        i.set("domainName", ad_domain_name)
        i.set("siteRole", default_site_role)
        g.append(i)
        tsr.append(g)

        url = self.rest.build_api_url("groups/?asJob={}".format(str(sync_as_background).lower()))
        self.rest.log(url)
        response = self.rest.send_add_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall('.//t:job', self.rest.ns_map)
            self.rest.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            self.rest.end_log_block()
            group = response.findall('.//t:group', self.rest.ns_map)
            return group[0].get('id')

    # Take a single user_luid string or a collection of luid_strings
    def add_users_to_group(self, username_or_luid_s: Union[List[str], str], group_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        group_luid = self.rest.query_group_luid(group_name_or_luid)

        users = self.rest.to_list(username_or_luid_s)
        for user in users:
            user_luid = self.rest.query_user_luid(user)

            tsr = ET.Element("tsRequest")
            u = ET.Element("user")
            u.set("id", user_luid)
            tsr.append(u)

            url = self.rest.build_api_url("groups/{}/users/".format(group_luid))
            try:
                self.rest.log("Adding username ID {} to group ID {}".format(user_luid, group_luid))
                result = self.rest.send_add_request(url, tsr)
                return result
            except RecoverableHTTPException as e:
                self.rest.log("Recoverable HTTP exception {} with Tableau Error Code {}, skipping".format(str(e.http_code), e.tableau_error_code))
        self.rest.end_log_block()

    def query_users_in_group(self, group_name_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        luid = self.rest.query_group_luid(group_name_or_luid)
        users = self.rest.query_resource("groups/{}/users".format(luid))
        self.rest.end_log_block()
        return users

    # Local Authentication update group
    def update_group(self, name_or_luid: str, new_group_name: str) -> ET.Element:
        self.rest.start_log_block()
        group_luid = self.rest.query_group_luid(name_or_luid)

        tsr = ET.Element("tsRequest")
        g = ET.Element("group")
        g.set("name", new_group_name)
        tsr.append(g)

        url = self.rest.build_api_url("groups/{}".format(group_luid))
        response = self.rest.send_update_request(url, tsr)
        self.rest.end_log_block()
        return response

    # AD group sync. Must specify the domain and the default site role for imported users
    def sync_ad_group(self, group_name_or_luid: str, ad_group_name: str, ad_domain: str, default_site_role: str,
                      sync_as_background: bool = True) -> str:
        self.rest.start_log_block()
        if sync_as_background not in [True, False]:
            error = "'{}' passed for sync_as_background. Use True or False".format(str(sync_as_background).lower())
            raise InvalidOptionException(error)

        # Check that the group exists
        self.query_group(group_name_or_luid)
        tsr = ET.Element('tsRequest')
        g = ET.Element('group')
        g.set('name', ad_group_name)
        i = ET.Element('import')
        i.set('source', 'ActiveDirectory')
        i.set('domainName', ad_domain)
        i.set('siteRole', default_site_role)
        g.append(i)
        tsr.append(g)

        group_luid = self.rest.query_group_luid(group_name_or_luid)
        url = self.rest.build_api_url(
            "groups/{}".format(group_luid) + "?asJob={}".format(str(sync_as_background)).lower())
        response = self.rest.send_update_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall('.//t:job', self.rest.ns_map)
            self.rest.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            group = response.findall('.//t:group', self.rest.ns_map)
            self.rest.end_log_block()
            return group[0].get('id')

    def delete_groups(self, group_name_or_luid_s: Union[List[str], str]):
        self.rest.start_log_block()
        groups = self.rest.to_list(group_name_or_luid_s)
        for group_name_or_luid in groups:
            if group_name_or_luid == 'All Users':
                self.rest.log('Cannot delete All Users group, skipping')
                continue
            if self.rest.is_luid(group_name_or_luid):
                group_luid = group_name_or_luid
            else:
                group_luid = self.rest.query_group_luid(group_name_or_luid)
            url = self.rest.build_api_url("groups/{}".format(group_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()

    def remove_users_from_group(self, username_or_luid_s: Union[List[str], str], group_name_or_luid: str):
        self.rest.start_log_block()
        group_name = ""
        if self.rest.is_luid(group_name_or_luid):
            group_luid = group_name_or_luid
        else:
            group_name = group_name_or_luid
            group_luid = self.rest.query_group_name(group_name_or_luid)
        users = self.rest.to_list(username_or_luid_s)
        for user in users:
            username = ""
            if self.rest.is_luid(user):
                user_luid = user
            else:
                username = user
                user_luid = self.rest.query_user_luid(user)
            url = self.rest.build_api_url("groups/{}/users/{}".format(group_luid, user_luid))
            self.rest.log('Removing user {}, id {} from group {}, id {}'.format(username, user_luid, group_name, group_luid))
            self.rest.send_delete_request(url)
        self.rest.end_log_block()


class GroupMethods37(GroupMethods):
    def __init__(self, rest_api_base: TableauRestApiBase36):
        self.rest = rest_api_base

    def get_groups_for_a_user(self, username_or_luid: str) -> ET.Element:
        self.rest.start_log_block()
        luid = self.rest.query_user_luid(username_or_luid)
        users = self.rest.query_resource("users/{}/groups".format(luid))
        self.rest.end_log_block()
        return users