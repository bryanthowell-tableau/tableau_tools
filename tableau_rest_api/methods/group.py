from .rest_api_base import *
class GroupMethods(TableauRestApiBase):
    def query_groups(self) -> etree.Element:
        self.start_log_block()
        groups = self.query_resource("groups")
        for group in groups:
            # Add to group-name : luid cache
            group_luid = group.get("id")
            group_name = group.get('name')
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

    # # No basic verb for querying a single group, so run a query_groups

    def query_groups_json(self, page_number: Optional[int]=None) -> str:
        """
        :type page_number: int
        :rtype: json
        """
        self.start_log_block()
        groups = self.query_resource_json("groups", page_number=page_number)
        #for group in groups:
        #    # Add to group-name : luid cache
        #    group_luid = group.get(u"id")
        #    group_name = group.get(u'name')
        #    self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return groups

    def query_group(self, group_name_or_luid: str) -> etree.Element:
        self.start_log_block()
        group = self.query_single_element_from_endpoint('group', group_name_or_luid)
        # Add to group_name : luid cache
        group_luid = group.get("id")
        group_name = group.get('name')
        self.group_name_luid_cache[group_name] = group_luid

        self.end_log_block()
        return group

    # Groups luckily cannot have the same 'pretty name' on one site
    def query_group_luid(self, group_name: str) -> str:
        self.start_log_block()
        if group_name in self.group_name_luid_cache:
            group_luid = self.group_name_luid_cache[group_name]
            self.log('Found group name {} in cache with luid {}'.format(group_name, group_luid))
        else:
            group_luid = self.query_luid_from_name(content_type='group', name=group_name)
            self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_luid

    def query_group_name(self, group_luid: str) -> str:
        self.start_log_block()
        for name, luid in list(self.group_name_luid_cache.items()):
            if luid == group_luid:
                group_name = name
                self.log('Found group name {} in cache with luid {}'.format(group_name, group_luid))
                return group_name
        # If match is found
        group = self.query_single_element_from_endpoint('group', group_luid)
        group_luid = group.get("id")
        group_name = group.get('name')
        self.log('Loading the Group: LUID cache')
        self.group_name_luid_cache[group_name] = group_luid
        self.end_log_block()
        return group_name

    # Returns the LUID of an existing group if one already exists
    def create_group(self, group_name=None, direct_xml_request=None):
        """
        :type group_name: unicode
        :type direct_xml_request: etree.Element
        :rtype: unicode
        """
        self.start_log_block()

        if direct_xml_request is not None:
            tsr = direct_xml_request
        else:
            tsr = etree.Element("tsRequest")
            g = etree.Element("group")
            g.set("name", group_name)
            tsr.append(g)

        url = self.build_api_url("groups")
        try:
            new_group = self.send_add_request(url, tsr)
            self.end_log_block()
            return new_group.findall('.//t:group', self.ns_map)[0].get("id")
        # If the name already exists, a HTTP 409 throws, so just find and return the existing LUID
        except RecoverableHTTPException as e:
            if e.http_code == 409:
                self.log('Group named {} already exists, finding and returning the LUID'.format(group_name))
                self.end_log_block()
                return self.query_group_luid(group_name)

    # Creating a synced ad group is completely different, use this method
    # The luid is only available in the Response header if bg sync. Nothing else is passed this way -- how to expose?
    def create_group_from_ad_group(self, ad_group_name, ad_domain_name, default_site_role='Unlicensed',
                                   sync_as_background=True):
        """
        :type ad_group_name: unicode
        :type ad_domain_name: unicode
        :type default_site_role: bool
        :type sync_as_background:
        :rtype: unicode
        """
        self.start_log_block()
        if default_site_role not in self._site_roles:
            raise InvalidOptionException('"{}" is not an acceptable site role'.format(default_site_role))

        tsr = etree.Element("tsRequest")
        g = etree.Element("group")
        g.set("name", ad_group_name)
        i = etree.Element("import")
        i.set("source", "ActiveDirectory")
        i.set("domainName", ad_domain_name)
        i.set("siteRole", default_site_role)
        g.append(i)
        tsr.append(g)

        url = self.build_api_url("groups/?asJob={}".format(str(sync_as_background).lower()))
        self.log(url)
        response = self.send_add_request(url, tsr)
        # Response is different from immediate to background update. job ID lets you track progress on background
        if sync_as_background is True:
            job = response.findall('.//t:job', self.ns_map)
            self.end_log_block()
            return job[0].get('id')
        if sync_as_background is False:
            self.end_log_block()
            group = response.findall('.//t:group', self.ns_map)
            return group[0].get('id')
