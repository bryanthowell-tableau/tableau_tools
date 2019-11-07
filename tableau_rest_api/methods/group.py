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
